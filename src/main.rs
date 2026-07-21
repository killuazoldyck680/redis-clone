use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{env, usize};
use tokio::net::{TcpListener, TcpStream};

use anyhow::Result;
use resp::Value;

use crate::resp::StreamEntry;

mod resp;

enum DataType {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
}

struct DbValue {
    value: DataType,
    expires_at: Option<Instant>,
}

type Db = Arc<Mutex<HashMap<String, DbValue>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("connection established");

                let db_clone = Arc::clone(&db);

                tokio::spawn(async move {
                    handle_conn(stream, db_clone).await;
                });
            }
            Err(e) => {
                println!("error: {e}")
            }
        }
    }
}

async fn handle_conn(stream: TcpStream, db: Db) {
    let mut handler = resp::RespHandler::new(stream);

    println!("Starting read loop");

    loop {
        let value = handler.read_value().await.unwrap();

        println!("Got value {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.trim() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),

                "set" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();
                    let val = unpack_bulk_str(args.get(1).cloned().unwrap()).unwrap();

                    let mut expires_at = None;
                    if let (Some(opt), Some(expiry_val)) = (args.get(2), args.get(3)) {
                        let raw_opt = unpack_bulk_str(opt.clone()).unwrap();
                        // Strip any hidden protocol symbols (\r or \n) and trailing spaces
                        let clean_opt = raw_opt
                            .trim_matches(|c: char| c == '\r' || c == '\n' || c.is_whitespace())
                            .to_lowercase();

                        if clean_opt == "px" {
                            let raw_ms = unpack_bulk_str(expiry_val.clone()).unwrap();
                            let clean_ms = raw_ms.trim_matches(|c: char| {
                                c == '\r' || c == '\n' || c.is_whitespace()
                            });

                            if let Ok(ms) = clean_ms.parse::<u64>() {
                                let now = Instant::now();
                                let target_expiry = now + std::time::Duration::from_millis(ms);

                                println!("--> [DEBUG SET] Current Instant: {:?}", now);
                                println!("--> [DEBUG SET] Adding Delay: {} ms", ms);
                                println!("--> [DEBUG SET] Will Expire At: {:?}", target_expiry);

                                expires_at = Some(target_expiry);
                            }
                        }
                    }

                    let mut db_lock = db.lock().unwrap();

                    db_lock.insert(
                        key,
                        DbValue {
                            value: DataType::String(val),
                            expires_at,
                        },
                    );

                    Value::SimpleString("OK".to_string())
                }

                "get" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let mut db_lock = db.lock().unwrap();

                    let is_expired = if let Some(db_val) = db_lock.get(&key) {
                        if let Some(expiry) = db_val.expires_at {
                            let now = Instant::now();

                            // --- ADD THESE DIAGNOSTIC LOGS ---
                            println!("--> [DEBUG GET] Current Instant: {:?}", now);
                            println!("--> [DEBUG GET] Key Expiry Time: {:?}", expiry);
                            println!("--> [DEBUG GET] Is Current > Expiry? {}", now > expiry);
                            // ---------------------------------

                            now > expiry
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    // 2. If it is expired, we remove it. The immutable borrow from above is completely gone here!
                    if is_expired {
                        db_lock.remove(&key);
                        Value::NullBulkString
                    } else {
                        // 3. Otherwise, fetch it normally
                        match db_lock.get(&key) {
                            Some(db_val) => match &db_val.value {
                                DataType::String(s) => Value::BulkString(s.clone()),
                                _ => Value::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                            },
                            None => Value::NullBulkString,
                        }
                    }
                }
                "rpush" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let mut new_elements = Vec::new();

                    for arg in args.into_iter().skip(1) {
                        if let Ok(element_str) = unpack_bulk_str(arg) {
                            new_elements.push(element_str);
                        }
                    }

                    let mut db_lock = db.lock().unwrap();

                    let final_len = match db_lock.get_mut(&key) {
                        Some(db_val) => match &mut db_val.value {
                            DataType::List(existing_list) => {
                                existing_list.extend(new_elements);
                                existing_list.len()
                            }
                            DataType::String(_) => {
                                panic!(
                                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                                );
                            }
                            _ => {
                                panic!("Unexpected database type value found");
                            }
                        },

                        None => {
                            let list_len = new_elements.len();

                            db_lock.insert(
                                key,
                                DbValue {
                                    value: DataType::List(new_elements),
                                    expires_at: None,
                                },
                            );
                            list_len
                        }
                    };

                    Value::Integer(final_len as i64)
                }
                "lrange" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();
                    let start_index = unpack_bulk_str(args.get(1).cloned().unwrap()).unwrap();
                    let stop_index = unpack_bulk_str(args.get(2).cloned().unwrap()).unwrap();

                    let mut start_index = start_index.parse::<i64>().unwrap();

                    let mut stop_index = stop_index.parse::<i64>().unwrap();

                    let db_lock = db.lock().unwrap();

                    let final_key = match db_lock.get(&key) {
                        Some(db_val) => match &db_val.value {
                            DataType::List(existing_list) => {
                                let length = existing_list.len() as i64;

                                if start_index < 0 {
                                    start_index += length;
                                }
                                if stop_index < 0 {
                                    stop_index += length;
                                }
                                if start_index < 0 {
                                    start_index = 0;
                                }

                                if stop_index < 0 {
                                    stop_index = 0;
                                }

                                if start_index >= length || start_index > stop_index {
                                    Value::Array(vec![])
                                } else {
                                    if stop_index >= length {
                                        stop_index = length - 1;
                                    }
                                    if let Some(element_slice) = existing_list
                                        .get(start_index as usize..=stop_index as usize)
                                    {
                                        Value::Array(
                                            element_slice
                                                .iter()
                                                .map(|item| Value::BulkString(item.clone()))
                                                .collect::<Vec<Value>>(),
                                        )
                                    } else {
                                        Value::Array(vec![])
                                    }
                                }
                            }

                            _ => Value::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        },

                        None => Value::Array(vec![]),
                    };

                    final_key
                }
                "lpush" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let mut new_elements = Vec::new();

                    for arg in args.into_iter().skip(1) {
                        if let Ok(element_str) = unpack_bulk_str(arg) {
                            new_elements.push(element_str);
                        }
                    }

                    let mut db_lock = db.lock().unwrap();

                    let final_list = match db_lock.get_mut(&key) {
                        Some(db_val) => match &mut db_val.value {
                            DataType::List(existing_list) => {
                                for item in new_elements {
                                    existing_list.insert(0, item);
                                }
                                existing_list.len()
                            }

                            DataType::String(_) => {
                                panic!("error");
                            }

                            _ => {
                                panic!("error")
                            }
                        },

                        None => {
                            let list_len = new_elements.len();
                            db_lock.insert(
                                key,
                                DbValue {
                                    value: DataType::List(new_elements),
                                    expires_at: None,
                                },
                            );

                            list_len
                        }
                    };

                    Value::Integer(final_list as i64)
                }
                "llen" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let db_lock = db.lock().unwrap();

                    let list_len = match db_lock.get(&key) {
                        Some(db_val) => match &db_val.value {
                            DataType::List(existing_list) => existing_list.len(),

                            _ => 0,

                            
                        },
                        None => 0,
                    };

                    Value::Integer(list_len as i64)
                }
                "lpop" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let count_opt = args.get(1).cloned();

                    let count_opt = count_opt.map(|val| {
                        unpack_bulk_str(val.clone())
                            .unwrap()
                            .parse::<usize>()
                            .unwrap()
                    });

                    let mut db_lock = db.lock().unwrap();

                    let popped_val = match db_lock.get_mut(&key) {
                        Some(db_val) => match &mut db_val.value {
                            DataType::List(existing_list) => match count_opt {
                                Some(count) => {
                                    let mut popped_elments = Vec::new();

                                    let iterations = std::cmp::min(count, existing_list.len());

                                    for _ in 0..iterations {
                                        let element = existing_list.remove(0);

                                        popped_elments.push(Value::BulkString(element));
                                    }
                                    Value::Array(popped_elments)
                                }

                                None => {
                                    if existing_list.is_empty() {
                                        Value::NullBulkString
                                    } else {
                                        Value::BulkString(existing_list.remove(0))
                                    }
                                }
                            },

                            _ => Value::NullBulkString
                        },

                        None => Value::NullBulkString,
                    };
                    popped_val
                }
                "blpop" => {
                    // Parse as f64 to properly handle decimal timeouts like 0.5
                    let timeout_secs = unpack_bulk_str(args.last().cloned().unwrap())
                        .unwrap()
                        .parse::<f64>()
                        .unwrap();

                    let keys: Vec<String> = args[..args.len() - 1]
                        .iter()
                        .cloned()
                        .map(|val| unpack_bulk_str(val).unwrap())
                        .collect();

                    let timeout_duration = std::time::Duration::from_secs_f64(timeout_secs);

                    // 1. Fast path check
                    let fast_path_val = {
                        let mut db_lock = db.lock().unwrap();
                        let mut found_val = None;

                        for key in &keys {
                            if let Some(db_val) = db_lock.get_mut(key) {
                                if let DataType::List(existing_list) = &mut db_val.value {
                                    if !existing_list.is_empty() {
                                        let element = existing_list.remove(0);
                                        found_val = Some(Value::Array(vec![
                                            Value::BulkString(key.clone()),
                                            Value::BulkString(element),
                                        ]));
                                        break;
                                    }
                                }
                            }
                        }
                        found_val
                    };

                    // 2. Evaluate fast-path or proceed to the polling loop
                    if let Some(response_val) = fast_path_val {
                        response_val
                    } else {
                        let start_time = std::time::Instant::now();

                        let final_polled_val = loop {
                            let popped_element = {
                                let mut loop_db_lock = db.lock().unwrap();
                                let mut found = None;

                                for key in &keys {
                                    if let Some(db_val) = loop_db_lock.get_mut(key) {
                                        if let DataType::List(existing_list) = &mut db_val.value {
                                            if !existing_list.is_empty() {
                                                let element = existing_list.remove(0);
                                                found = Some((key.clone(), element));
                                                break;
                                            }
                                        }
                                    }
                                }
                                found
                            };

                            if let Some((key_name, element_val)) = popped_element {
                                break Value::Array(vec![
                                    Value::BulkString(key_name),
                                    Value::BulkString(element_val),
                                ]);
                            }

                            // Correct timeout check using Duration comparison
                            if timeout_secs > 0.0 && start_time.elapsed() >= timeout_duration {
                                break Value::NullArray;
                            }

                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        };

                        final_polled_val
                    }
                }

                "type" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let mut db_lock = db.lock().unwrap();

                    let checked_val = match db_lock.get(&key) {
                        Some(db_val) => {
                            if let Some(expiry) = db_val.expires_at {
                            if Instant::now() > expiry {
                               db_lock.remove(&key);
                               Value::SimpleString("none".to_string()) 
                            } else {
                                match &db_val.value {
                                    DataType::String(_) => Value::SimpleString("string".to_string()),
                                    DataType::List(_) => Value::SimpleString("list".to_string()),
                                    DataType::Stream(_) => Value::SimpleString("stream".to_string()),
                                }
                            }
                            } else {
                                match &db_val.value {
                                    DataType::String(_) => Value::SimpleString("string".to_string()),
                                    DataType::List(_) => Value::SimpleString("list".to_string()),
                                    DataType::Stream(_) => Value::SimpleString("stream".to_string()), 
                                }
                            }
                        }
                        None => Value::SimpleString("none".to_string()), 
                    };

                    checked_val


                }

                "xadd" => {
                  let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                  let id = unpack_bulk_str(args.get(1).cloned().unwrap()).unwrap();

                  let remaining_args = &args[2..];
                  let mut fields = Vec::new();

                  for chunk in remaining_args.chunks(2) {
                    if chunk.len() == 2 {
                        let field_v = unpack_bulk_str(chunk[1].clone()).unwrap();

                        let field_k = unpack_bulk_str(chunk[0].clone()).unwrap();

                        fields.push((field_k,field_v));
                    }
                }

                let entry = StreamEntry {
                    id: id.clone(),
                    fields
                };

                let mut db_lock = db.lock().unwrap();

                match db_lock.get_mut(&key) {
                    Some(db_val) => {
                       if let DataType::Stream(ref mut entries)  = db_val.value {
                        entries.push(entry);
                       }
                    }

                    None => {
                        db_lock.insert(key, DbValue { value: DataType::Stream(vec![entry]), expires_at: None, },);
                    }
                }
                Value::BulkString(id)








                }

                c => panic!("Error {c}"),
            }
        } else {
            break;
        };

        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            let raw_cmd = unpack_bulk_str(a.first().unwrap().clone())?;
            Ok((raw_cmd.to_lowercase(), a.into_iter().skip(1).collect()))
        }

        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
