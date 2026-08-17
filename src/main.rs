use std::collections::HashMap;
use std::env::args;
use std::fmt::format;
use std::sync::{Arc,Mutex};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, result, usize, vec};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::OwnedWriteHalf;



use anyhow::Result;
use resp::Value;
use tokio::stream;

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
    version: usize,
}

type Db = Arc<Mutex<HashMap<String, DbValue>>>;

type ReplicaList = Arc<std::sync::Mutex<Vec<Arc<std::sync::Mutex<TcpStream>>>>>;

#[tokio::main]
async fn main() {
    let mut port = "6379".to_string();
    let args: Vec<String> = std::env::args().collect();

    let mut is_replica = false;
    let mut i = 1;
    let mut replica_info: Option<(String, String)> = None;

    while i < args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            port = args[i + 1].clone();
            i += 2;
        } else if args[i] == "--replicaof" && i + 1 < args.len() {
            is_replica = true;
            let parts: Vec<&str> = args[i + 1].split_whitespace().collect();
            if parts.len() == 2 {
                replica_info = Some((parts[0].to_string(), parts[1].to_string()));
            }
            i += 2;
        } else {
            i += 1;
        }
    }

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await.unwrap();

    let replicas: ReplicaList = Arc::new(Mutex::new(Vec::new()));
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let master_repl_offset = Arc::new(Mutex::new(0usize));

    if let Some((master_host, master_port)) = replica_info {
        let master_addr = format!("{master_host}:{master_port}");
        let port_clone = port.clone();
        
        let db_master = Arc::clone(&db); 
        let replicas_master = Arc::clone(&replicas);   
        let offset_master = Arc::clone(&master_repl_offset); // FIX 1: Clone before spawn

        tokio::spawn(async move { 
            if let Ok(stream) = TcpStream::connect(&master_addr).await {
                let mut reader = tokio::io::BufReader::new(stream);
                let mut line = String::new();

                // 1. PING
                let ping_cmd = "*1\r\n$4\r\nPING\r\n";
                let _ = reader.write_all(ping_cmd.as_bytes()).await;
                let _ = reader.flush().await;
                line.clear();
                let _ = reader.read_line(&mut line).await;

                // 2. REPLCONF listening-port
                let replconf_port = format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n", 
                    port_clone.len(), 
                    port_clone
                );
                let _ = reader.write_all(replconf_port.as_bytes()).await;
                let _ = reader.flush().await;
                line.clear();
                let _ = reader.read_line(&mut line).await;

                // 3. REPLCONF capa
                let replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                let _ = reader.write_all(replconf_capa.as_bytes()).await;
                let _ = reader.flush().await;
                line.clear();
                let _ = reader.read_line(&mut line).await;

                // 4. PSYNC
                let psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                let _ = reader.write_all(psync.as_bytes()).await;
                let _ = reader.flush().await;
                
                // 5. READ +FULLRESYNC line
                line.clear();
                let _ = reader.read_line(&mut line).await;

                // 6. READ $rdb_len line
                line.clear();
                let _ = reader.read_line(&mut line).await;

                // 7. DRAIN RDB PAYLOAD
                if line.starts_with('$') {
                    if let Ok(rdb_len) = line.trim_start_matches('$').trim().parse::<usize>() {
                        let mut rdb_buf = vec![0u8; rdb_len];
                        let _ = reader.read_exact(&mut rdb_buf).await;
                    }
                }

                let stream = reader.into_inner();

                println!("Handshake complete. Starting master replication loop...");

                handle_conn(stream, db_master, true, replicas_master, true, offset_master).await;
            }
        });
    }

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("connection established");

                let db_client = Arc::clone(&db);
                let replicas_client = Arc::clone(&replicas);
                let offset_client = Arc::clone(&master_repl_offset);
                
                tokio::spawn(async move {
                    handle_conn(stream, db_client, is_replica, replicas_client, false, offset_client).await; // FIX 2: Pass offset_client directly
                });
            }
            Err(e) => {
                println!("error: {e}")
            }
        }
    }
}
async fn execute_command(command: &str, args: Vec<Value>, db: &Db, is_replica: bool, replicas: &ReplicaList, write_half: &Arc<std::sync::Mutex<TcpStream>>, master_repl_offset: Arc<Mutex<usize>>) -> Value {
    let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

    
    match command.to_lowercase().as_str() {
        "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),

"set" => {
    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();
    let val = unpack_bulk_str(args.get(1).cloned().unwrap()).unwrap();

    let mut expires_at = None;
    if let (Some(opt), Some(expiry_val)) = (args.get(2), args.get(3)) {
        let raw_opt = unpack_bulk_str(opt.clone()).unwrap();
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

    println!("3. Entering SET execution...");
    println!("4. Waiting for DB lock...");

    // 1. Scope DB lock so it drops before network operations
    {
        let mut db_lock = db.lock().unwrap();
        println!("5. DB lock acquired!");

        let new_version = db_lock.get(&key).map(|v| v.version).unwrap_or(0) + 1;

        db_lock.insert(
            key.clone(),
            DbValue {
                value: DataType::String(val.clone()),
                expires_at,
                version: new_version,
            },
        );
    } 

    // --- PROPAGATION TO REPLICAS ---
 use std::io::Write; // <--- MUST BE IMPORTED

// --- PROPAGATION TO REPLICAS ---
// --- PROPAGATION TO REPLICAS ---
let cmd_bytes = format!(
    "*{}\r\n${}\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
    3, 3, key.len(), key, val.len(), val
);

let replica_handles: Vec<_> = {
    let replicas_guard = replicas.lock().unwrap();
    println!("--> Broadcasting to {} registered replica(s)...", replicas_guard.len());
    replicas_guard.clone()
};

for (idx, replica) in replica_handles.iter().enumerate() {
    let mut writer = replica.lock().unwrap();

    // try_write writes directly to the non-blocking socket buffer synchronously
    match writer.try_write(cmd_bytes.as_bytes()) {
        Ok(bytes_written) => println!("--> [Replica {}] Sent {} bytes successfully!", idx, bytes_written),
        Err(e) => println!("--> [Replica {}] try_write FAILED: {:?}", idx, e),
    }
}    Value::SimpleString("OK".to_string())
}                "get" => {
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
                            let new_version = db_lock.get(&key).map(|v| v.version).unwrap_or(0) + 1;

                            db_lock.insert(
                                key,
                                DbValue {
                                    value: DataType::List(new_elements),
                                    expires_at: None,
                                    version: new_version,
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

                            let new_version = db_lock.get(&key).map(|v| v.version).unwrap_or(0) + 1;

                            db_lock.insert(
                                key,
                                DbValue {
                                    value: DataType::List(new_elements),
                                    expires_at: None,
                                    version: 0,
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

                            _ => Value::NullBulkString,
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
                                        DataType::String(_) => {
                                            Value::SimpleString("string".to_string())
                                        }
                                        DataType::List(_) => {
                                            Value::SimpleString("list".to_string())
                                        }
                                        DataType::Stream(_) => {
                                            Value::SimpleString("stream".to_string())
                                        }
                                    }
                                }
                            } else {
                                match &db_val.value {
                                    DataType::String(_) => {
                                        Value::SimpleString("string".to_string())
                                    }
                                    DataType::List(_) => Value::SimpleString("list".to_string()),
                                    DataType::Stream(_) => {
                                        Value::SimpleString("stream".to_string())
                                    }
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

                    let (new_ms, second_str) = if id == "*" {
                        let new_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        (new_ms, "*".to_string())
                    } else {
                        let (first, second) = id.split_once('-').expect("missing hyphen");
                        let parsed_ms: u64 = first.parse().expect("invalid u64 for new_ms");
                        (parsed_ms, second.to_string())
                    };

                    let remaining_args = &args[2..];
                    let mut fields = Vec::new();

                    for chunk in remaining_args.chunks(2) {
                        if chunk.len() == 2 {
                            let field_k = unpack_bulk_str(chunk[0].clone()).unwrap();
                            let field_v = unpack_bulk_str(chunk[1].clone()).unwrap();
                            fields.push((field_k, field_v));
                        }
                    }

                    let mut db_lock = db.lock().unwrap();

                    match db_lock.get_mut(&key) {
                        Some(db_val) => match &mut db_val.value {
                            DataType::Stream(entries) => {
                                if second_str == "*" {
                                    // Auto-generate sequence number
                                    let new_seq = match entries.last() {
                                        Some(last_entry) => {
                                            let (f, s) = last_entry
                                                .id
                                                .as_str()
                                                .split_once('-')
                                                .expect("missing hyphen");
                                            let last_ms: u64 =
                                                f.parse().expect("invalid u64 for last_ms");
                                            let last_seq: u64 =
                                                s.parse().expect("invalid u64 for last_seq");

                                            if new_ms == last_ms {
                                                last_seq + 1
                                            } else {
                                                if new_ms == 0 { 1 } else { 0 }
                                            }
                                        }
                                        None => {
                                            if new_ms == 0 {
                                                1
                                            } else {
                                                0
                                            }
                                        }
                                    };

                                    let final_id = format!("{}-{}", new_ms, new_seq);
                                    let entry = StreamEntry {
                                        id: final_id.clone(),
                                        fields,
                                    };
                                    entries.push(entry);
                                    Value::BulkString(final_id)
                                } else {
                                    // Explicit ID validation (e.g., 1000-1)
                                    let new_seq: u64 =
                                        second_str.parse().expect("invalid u64 for new_seq");

                                    if new_ms == 0 && new_seq == 0 {
                                        Value::Error(
                                            "ERR The ID specified in XADD must be greater than 0-0"
                                                .to_string(),
                                        )
                                    } else if let Some(last_entry) = entries.last() {
                                        let (f, s) = last_entry
                                            .id
                                            .as_str()
                                            .split_once('-')
                                            .expect("missing hyphen");
                                        let last_ms: u64 =
                                            f.parse().expect("invalid u64 for last_ms");
                                        let last_seq: u64 =
                                            s.parse().expect("invalid u64 for last_seq");

                                        if new_ms < last_ms
                                            || (new_ms == last_ms && new_seq <= last_seq)
                                        {
                                            Value::Error("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string())
                                        } else {
                                            let entry = StreamEntry {
                                                id: id.clone(),
                                                fields,
                                            };
                                            entries.push(entry);
                                            Value::BulkString(id)
                                        }
                                    } else {
                                        let entry = StreamEntry {
                                            id: id.clone(),
                                            fields,
                                        };
                                        entries.push(entry);
                                        Value::BulkString(id)
                                    }
                                }
                            }
                            _ => Value::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        },
                        None => {
                            // New Stream Key
                            if second_str == "*" {
                                let new_seq = if new_ms == 0 { 1 } else { 0 };
                                let final_id = format!("{}-{}", new_ms, new_seq);
                                let entry = StreamEntry {
                                    id: final_id.clone(),
                                    fields,
                                };
                                let new_version = db_lock.get(&key).map(|v| v.version).unwrap_or(0) + 1;
                                db_lock.insert(
                                    key,
                                    DbValue {
                                        value: DataType::Stream(vec![entry]),
                                        expires_at: None,
                                        version: new_version,
                                    },
                                );
                                Value::BulkString(final_id)
                            } else {
                                let new_seq: u64 =
                                    second_str.parse().expect("invalid u64 for new_seq");

                                if new_ms == 0 && new_seq == 0 {
                                    Value::Error(
                                        "ERR The ID specified in XADD must be greater than 0-0"
                                            .to_string(),
                                    )
                                } else {
                                    let entry = StreamEntry {
                                        id: id.clone(),
                                        fields,
                                    };
                                    let new_version = db_lock.get(&key).map(|v| v.version).unwrap_or(0) + 1;
                                    db_lock.insert(
                                        key,
                                        DbValue {
                                            value: DataType::Stream(vec![entry]),
                                            expires_at: None,
                                            version: new_version,
                                        },
                                    );
                                    Value::BulkString(id)
                                }
                            }
                        }
                    }
                }

                "xrange" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let raw_start = unpack_bulk_str(args.get(1).cloned().unwrap()).unwrap();

                    let raw_end = unpack_bulk_str(args.get(2).cloned().unwrap()).unwrap();

                    let (start_ms, start_seq) = if raw_start == "-" {
                        (0, 0)
                    } else if raw_start.contains('-') {
                        let (l, r) = raw_start.split_once('-').expect("missing hyphen");
                        (
                            l.parse::<u64>().expect("failed to parse left part"),
                            r.parse::<u64>().expect("failed to parse right part"),
                        )
                    } else {
                        (raw_start.parse::<u64>().expect("invalid start_ms"), 0)
                    };

                    let (end_ms, end_seq) = if raw_end == "+" {
                        (u64::MAX, u64::MAX)
                    } else if raw_end.contains('-') {
                        let (l, r) = raw_end.split_once('-').expect("missing hyphen");

                        (
                            l.parse::<u64>().expect("invalid end_ms"),
                            r.parse::<u64>().expect("invalid end_seq"),
                        )
                    } else {
                        (
                            raw_end.parse::<u64>().expect("invalid end_ms"),
                            u64::MAX, // Default sequence for end ID
                        )
                    };

                    let mut db_lock = db.lock().unwrap();

                    match db_lock.get_mut(&key) {
                        Some(db_val) => match &db_val.value {
                            DataType::Stream(entries) => {
                                let mut result_entries = Vec::new();

                                for entry in entries {
                                    let (e_ms_str, e_seq_str) = entry.id.split_once('-').unwrap();

                                    let entry_ms: u64 = e_ms_str.parse().unwrap();

                                    let entry_seq: u64 = e_seq_str.parse().unwrap();

                                    let is_after_start = (entry_ms > start_ms)
                                        || (entry_ms == start_ms && entry_seq >= start_seq);

                                    let is_before_end = (entry_ms < end_ms)
                                        || (entry_ms == end_ms && entry_seq <= end_seq);

                                    if is_after_start && is_before_end {
                                        let mut fields_resp = Vec::new();
                                        for (k, v) in &entry.fields {
                                            fields_resp.push(Value::BulkString(k.clone()));
                                            fields_resp.push(Value::BulkString(v.clone()));
                                        }

                                        result_entries.push(Value::Array(vec![
                                            Value::BulkString(entry.id.clone()),
                                            Value::Array(fields_resp),
                                        ]));
                                    }
                                }

                                Value::Array(result_entries)
                            }

                            _ => Value::Array(vec![]),
                        },

                        None => Value::Array(vec![]),
                    }
                }

                "xread" => {


    let mut block_ms: Option<u64> = None;
    let mut stream_args_start_index = 1;

    if let Some(first_arg) = args.get(0) {
        let block_arg = unpack_bulk_str(first_arg.clone()).unwrap();
        if block_arg.to_lowercase() == "block" {
        let ms = unpack_bulk_str(args.get(1).cloned().unwrap())
            .unwrap()
            .parse::<u64>()
            .unwrap();
        block_ms = Some(ms);
        stream_args_start_index = 3;
    }

    }

    
    

    let stream_args = &args[stream_args_start_index..];
    let num_streams = stream_args.len() / 2;
    let (keys, ids) = stream_args.split_at(num_streams);

    let mut resolved_ids : Vec<String> = Vec::new();

    for i in 0..num_streams {
        let key = unpack_bulk_str(keys.get(i).cloned().unwrap()).unwrap();
        let id = unpack_bulk_str(ids.get(i).cloned().unwrap()).unwrap();

        let resolved_id = if id.as_str() == "$" {
    let db_lock = db.lock().unwrap();

    let last_id = db_lock.get(&key).and_then(|db_val| {
        if let DataType::Stream(entries) = &db_val.value {
            entries.last().map(|e| e.id.clone())
        } else {
            None
        }
    });

    last_id.unwrap_or_else(|| "0-0".to_string())
} else {
    id
}; 

        resolved_ids.push(resolved_id);
    }

    let read_streams = || {
        let mut outer_results = Vec::new();
        let db_lock = db.lock().unwrap();

        for i in 0..num_streams {
            let key = unpack_bulk_str(keys[i].clone()).unwrap();
            let id = &resolved_ids[i];

            let (l, r) = id.split_once('-').expect("missing hyphen");
            let start_ms = l.parse::<u64>().expect("invalid start_ms");
            let start_seq = r.parse::<u64>().expect("invalid start_seq");

            if let Some(db_val) = db_lock.get(&key) {
                if let DataType::Stream(entries) = &db_val.value {
                    let mut result_entries = Vec::new();

                    for entry in entries {
                        let (e_ms_str, e_seq_str) = entry.id.split_once('-').unwrap();
                        let entry_ms = e_ms_str.parse::<u64>().unwrap();
                        let entry_seq = e_seq_str.parse::<u64>().unwrap();

                        let is_after = (entry_ms > start_ms)
                            || (entry_ms == start_ms && entry_seq > start_seq);

                        if is_after {
                            let mut fields_resp = Vec::new();
                            for (k, v) in &entry.fields {
                                fields_resp.push(Value::BulkString(k.clone()));
                                fields_resp.push(Value::BulkString(v.clone()));
                            }

                            result_entries.push(Value::Array(vec![
                                Value::BulkString(entry.id.clone()),
                                Value::Array(fields_resp),
                            ]));
                        }
                    }

                    if !result_entries.is_empty() {
                        outer_results.push(Value::Array(vec![
                            Value::BulkString(key),
                            Value::Array(result_entries),
                        ]));
                    }
                }
            }
        }

        outer_results
    };

    let mut results = read_streams();

    
    if results.is_empty() && block_ms.is_some() {
        let timeout = block_ms.unwrap();
        let start_time = std::time::Instant::now();

        loop {
            if timeout > 0 && start_time.elapsed().as_millis() as u64 >= timeout {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(20));

            results = read_streams();
            if !results.is_empty() {
                break;
            }
        }
    }

    if results.is_empty() && block_ms.is_some() {
        Value::NullArray
    } else {
        Value::Array(results)
    }
} 

        "incr" => {
           
            let arg = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

            

           let mut db_lock = db.lock().unwrap();

           match db_lock.get_mut(&arg)  {
            Some(db_val) => {
                if let DataType::String(ref current_str) = db_val.value {
                    match current_str.parse::<i64>() {
                        Ok(mut num) => {
                            num += 1;
                            db_val.value = DataType::String(num.to_string());

                            Value::Integer(num)
                        }

                        Err(_) => {
                           Value::Error("ERR value is not an integer or out of range".to_string()) 
                        }
                    }

                    
                } else {
                    panic!("Value is not a string");
                }

                
            }

            None => {
                  db_lock.insert(arg, DbValue { value: DataType::String("1".to_string()), expires_at: None, version: 0 });

                Value::Integer(1)
            }

            
           }
        }
        "watch" => {
            Value::SimpleString("OK".to_string())
        }

        "info" => {
            let role = if is_replica {"slave"} else {"master"};


           let current_offset = *master_repl_offset.lock().unwrap();
            
            Value::BulkString(format!("# Replication\r\nrole:{role}\r\nmaster_replid:{master_replid}\r\nmaster_repl_offset:{current_offset}\r\n"))
}

    "replconf" => {
    let sub_cmd = args.get(0)
        .and_then(|a| unpack_bulk_str(a.clone()).ok())
        .unwrap_or_default()
        .to_lowercase();

    if sub_cmd == "listening-port" || sub_cmd == "capa" {
        Value::SimpleString("OK".to_string())
    } else if sub_cmd == "getack" {
        let current_offset = *master_repl_offset.lock().unwrap();

        Value::Array(vec![
            Value::BulkString("REPLCONF".to_string()),
            Value::BulkString("ACK".to_string()),
            Value::BulkString(current_offset.to_string()),
        ])
    } else {
        Value::SimpleString("OK".to_string())
    }
}

 "psync" => {
    // 1. Register replica stream
    replicas.lock().unwrap().push(write_half.clone());

    // 2. Build FULLRESYNC + RDB payload
    let current_offset = *master_repl_offset.lock().unwrap();
    let fullresync = format!("+FULLRESYNC {} {}\r\n", master_replid, current_offset);
    let hex_str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656d12c0101200fa0c616f662d626173656c6f6164696e67c000fe00fb0000ff89506c7e0c9202d7";
    let bytes = hex::decode(hex_str).unwrap();
    let rdb_header = format!("${}\r\n", bytes.len());

    let mut payload = Vec::new();
    payload.extend_from_slice(fullresync.as_bytes());
    payload.extend_from_slice(rdb_header.as_bytes());
    payload.extend_from_slice(&bytes);

    // 3. Write payload using Tokio's AsyncWriteExt via try_write or synchronous socket buffer
    use tokio::io::AsyncWriteExt;
    {
        let mut writer = write_half.lock().unwrap();
        // Option 1: If write_half is std::net::TcpStream or std::io::Write:
        // let _ = writer.write_all(&payload);
        
        // Option 2: If write_half is tokio::net::TcpStream:
        // Use try_write to dump raw bytes straight to socket buffer synchronously
        let _ = writer.try_write(&payload);
    }

    Value::None
}

 "wait" => {
    let num_replicas: usize = match args.get(0).and_then(|a| unpack_bulk_str(a.clone()).ok()) {
        Some(s) => match s.parse() {
            Ok(n) => n,
            Err(_) => return Value::Error("ERR value is not an integer or out of range".to_string()),
        },

        None => return Value::Error("ERR wrong number of arguments for 'wait' command".to_string()),
    };

    let timeout_ms: u64 = match args.get(1).and_then(|a|  unpack_bulk_str(a.clone()).ok()) {
       Some(s) => match s.parse() {
           Ok(n) => n,
           Err(_) => return Value::Error("ERR value is not an integer or out of range".to_string()),
       },

       None => return Value::Error("ERR wrong number of arguments for 'wait' command".to_string()),
    };

    let connected_replicas_count = replicas.lock().unwrap().len();

    
    


    let target_offset = *master_repl_offset.lock().unwrap();

    if num_replicas == 0 || connected_replicas_count == 0{
        Value::Integer(0)
    } else if target_offset == 0{
        Value::Integer(connected_replicas_count as i64)
    } else if *current_offset > 0{
        REPLCONF GETACK * (*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n)
    }
    
    
     else {
        Value::Integer(connected_replicas_count as i64)
    }


 }
    _ => Value::Error("ERR unknown command".to_string())
}
}

  


async fn handle_conn(stream: TcpStream, db: Db, is_replica: bool, replicas: ReplicaList, is_master_connection: bool, master_repl_offset: Arc<Mutex<usize>>) {
    
   let std_stream = stream.into_std().expect("failed to convert to std stream");
let std_clone = std_stream.try_clone().expect("failed to clone std stream");

let mut stream = TcpStream::from_std(std_stream).expect("failed to convert back to tokio stream");
let writer_stream = TcpStream::from_std(std_clone).expect("failed to convert clone to tokio stream");
let write_half = Arc::new(Mutex::new(writer_stream));
    let mut handler = resp::RespHandler::new(stream);

    let mut in_transaction = false;
    let mut command_queue: Vec<Value> = Vec::new();
    let mut watched_versions: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    println!("Starting read loop");

    loop {
        println!("1. Reading value from socket...");
        let (value, bytes_read) = match handler.read_value().await {
            Ok(Some((v, bytes))) => (v,bytes),
            _ => break, // Connection closed or socket read error
        };
        println!("2. Value read successfully: {:?}", value);

        println!("Got value {:?}", value);

        let (command, args) = match extract_command(value.clone()) {
            Ok(cmd_tuple) => cmd_tuple,
            Err(_) => {
                let _ = handler.write_value(Value::Error("ERR bad protocol".to_string())).await;
                continue;
            }
        };

        let cmd_name = command.trim().to_lowercase();
        let is_getack = cmd_name == "replconf" && args.get(0).and_then(|a| unpack_bulk_str(a.clone()).ok()).map(|s| s.to_lowercase() == "getack").unwrap_or(false);


        let response = if in_transaction && cmd_name != "exec" && cmd_name != "discard" {
            if cmd_name == "watch" {
                Value::Error("ERR WATCH inside MULTI is not allowed".to_string())
            } else {
                command_queue.push(value);
                Value::SimpleString("QUEUED".to_string())
            }
        } else {
            match cmd_name.as_str() {
                "multi" => {
                    if in_transaction {
                        Value::Error("ERR MULTI calls cannot be nested".to_string())
                    } else {
                        in_transaction = true;
                        command_queue.clear();
                        Value::SimpleString("OK".to_string())
                    }
                }

                "exec" => {
                    if !in_transaction {
                        Value::Error("ERR EXEC without MULTI".to_string())
                    } else {
                        in_transaction = false;

                        let is_dirty = {
                            let db_lock = db.lock().unwrap();

                            watched_versions.iter().any(|(key, watched_ver)| {
                                // If key is deleted/missing, treat present state as distinct 
                                // from a valid positive version number to prevent false matches.
                                match db_lock.get(key) {
                                    Some(entry) => entry.version != *watched_ver,
                                    None => *watched_ver != 0,
                                }
                            })
                        };

                        // WATCH context is flushed upon EXEC regardless of outcome
                        watched_versions.clear();

                        if is_dirty {
                            command_queue.clear();
                            Value::NullArray
                        } else {
                            let mut results = Vec::new();
                            for queued_v in command_queue.drain(..) {
                                let (q_cmd, q_args) = extract_command(queued_v).unwrap();
                                let res = execute_command(&q_cmd, q_args, &db, is_replica, &replicas, &write_half, Arc::clone(&master_repl_offset)).await;
                                results.push(res);
                            }
                            Value::Array(results)
                        }
                    }
                }

                "discard" => {
                    if !in_transaction {
                        Value::Error("ERR DISCARD without MULTI".to_string())
                    } else {
                        in_transaction = false;
                        command_queue.clear();
                        watched_versions.clear(); // Reset watched keys on discard
                        Value::SimpleString("OK".to_string())
                    }
                }

                "watch" => {
                    let db_lock = db.lock().unwrap();

                    for arg in args {
                        // Safely extract string values without assuming exact RESP variant
                        if let Ok(key_str) = unpack_bulk_str(arg) {
                            let version = db_lock
                                .get(&key_str)
                                .map(|entry| entry.version)
                                .unwrap_or(0);

                            watched_versions.insert(key_str, version);
                        }
                    }
                    Value::SimpleString("OK".to_string())
                }

                "unwatch" => {
                    watched_versions.clear();
                    Value::SimpleString("OK".to_string())
                }

                c => execute_command(c, args.clone(), &db, is_replica, &replicas, &write_half, Arc::clone(&master_repl_offset)).await,
            }
        };

        

        

        if matches!(response, Value::None) {
            if is_master_connection {
                *master_repl_offset.lock().unwrap() += bytes_read;
            }
            
            continue;


        }

        if is_master_connection {
            if is_getack {
                println!("Sending GETACK response: {:?}", response);

                let write_result = handler.write_value(response).await;

                *master_repl_offset.lock().unwrap() += bytes_read;

                if write_result.is_err() {
                    break;
                }

            } else {
                println!("replica executed command silently");

                let mut offset_guard = master_repl_offset.lock().unwrap();

                *offset_guard += bytes_read;
            }

            continue;
        }

        println!("Sending value {:?}", response);

        if handler.write_value(response).await.is_err() {
            break;
        }
        
    

         
        

        
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
