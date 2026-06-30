use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};

use  resp::Value;
use anyhow:: Result;

mod resp;

type Db = Arc<Mutex<HashMap<String,String>>>;

#[tokio::main]
async fn main() {

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db:Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok(( stream, _)) => {
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

async fn handle_conn(stream: TcpStream, db:Db) {
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

                   let mut db_lock = db.lock().unwrap();

                   db_lock.insert(key, val);

                   Value::SimpleString("OK".to_string())

                }

                "get" => {
                    let key = unpack_bulk_str(args.get(0).cloned().unwrap()).unwrap();

                    let db_lock = db.lock().unwrap();

                    match db_lock.get(&key) {
                        Some(val) => Value::BulkString(val.clone()),

                        None => Value::NullBulkString,
                    }


                }
                c => panic!("Error {c}")

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
            Ok((
                raw_cmd.to_lowercase(),
                a.into_iter().skip(1).collect(),

            ))
        },

        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}






