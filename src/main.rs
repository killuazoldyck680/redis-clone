use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};

use  resp::Value;
use anyhow:: Result;

mod resp;



#[tokio::main]
async fn main() {

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok(( stream, _)) => {
                println!("connection established");

                tokio::spawn(async move {

                    handle_conn(stream).await
                });
                
                
            }
            Err(e) => {
                println!("error: {e}")
            }
        }
    }
}

async fn handle_conn(stream: TcpStream) {
    let mut handler = resp::RespHandler::new(stream);

    println!("Starting read loop");

    loop {
        let value = handler.read_value().await.unwrap();

        println!("Got value {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                c => panic!("Cannot handle command {}", c),

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






