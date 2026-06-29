

use tokio::net::{TcpListener, TcpStream};

use  resp::Value;
use anyhow::Result;

mod resp;



#[tokio::main]
async fn main() {

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((mut stream, _)) => {
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




