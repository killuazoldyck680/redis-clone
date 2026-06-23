
use std::io::{Write,Read};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection")
            }

            Ok(mut stream) => {
                stream.write_all(b"+PONG\r\n").unwrap();
            }

            Err(e) => {
                println!("error occured {}",e)
            }
        }
    }
}
