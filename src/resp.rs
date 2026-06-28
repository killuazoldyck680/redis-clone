use tokio::{net::TcpStream, io::{AsyncReadExt, AsyncWriteExt}};
use bytes::BytesMut;




pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
}

pub struct RespHandler {
    stream : TcpStream,
    buffer: BytesMut,
    
}

impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            _ => panic!("Unsupported value for serialize")
        }
    }
}

impl RespHandler {
    pub fn new(stream:TcpStream) -> Self {
        RespHandler { stream, buffer:BytesMut::with_capacity(512), }


    }


    fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
        for i in 1..buffer.len() {
            if buffer [i-1] == b'\r' && buffer[i] == b'\n' {
               return Some((&buffer[0..(i-1)], i+1)); 
            }
            
        }
        return None;
    }
}