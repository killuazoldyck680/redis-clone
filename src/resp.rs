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
}