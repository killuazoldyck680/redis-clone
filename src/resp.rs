use std::net::TcpStream;
use bytes::{BytesMut};




pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
}

pub struct RespHandler {
    stream : TcpStream,
    buffer: BytesMut,
    
}

impl Value

impl RespHandler {
    pub fn new(stream:TcpStream) -> Self {
        RespHandler { stream, buffer:BytesMut::with_capacity(512), }


    }ll
}