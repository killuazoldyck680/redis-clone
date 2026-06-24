



pub enum Value {
    SimpleString: String,
    BulkString: String,
    Array: Vec(<Value>),
}