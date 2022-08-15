use getset::Getters;

#[derive(Getters, Clone)]
pub struct KeyValueList {
    #[getset(get = "pub")]
    data: Vec<KeyValue>,
}

impl KeyValueList {
    pub fn new(data: Vec<KeyValue>) -> Self {
        KeyValueList { data }
    }
}

impl From<KeyValue> for KeyValueList {
    fn from(kv: KeyValue) -> Self {
        KeyValueList::new(vec![kv])
    }
}

#[derive(Getters, Debug, Clone)]
pub struct KeyValue {
    #[getset(get = "pub")]
    key: String,

    #[getset(get = "pub")]
    value: String,
}

impl KeyValue {
    pub fn from_str(key: &str, value: &str) -> KeyValue {
        KeyValue::from_string(key.to_string(), value.to_string())
    }

    pub fn from_string(key: String, value: String) -> KeyValue {
        KeyValue { key, value }
    }
}

unsafe impl Send for KeyValue {}

#[cfg(test)]
mod tests {
    use super::KeyValue;

    #[test]
    fn can_create_kv() {
        let key = "mykey";
        let value = "myvalue";
        let record = KeyValue::from_str(key, value);
        assert_eq!(*record.key(), key.to_string());
        assert_eq!(*record.value(), value.to_string());
    }
}
