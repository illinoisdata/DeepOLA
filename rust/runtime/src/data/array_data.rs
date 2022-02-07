use getset::Getters;

#[derive(Getters, Debug, Clone)]
pub struct ArrayData {

    #[getset(get = "pub")]
    key_index: usize,

    #[getset(get = "pub")]
    array: Vec<String>,
}

impl ArrayData {
    pub fn key(&self) -> String {
        self.array[self.key_index].clone()
    }
}
