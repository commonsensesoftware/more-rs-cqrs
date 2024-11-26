use prost::Message;

#[derive(Message, Clone)]
pub struct Item {
    #[prost(string)]
    pub sku: String,

    #[prost(string)]
    pub name: String,

    #[prost(float)]
    pub price: f32,

    #[prost(uint32)]
    pub quantity: u32,
}

impl Item {
    pub fn new<S: Into<String>>(
        sku: S,
        name: S,
        price: f32,
        quantity: u32
    ) -> Self {
        Self {
            sku: sku.into(),
            name: name.into(),
            price,
            quantity,
        }
    }
}