use crate::item::Item;
use cqrs::{encoding::Uuid, event, transcode};
use prost::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

trait Time: Sized {
    fn to_secs(self) -> u64;
    fn from_secs(secs: u64) -> Self;
}

impl Time for SystemTime {
    #[inline]
    fn to_secs(self) -> u64 {
        self.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    fn from_secs(secs: u64) -> Self {
        UNIX_EPOCH + Duration::from_secs(secs)
    }
}

#[transcode(with = ProtoBuf)]
mod events {
    #[event]
    #[derive(Message)]
    pub struct Drafted {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(uint64)]
        date: u64,
    }

    impl Drafted {
        pub fn on<U: Into<Uuid>>(id: U, date: SystemTime) -> Self {
            Self {
                id: Some(id.into()),
                version: Default::default(),
                date: date.to_secs(),
            }
        }

        pub fn id(&self) -> Uuid {
            self.id.unwrap_or_default()
        }

        pub fn date(&self) -> SystemTime {
            SystemTime::from_secs(self.date)
        }
    }

    #[event]
    #[derive(Message)]
    pub struct ItemAdded {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(uint64)]
        date: u64,

        #[prost(message)]
        item: Option<Item>,
    }

    impl ItemAdded {
        pub fn of<U: Into<Uuid>>(id: U, date: SystemTime, item: Item) -> Self {
            Self {
                id: Some(id.into()),
                version: Default::default(),
                date: date.to_secs(),
                item: Some(item),
            }
        }

        pub fn date(&self) -> SystemTime {
            SystemTime::from_secs(self.date)
        }

        pub fn item(&self) -> &Item {
            self.item.as_ref().unwrap()
        }
    }

    #[event]
    #[derive(Message)]
    pub struct ItemRemoved {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(uint64)]
        date: u64,

        #[prost(message)]
        item: Option<Item>,
    }

    impl ItemRemoved {
        pub fn of<U: Into<Uuid>>(id: U, date: SystemTime, item: Item) -> Self {
            Self {
                id: Some(id.into()),
                version: Default::default(),
                date: date.to_secs(),
                item: Some(item),
            }
        }

        pub fn date(&self) -> SystemTime {
            SystemTime::from_secs(self.date)
        }

        pub fn item(&self) -> &Item {
            self.item.as_ref().unwrap()
        }
    }

    #[event]
    #[derive(Message)]
    pub struct AddressUpdated {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(uint64)]
        date: u64,

        #[prost(string)]
        street: String,

        #[prost(string)]
        region: String,

        #[prost(string)]
        postal_code: String,
    }

    impl AddressUpdated {
        pub fn to<U: Into<Uuid>>(
            id: U,
            date: SystemTime,
            street: String,
            region: String,
            postal_code: String,
        ) -> Self {
            Self {
                id: Some(id.into()),
                version: Default::default(),
                date: date.to_secs(),
                street,
                region,
                postal_code,
            }
        }

        pub fn date(&self) -> SystemTime {
            SystemTime::from_secs(self.date)
        }

        pub fn street(&self) -> &str {
            &self.street
        }

        pub fn region(&self) -> &str {
            &self.region
        }

        pub fn postal_code(&self) -> &str {
            &self.postal_code
        }
    }

    #[event]
    #[derive(Message)]
    pub struct Paid {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(uint64)]
        date: u64,

        #[prost(float)]
        amount: f32,

        #[prost(string)]
        transaction_id: String,
    }

    impl Paid {
        pub fn with<U: Into<Uuid>>(
            id: U,
            date: SystemTime,
            amount: f32,
            transaction_id: String,
        ) -> Self {
            Self {
                id: Some(id.into()),
                version: Default::default(),
                date: date.to_secs(),
                amount,
                transaction_id,
            }
        }

        pub fn date(&self) -> SystemTime {
            SystemTime::from_secs(self.date)
        }

        pub fn amount(&self) -> f32 {
            self.amount
        }

        pub fn transaction_id(&self) -> &str {
            &self.transaction_id
        }
    }

    #[event]
    #[derive(Message)]
    pub struct Canceled {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(uint64)]
        date: u64,
    }

    impl Canceled {
        pub fn on<U: Into<Uuid>>(id: U, date: SystemTime) -> Self {
            Self {
                id: Some(id.into()),
                version: Default::default(),
                date: date.to_secs(),
            }
        }

        pub fn date(&self) -> SystemTime {
            SystemTime::from_secs(self.date)
        }
    }

    #[event]
    #[derive(Message)]
    pub struct Fulfilled {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(uint64)]
        date: u64,
    }

    impl Fulfilled {
        pub fn on<U: Into<Uuid>>(id: U, date: SystemTime) -> Self {
            Self {
                id: Some(id.into()),
                version: Default::default(),
                date: date.to_secs(),
            }
        }

        pub fn date(&self) -> SystemTime {
            SystemTime::from_secs(self.date)
        }
    }
}
