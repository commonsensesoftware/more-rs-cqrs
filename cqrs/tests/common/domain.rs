use cqrs::{aggregate, event, snapshot, transcode, when, Aggregate, Version};
use prost::Message;
use std::time::{Duration, SystemTime};

// since this is a test crate, the test configuration needs to be
// specified in order to expand macros
//
// RUSTFLAGS='--cfg test' cargo expand
// $env:RUSTFLAGS='--cfg test' ; cargo expand

#[transcode(with = cqrs::encoding::ProtoBuf)]
mod events {
    #[event]
    #[derive(Message, PartialEq)]
    pub struct Debited {
        #[prost(string)]
        pub id: String,

        #[prost(float)]
        pub amount: f32,
    }

    impl Debited {
        pub fn new<S: Into<String>>(id: S, version: Version, amount: f32) -> Self {
            Self {
                id: id.into(),
                version: Some(version),
                amount,
            }
        }
    }

    #[event]
    #[derive(Message, PartialEq)]
    pub struct Credited {
        #[prost(string)]
        pub id: String,

        #[prost(float)]
        pub amount: f32,
    }

    impl Credited {
        pub fn new<S: Into<String>>(id: S, version: Version, amount: f32) -> Self {
            Self {
                id: id.into(),
                version: Some(version),
                amount,
            }
        }
    }

    #[snapshot]
    #[derive(Message, PartialEq)]
    pub struct Statement {
        #[prost(string)]
        pub id: String,

        #[prost(float)]
        pub balance: f32,

        #[prost(uint64)]
        pub date: u64,
    }

    impl Statement {
        fn new<S: Into<String>>(id: S, version: Version, balance: f32) -> Self {
            Self {
                id: id.into(),
                version: Some(version),
                balance,
                date: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        }

        pub fn balance(&self) -> f32 {
            self.balance
        }

        #[allow(dead_code)]
        pub fn date(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(self.date)
        }
    }
}

#[aggregate(String)]
#[derive(Default)]
pub struct Account {
    pub balance: f32,
}

#[aggregate(String)]
impl Account {
    pub fn open<S: Into<String>>(id: S) -> Self {
        let mut me = Self::default();
        me.id = id.into();
        me
    }

    pub fn credit(&mut self, amount: f32) {
        self.record(Credited::new(&self.id, self.version, amount));
    }

    pub fn debit(&mut self, amount: f32) {
        self.record(Debited::new(&self.id, self.version, amount));
    }

    #[when]
    fn apply_debit(&mut self, event: &Debited) {
        self.id = event.id.clone();
        self.balance -= event.amount;
    }

    #[when]
    fn apply_credit(&mut self, event: &Credited) {
        self.id = event.id.clone();
        self.balance += event.amount;
    }

    #[when]
    fn apply_snapshot(&mut self, snapshot: &Statement) {
        self.id = snapshot.id.clone();
        self.balance = snapshot.balance();
    }

    fn snapshot(&self) -> Statement {
        Statement::new(&self.id, self.version, self.balance)
    }
}
