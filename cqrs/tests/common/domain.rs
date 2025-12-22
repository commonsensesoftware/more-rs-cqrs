use cqrs::{aggregate, event, snapshot, transcode, when};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

// since this is a test crate, the test configuration needs to be
// specified in order to expand macros
//
// RUSTFLAGS='--cfg test' cargo expand
// $env:RUSTFLAGS='--cfg test' ; cargo expand

#[transcode(with = Json)]
mod events {
    #[event]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub struct Debited {
        pub id: String,
        pub amount: f32,
    }

    impl Debited {
        pub fn new<S: Into<String>>(id: S, amount: f32) -> Self {
            Self {
                id: id.into(),
                amount,
            }
        }
    }

    #[event]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub struct Credited {
        pub id: String,
        pub amount: f32,
    }

    impl Credited {
        pub fn new<S: Into<String>>(id: S, amount: f32) -> Self {
            Self {
                id: id.into(),
                amount,
            }
        }
    }

    #[snapshot]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub struct Statement {
        pub id: String,
        pub balance: f32,
        pub date: u64,
    }

    impl Statement {
        fn new<S: Into<String>>(id: S, balance: f32) -> Self {
            Self {
                id: id.into(),
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
        self.record(Credited::new(&self.id, amount));
    }

    pub fn debit(&mut self, amount: f32) {
        self.record(Debited::new(&self.id, amount));
    }

    #[when]
    fn debited(&mut self, event: &Debited) {
        self.id = event.id.clone();
        self.balance -= event.amount;
    }

    #[when]
    fn credited(&mut self, event: &Credited) {
        self.id = event.id.clone();
        self.balance += event.amount;
    }

    #[when]
    fn statement_received(&mut self, snapshot: &Statement) {
        self.id = snapshot.id.clone();
        self.balance = snapshot.balance();
    }

    fn new_snapshot(&self) -> Statement {
        Statement::new(&self.id, self.balance)
    }
}
