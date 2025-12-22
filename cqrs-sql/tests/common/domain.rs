#![allow(dead_code)]

use cqrs::{aggregate, event, snapshot, transcode, when};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter, Result as FormatResult},
    time::{Duration, SystemTime},
};
use thiserror::Error;

#[inline]
fn to_secs(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[inline]

fn from_secs(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

macro_rules! event_impl {
    ($type:ty) => {
        impl $type {
            pub fn new(id: String, date: SystemTime) -> Self {
                Self {
                    id,
                    date: to_secs(date),
                }
            }

            pub fn id(&self) -> &str {
                &self.id
            }

            pub fn date(&self) -> SystemTime {
                from_secs(self.date)
            }
        }
    };
    ($type:ty, $amount:ident) => {
        impl $type {
            pub fn new(id: String, $amount: f32, date: SystemTime) -> Self {
                Self {
                    id,
                    $amount,
                    date: to_secs(date),
                }
            }

            pub fn id(&self) -> &str {
                &self.id
            }

            pub fn $amount(&self) -> f32 {
                self.$amount
            }

            pub fn date(&self) -> SystemTime {
                from_secs(self.date)
            }
        }
    };
}

#[transcode(with = Json)]
mod events {
    #[event]
    #[derive(Default, Deserialize, Serialize)]
    pub struct Opened {
        id: String,
        amount: f32,
        date: u64,
    }

    #[event]
    #[derive(Default, Deserialize, Serialize)]
    pub struct Debited {
        id: String,
        amount: f32,
        date: u64,
    }

    #[event]
    #[derive(Default, Deserialize, Serialize)]
    pub struct Credited {
        id: String,
        amount: f32,
        date: u64,
    }

    #[event]
    #[derive(Default, Deserialize, Serialize)]
    pub struct Closed {
        id: String,
        date: u64,
    }

    #[snapshot]
    #[derive(Default, Deserialize, Serialize)]
    pub struct Statement {
        id: String,
        balance: f32,
        date: u64,
    }

    event_impl!(Opened, amount);
    event_impl!(Debited, amount);
    event_impl!(Credited, amount);
    event_impl!(Closed);
    event_impl!(Statement, balance);
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Open,
    Closed,
}

impl Default for Status {
    fn default() -> Self {
        Self::Open
    }
}

#[derive(Error, Copy, Clone, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("insufficient funds")]
    InsufficientFunds,

    #[error("account closed")]
    AccountClosed,
}

pub type Transaction = Result<(), Error>;

#[aggregate(String)]
#[derive(Default)]
pub struct Account {
    balance: f32,
    status: Status,
}

#[aggregate(String)]
impl Account {
    pub fn balance(&self) -> f32 {
        self.balance
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn open<S: Into<String>>(id: S, amount: f32) -> Self {
        let mut me = Self::default();
        let id = id.into();

        me.record(Opened::new(id.clone(), amount, me.clock.now()));
        me
    }

    pub fn credit(&mut self, amount: f32) -> Transaction {
        if self.status == Status::Closed {
            Err(Error::AccountClosed)
        } else {
            self.record(Credited::new(self.id.clone(), amount, self.clock.now()));
            Ok(())
        }
    }

    pub fn debit(&mut self, amount: f32) -> Transaction {
        if self.status == Status::Closed {
            Err(Error::AccountClosed)
        } else if (self.balance - amount) < 0.0 {
            Err(Error::InsufficientFunds)
        } else {
            self.record(Debited::new(self.id.clone(), amount, self.clock.now()));
            Ok(())
        }
    }

    #[when]
    fn opened(&mut self, event: &Opened) {
        self.id = event.id.clone();
        self.balance += event.amount;
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
    fn start_from_statement(&mut self, statement: &Statement) {
        self.id = statement.id.clone();
        self.balance = statement.balance();
    }

    fn new_snapshot(&self) -> Statement {
        Statement::new(self.id.clone(), self.balance, self.clock.now())
    }
}

impl Debug for Account {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        f.debug_struct("Account")
            .field("balance", &self.balance)
            .field("status", &self.status)
            .field("id", &self.id)
            .field("version", &self.version)
            .field("events", &self.events.len())
            .field("clock", &self.clock)
            .finish()
    }
}
