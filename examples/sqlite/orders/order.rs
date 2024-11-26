use crate::{events::*, item::Item};
use cqrs::{aggregate, when, Aggregate};
use std::time::SystemTime;
use uuid::Uuid;
use thiserror::Error;

enum State {
    Drafted,
    Placed,
    Canceled,
    Fulfilled,
}

impl Default for State {
    fn default() -> Self {
        Self::Drafted
    }
}

#[derive(Error, Debug, Copy, Clone, PartialEq, Eq)]
pub enum OrderError {
    #[error("insufficient funds")]
    InsufficientFunds,

    #[error("the order has already shipped")]
    AlreadyShipped,

    #[error("the order has been canceled")]
    Canceled,

    #[error("the order has already been delivered")]
    Delivered,

    #[error("the order has already been checked out")]
    CheckedOut,
}

pub type OrderResult = Result<(), OrderError>;

pub struct Address {
    pub street: String,
    pub region: String,
    pub postal_code: String,
}

pub struct Payment {
    pub date: SystemTime,
    pub amount: f32,
    pub transaction_id: String,
}

#[aggregate]
#[derive(Default)]
pub struct Order {
    state: State,
    items: Vec<Item>,
    address: Option<Address>,
    payment: Option<Payment>,
    fulfilled_on: Option<SystemTime>,
    canceled_on: Option<SystemTime>,
}

#[aggregate]
impl Order {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn draft(date: SystemTime) -> Self {
        let mut me = Self::default();
        me.record(Drafted::on(Uuid::new_v4(), date));
        me
    }

    pub fn add(&mut self, date: SystemTime, item: Item) -> OrderResult {
        match self.state {
            State::Drafted => {
                self.record(ItemAdded::of(self.id, date, item));
                Ok(())
            }
            State::Canceled => Err(OrderError::Canceled),
            State::Fulfilled => Err(OrderError::Delivered),
            State::Placed => Err(OrderError::CheckedOut),
        }
    }

    pub fn remove(&mut self, date: SystemTime, item: Item) -> OrderResult {
        match self.state {
            State::Drafted => {
                self.record(ItemRemoved::of(self.id, date, item));
                Ok(())
            }
            State::Canceled => Err(OrderError::Canceled),
            State::Fulfilled => Err(OrderError::Delivered),
            State::Placed => Err(OrderError::CheckedOut),
        }
    }

    pub fn ship_to<S: Into<String>>(
        &mut self,
        date: SystemTime,
        street: S,
        region: S,
        postal_code: S,
    ) -> OrderResult {
        match self.state {
            State::Drafted => {
                self.record(AddressUpdated::to(
                    self.id,
                    date,
                    street.into(),
                    region.into(),
                    postal_code.into(),
                ));
                Ok(())
            }
            State::Canceled => Err(OrderError::Canceled),
            State::Fulfilled => Err(OrderError::Delivered),
            State::Placed => Err(OrderError::CheckedOut),
        }
    }

    pub fn checkout<S: Into<String>>(
        &mut self,
        date: SystemTime,
        amount: f32,
        transaction_id: S,
    ) -> OrderResult {
        match self.state {
            State::Drafted => {
                let total = self.items.iter().map(|i| i.price * i.quantity as f32).sum();

                if amount < total {
                    Err(OrderError::InsufficientFunds)
                } else {
                    self.record(Paid::with(self.id, date, amount, transaction_id.into()));
                    Ok(())
                }
            }
            State::Canceled => Err(OrderError::Canceled),
            State::Fulfilled => Err(OrderError::Delivered),
            State::Placed => Err(OrderError::CheckedOut),
        }
    }

    pub fn cancel(&mut self, date: SystemTime) -> OrderResult {
        match self.state {
            State::Drafted | State::Placed => {
                self.record(Canceled::on(self.id, date));
                Ok(())
            }
            State::Canceled => Ok(()),
            State::Fulfilled => Err(OrderError::Delivered),
        }
    }

    pub fn ship(&mut self, date: SystemTime) -> OrderResult {
        match self.state {
            State::Drafted => Err(OrderError::InsufficientFunds),
            State::Placed => {
                self.record(Fulfilled::on(self.id, date));
                Ok(())
            }
            State::Canceled => Err(OrderError::Canceled),
            State::Fulfilled => Ok(()),
        }
    }

    pub fn download(&mut self, date: SystemTime) -> OrderResult {
        self.ship(date)
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }

    pub fn address(&self) -> Option<&Address> {
        self.address.as_ref()
    }

    pub fn payment(&self) -> Option<&Payment> {
        self.payment.as_ref()
    }

    pub fn fulfilled_on(&self) -> Option<SystemTime> {
        self.fulfilled_on
    }

    pub fn canceled_on(&self) -> Option<SystemTime> {
        self.canceled_on
    }

    #[when]
    fn _drafted(&mut self, event: &Drafted) {
        self.id = event.id().into();
        self.state = State::Drafted;
    }

    #[when]
    fn _item_add(&mut self, event: &ItemAdded) {
        if let Some(item) = self.items.iter_mut().find(|i| i.sku == event.item().sku) {
            item.quantity = item.quantity.saturating_add(event.item().quantity);
        } else {
            self.items.push(event.item().clone());
        }
    }

    #[when]
    fn _item_removed(&mut self, event: &ItemRemoved) {
        if let Some(item) = self.items.iter_mut().find(|i| i.sku == event.item().sku) {
            item.quantity = item.quantity.saturating_sub(event.item().quantity);
        }
    }

    #[when]
    fn _address_updated(&mut self, event: &AddressUpdated) {
        self.address = Some(Address {
            street: event.street().into(),
            region: event.region().into(),
            postal_code: event.postal_code().into(),
        });
    }

    #[when]
    fn _paid(&mut self, event: &Paid) {
        self.payment = Some(Payment {
            date: event.date(),
            amount: event.amount(),
            transaction_id: event.transaction_id().into(),
        });
        self.state = State::Placed;
    }

    #[when]
    fn _canceled(&mut self, event: &Canceled) {
        self.canceled_on = Some(event.date());
        self.state = State::Canceled;
    }

    #[when]
    fn _fulfilled(&mut self, event: &Fulfilled) {
        self.fulfilled_on = Some(event.date());
        self.state = State::Fulfilled;
    }
}
