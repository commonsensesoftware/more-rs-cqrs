#![allow(dead_code)]

mod events;
mod item;
mod order;

use cqrs::{di::CqrsExt, Clock, Repository, RepositoryError, StoreMigrator};
use cqrs_sql::PostgresExt;
use di::ServiceCollection;
use events::transcoder::events;
use item::Item;
use order::{Order, OrderError};
use std::{
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use time::{format_description, Duration, OffsetDateTime, UtcOffset};
use uuid::Uuid;

fn format_date(date: Option<SystemTime>) -> String {
    if let Some(date) = date {
        let utc = date.duration_since(UNIX_EPOCH).unwrap();
        let utc = OffsetDateTime::UNIX_EPOCH + Duration::try_from(utc).unwrap();
        let local = utc.to_offset(UtcOffset::local_offset_at(utc).unwrap());
        let format = format_description::parse("[month]/[day]/[year] [hour]:[minute]").unwrap();

        local.format(&format).unwrap()
    } else {
        String::new()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    let postgres = Postgres::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let provider = ServiceCollection::new()
        .add_cqrs(|options| {
            options.transcoders.events.push(events());

            options
                .store::<Order>()
                .in_postgres()
                .with()
                .url(url)
                .migrations();
        })
        .build_provider()
        .unwrap();
    let migrator = provider.get_required::<StoreMigrator>();
    let clock = provider.get_required::<dyn Clock>();
    let repository = provider.get_required::<Repository<Order>>();

    // 1. migrate storage which, in this case, will only use postgresql
    migrator.run().await?;

    // 2. start a new order
    let mut order = Order::draft(clock.now());

    // 3. add some items
    order.add(clock.now(), Item::new("BTSPKR", "Speaker", 30.0, 1))?;
    order.add(clock.now(), Item::new("CKBK", "Book Book", 10.0, 1))?;
    order.add(clock.now(), Item::new("PANZ", "Pans", 15.0, 3))?;
    repository.save(&mut order).await?;

    // 4. add an address
    order.ship_to(clock.now(), "123 Some Place", "Seattle-US", "98121")?;
    repository.save(&mut order).await?;

    // 5. checkout
    let transaction_id = Uuid::new_v4().simple().to_string();
    let mut order2 = repository.get(&order.id(), None).await?;

    order.checkout(clock.now(), 85.0, transaction_id.clone())?;
    repository.save(&mut order).await?;

    // simulate double-pay
    order2.checkout(clock.now(), 85.0, transaction_id.clone())?;

    // we're protected from lost updates
    if let Err(RepositoryError::Conflict(_, _)) = repository.save(&mut order2).await {
        // we don't know why there was a conflict, but we're out-of-sync; refresh and try again
        order2 = repository.get(&order.id(), None).await?;

        // now we can't checkout because the order has already been checked out
        if let Err(OrderError::CheckedOut) = order2.checkout(clock.now(), 85.0, transaction_id) {
            println!("The order has already been checked out, but you were only charged once.")
        } else {
            unreachable!("business rule violated")
        }
    } else {
        unreachable!("lost update")
    }

    // 6. ship it
    order.ship(clock.now())?;
    repository.save(&mut order).await?;

    println!(
        "Order {} was delivered to {} on {}",
        order.id(),
        &order.address().unwrap().street,
        format_date(order.fulfilled_on())
    );

    Ok(())
}
