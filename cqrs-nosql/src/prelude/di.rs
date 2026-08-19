cfg_select! {
    feature = "cosmosdb" => {
        mod cosmosdb;
        pub use cosmosdb::*;
    }
    _ => {}
}

cfg_select! {
    feature = "dynamodb" => {
        mod dynamodb;
        pub use dynamodb::*;
    }
    _ => {}
}
