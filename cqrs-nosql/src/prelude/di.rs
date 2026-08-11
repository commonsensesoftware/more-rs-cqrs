cfg_select! {
    feature = "dynamodb" => {
        mod dynamodb;
        pub use dynamodb::*;
    }
    _ => {}
}