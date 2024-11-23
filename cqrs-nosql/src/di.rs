use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "dynamodb")] {
        mod dynamodb;
        pub use dynamodb::*;
    }
}