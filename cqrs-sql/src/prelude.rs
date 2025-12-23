use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "di")] {
        mod di;
        pub use di::*;
    }
}