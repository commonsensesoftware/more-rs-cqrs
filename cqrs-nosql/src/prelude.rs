cfg_select! {
    feature = "di" => {
        mod di;

        // the module is empty unless a storage provider is also enabled
        #[allow(unused_imports)]
        pub use di::*;
    }
    _ => {}
}
