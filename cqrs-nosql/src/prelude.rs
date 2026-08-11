cfg_select! {
    feature = "di" => {
        mod di;
        pub use di::*;
    }
    _ => {}
}
