use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "cbor")] {
        mod cbor;
        pub use cbor::Cbor;
    }
}

cfg_if! {
    if #[cfg(feature = "json")] {
        mod json;
        pub use json::Json;
    }
}

cfg_if! {
    if #[cfg(feature = "message-pack")] {
        mod message_pack;
        pub use message_pack::MessagePack;
    }
}

cfg_if! {
    if #[cfg(feature = "protobuf")] {
        mod protobuf;
        pub use protobuf::{ProtoBuf, Uuid};
    }
}
