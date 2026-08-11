cfg_select! {
    feature = "cbor" => {
        mod cbor;
        pub use cbor::Cbor;
    }
    _ => {}
}

cfg_select! {
    feature = "json" => {
        mod json;
        pub use json::Json;
    }
    _ => {}
}

cfg_select! {
    feature = "message-pack" => {
        mod message_pack;
        pub use message_pack::MessagePack;
    },
    _ => {},
}

cfg_select! {
    feature = "protobuf" => {
        mod protobuf;
        pub use protobuf::{ProtoBuf, Uuid};
    }
    _ => {}
}
