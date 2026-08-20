mod common;

use common::domain::{self, Credited, Debited};
use cqrs::{
    encoding::Json,
    event,
    message::{Encoded, EncodingError, Schema, Transcoder},
};
use serde::{Deserialize, Serialize};

// an event whose kind is pinned so that it survives moving or renaming the type
#[event(kind = "urn:sales:order:placed", version = 2)]
#[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
pub struct Placed {
    pub id: String,
}

mod external {
    use cqrs::{event, snapshot};
    use serde::{Deserialize, Serialize};

    #[event]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub struct Archived {
        pub id: String,
    }

    #[snapshot]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub struct Ledger {
        pub id: String,
    }
}

/// Messages used to verify how `#[transcode]` discovers events.
#[allow(dead_code)]
#[cqrs::transcode(with = Json, name = discovery, events(external::Archived, Renamed), snapshots(external::Ledger))]
mod discovered {
    // an enumeration is a message just as a structure is
    #[event]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub enum Closed {
        #[default]
        ByOwner,
        ByBank,
    }

    // the attribute is recognized whether it is imported or qualified
    #[cqrs::event]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub struct Renamed {
        pub name: String,
    }

    // a message that does not exist is not registered, which would otherwise
    // fail to compile because the registration outlived the message
    #[event]
    #[cfg(not(test))]
    #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
    pub struct Removed;

    // a nested module is scanned and is not erased; unlike the erased module, it
    // does not inherit the imports of the scope it is expanded into
    pub mod nested {
        use super::*;

        #[event]
        #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
        pub struct Frozen {
            pub reason: String,
        }
    }

    #[cfg(not(test))]
    pub mod absent {
        use super::*;

        #[event]
        #[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
        pub struct Thawed;
    }
}

#[test]
fn transcoder_should_roundtrip_event() {
    // arrange
    let transcoder = domain::transcoder::events();
    let expected = Credited::new("42", 50.0);

    // act
    let event = transcoder.encode(&expected).unwrap();
    let event = transcoder.decode(&Credited::schema(), &event).unwrap();
    let actual = event.as_any().downcast_ref::<Credited>().unwrap();

    // assert
    assert_eq!(*actual, expected);
}

#[test]
fn transcoder_should_register_enum_event() {
    // arrange
    let transcoder = discovery::events();
    let expected = Closed::ByBank;

    // act
    let event = transcoder.encode(&expected).unwrap();
    let event = transcoder.decode(&Closed::schema(), &event).unwrap();
    let actual = event.as_any().downcast_ref::<Closed>().unwrap();

    // assert
    assert_eq!(*actual, expected);
}

#[test]
fn transcoder_should_register_qualified_event() {
    // arrange
    let transcoder = discovery::events();
    let expected = Renamed { name: "Savings".into() };

    // act
    let event = transcoder.encode(&expected).unwrap();
    let event = transcoder.decode(&Renamed::schema(), &event).unwrap();
    let actual = event.as_any().downcast_ref::<Renamed>().unwrap();

    // assert
    assert_eq!(*actual, expected);
}

#[test]
fn transcoder_should_register_nested_event() {
    // arrange
    let transcoder = discovery::events();
    let expected = nested::Frozen { reason: "fraud".into() };

    // act
    let event = transcoder.encode(&expected).unwrap();
    let event = transcoder.decode(&nested::Frozen::schema(), &event).unwrap();
    let actual = event.as_any().downcast_ref::<nested::Frozen>().unwrap();

    // assert
    assert_eq!(*actual, expected);
}

#[test]
fn transcoder_should_register_external_event() {
    // arrange
    let transcoder = discovery::events();
    let expected = external::Archived { id: "42".into() };

    // act
    let event = transcoder.encode(&expected).unwrap();
    let event = transcoder.decode(&external::Archived::schema(), &event).unwrap();
    let actual = event.as_any().downcast_ref::<external::Archived>().unwrap();

    // assert
    assert_eq!(*actual, expected);
}

#[test]
fn transcoder_should_register_external_snapshot() {
    // arrange
    let transcoder = discovery::snapshots();
    let expected = external::Ledger { id: "42".into() };

    // act
    let snapshot = transcoder.encode(&expected).unwrap();
    let snapshot = transcoder.decode(&external::Ledger::schema(), &snapshot).unwrap();
    let actual = snapshot.as_any().downcast_ref::<external::Ledger>().unwrap();

    // assert
    assert_eq!(*actual, expected);
}

#[test]
fn transcoder_should_not_register_duplicate_event() {
    // arrange
    // 'Renamed' is both declared in the module and listed by the 'events' argument

    // act
    let transcoder = discovery::events();

    // assert
    assert!(transcoder.encode(&Renamed::default()).is_ok());
}

#[test]
fn encoding_version_should_use_pinned_kind() {
    // arrange
    let mut transcoder = cqrs::event::transcoder();

    transcoder.register(Json::<Placed>::version(2)).unwrap();

    let expected = Placed { id: "42".into() };

    // act
    let event = transcoder.encode(&expected).unwrap();
    let event = transcoder.decode(&Placed::schema(), &event).unwrap();
    let actual = event.as_any().downcast_ref::<Placed>().unwrap();

    // assert
    assert_eq!(Placed::schema(), Schema::new("urn:sales:order:placed", 2));
    assert_eq!(*actual, expected);
}

#[test]
fn encoding_with_schema_should_decode_previous_kind() {
    // arrange
    // the events were stored before the type was moved, so the current transcoder
    // cannot decode them; a transcoder that maps the old kind onto the type can
    let old = Schema::new("transcoder::before::Renamed", 1);
    let mut previous = cqrs::event::transcoder();

    previous.register(Json::<Renamed>::with_schema(old.clone())).unwrap();

    let expected = Renamed { name: "Savings".into() };
    let stored = discovery::events().encode(&expected).unwrap();

    // act
    let event = previous.decode(&old, &stored).unwrap();
    let actual = event.as_any().downcast_ref::<Renamed>().unwrap();

    // assert
    assert_eq!(*actual, expected);
    assert_eq!(
        discovery::events().decode(&old, &stored).err(),
        Some(EncodingError::Unregistered(old))
    );
}

#[test]
fn transcoder_merge_should_combine_disjoint_transcoders() {
    // arrange
    let mut transcoder = domain::transcoder::events();

    // act
    let result = transcoder.merge(discovery::events());

    // assert
    assert!(result.is_ok());
    assert!(transcoder.encode(&Debited::new("42", 10.0)).is_ok());
    assert!(transcoder.encode(&Closed::ByBank).is_ok());
}

#[test]
fn transcoder_merge_should_report_duplicate_schema() {
    // arrange
    let mut transcoder = Transcoder::<dyn cqrs::event::Event>::new();

    transcoder.merge(domain::transcoder::events()).unwrap();

    // act
    let result = transcoder.merge(domain::transcoder::events());

    // assert
    assert_eq!(result.unwrap_err(), EncodingError::DuplicateSchema(Credited::schema()));
}

#[test]
fn transcoder_merge_should_not_modify_on_duplicate_schema() {
    // arrange
    let mut transcoder = discovery::events();
    let mut duplicate = domain::transcoder::events();

    duplicate.register(Json::<Placed>::new()).unwrap();
    transcoder.merge(domain::transcoder::events()).unwrap();

    // act
    let result = transcoder.merge(duplicate);

    // assert
    assert!(result.is_err());
    assert_eq!(
        transcoder.encode(&Placed::default()).unwrap_err(),
        EncodingError::Unregistered(Placed::schema())
    );
}
