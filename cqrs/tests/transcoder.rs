mod common;

use common::domain::{self, Credited};
use cqrs::message::Encoded;

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
