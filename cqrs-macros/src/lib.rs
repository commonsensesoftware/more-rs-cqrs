extern crate proc_macro;

mod aggregate;

mod event;
mod projectors;
mod snapshot;
mod transcode;

use proc_macro::TokenStream;
use syn::parse_macro_input;
use transcode::TranscodeAttribute;

/// Represents the metadata used to identify the function invoked when an event occurs.
///
/// # Remarks
///
/// This attribute is inert.
#[proc_macro_attribute]
pub fn when(_metadata: TokenStream, input: TokenStream) -> TokenStream {
    input
}

/// Represents the metadata used to identify a snapshot function.
///
/// # Remarks
///
/// This attribute is inert.
#[proc_macro_attribute]
pub fn snapshot(metadata: TokenStream, input: TokenStream) -> TokenStream {
    match metadata.try_into() {
        Ok(attribute) => snapshot::expand(attribute, input.into()).into(),
        Err(error) => error,
    }
}

/// Represents the metadata used to identify that a structure is an aggregate root.
///
/// # Arguments
///
/// * `id_type` - the identifier type used for `Aggregate<T>`
///
/// # Remarks
///
/// This attribute can be applied to a structure and/or structure implementation.
#[proc_macro_attribute]
pub fn aggregate(metadata: TokenStream, input: TokenStream) -> TokenStream {
    aggregate::expand(metadata.into(), input.into()).into()
}

/// Represents the metadata used to identify an event structure.
///
/// # Arguments
///
/// * `kind` - the optional type of event, which defaults to the type name
/// * `revision` - the optional revision number of the event, which defaults to `1`
#[proc_macro_attribute]
pub fn event(metadata: TokenStream, input: TokenStream) -> TokenStream {
    match metadata.try_into() {
        Ok(attribute) => event::expand(attribute, input.into()).into(),
        Err(error) => error,
    }
}

/// Represents the metadata used to generate an event transcoder factory function.
///
/// # Arguments
///
/// * `name` - the name of the generated `Transcoder` factory function, which defaults to `transcoder`
/// * `with` - the type of `Encoding` used to transcode events
///
/// # Remarks
///
/// This attribute can only be applied to a module.
#[proc_macro_attribute]
pub fn transcode(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let attribute = parse_macro_input!(metadata as TranscodeAttribute);
    transcode::expand(attribute, input.into()).into()
}

/// Represents the metadata used to generate projectors.
///
/// # Remarks
///
/// This attribute can only be applied to a module.
#[proc_macro_attribute]
pub fn projectors(_metadata: TokenStream, input: TokenStream) -> TokenStream {
    projectors::expand(input.into()).into()
}
