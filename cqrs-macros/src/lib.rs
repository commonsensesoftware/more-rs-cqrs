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
/// * `events` - the optional paths of additional events declared outside of the module
/// * `snapshots` - the optional paths of additional snapshots declared outside of the module
///
/// # Remarks
///
/// This attribute can only be applied to an inline module. All of the structures, enumerations, and unions annotated
/// with `#[event]` or `#[snapshot]`, including those declared by a nested module, are registered with the generated
/// transcoders. Messages declared elsewhere, such as in another file or crate, are registered by listing their paths
/// with the `events` and `snapshots` arguments.
///
/// The module itself is erased and its items are expanded into the enclosing scope; Rust does not currently support an
/// inner attribute macro (for example, `#![transcode]`), which would otherwise make the module unnecessary. The
/// following are honored when the module is erased:
///
/// * Attributes, such as `#[cfg]` or `#[allow]`, are applied to each of the expanded items and the generated module
/// * Documentation is forwarded to the generated module
/// * A `cfg` gating an individual message also gates its registration
///
/// The following cannot be honored and are ignored:
///
/// * The visibility of the module; the visibility of each expanded item is used instead
/// * The scope of any `use` statement, which is expanded into the enclosing scope
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
