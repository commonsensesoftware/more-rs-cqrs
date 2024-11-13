extern crate proc_macro;

mod aggregate;

#[cfg(feature = "prost")]
pub(crate) mod config;

mod event;
mod projectors;
mod snapshot;
mod transcode;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parser, parse_macro_input, Field};
use transcode::TranscodeAttribute;

pub(crate) fn new_version_field() -> Field {
    cfg_if::cfg_if! {
        if #[cfg(feature = "prost")] {
            let config = crate::config::get();
            let tag = config.fields.prost.version.tag.to_string();

            Field::parse_named.parse2(quote! {
                #[prost(message, tag = #tag)]
                version: Option<cqrs::Version>
            }).unwrap()
        } else {
            Field::parse_named.parse2(quote! { version: cqrs::Version }).unwrap()
        }
    }
}

pub(crate) fn new_version_getter() -> proc_macro2::TokenStream {
    cfg_if::cfg_if! {
        if #[cfg(feature = "prost")] {
            quote! {
                fn version(&self) -> cqrs::Version {
                    self.version.unwrap_or_default()
                }
            }
        } else {
            quote! {
                fn version(&self) -> cqrs::Version {
                    self.version
                }
            }
        }
    }
}

pub(crate) fn new_version_setter() -> proc_macro2::TokenStream {
    cfg_if::cfg_if! {
        if #[cfg(feature = "prost")] {
            quote! {
                fn set_version(&mut self, version: cqrs::Version) {
                    self.version = Some(version);
                }
            }
        } else {
            quote! {
                fn set_version(&mut self, version: cqrs::Version) {
                    self.version = version;
                }
            }
        }
    }
}

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
