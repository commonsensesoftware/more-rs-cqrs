use parse::{Parse, ParseStream};
use proc_macro2::{Span, TokenStream};
use punctuated::Punctuated;
use quote::{ToTokens, quote};
use std::collections::HashSet;
use syn::{spanned::Spanned, *};

// the attributes that only have meaning on a module and, therefore, cannot be honored when the module is erased
const MODULE_ONLY: [&str; 3] = ["path", "macro_use", "no_implicit_prelude"];

pub(crate) struct TranscodeAttribute {
    name: Ident,
    with: Path,
    events: Vec<Path>,
    snapshots: Vec<Path>,
}

impl TranscodeAttribute {
    fn new(name: Ident, with: Path, events: Vec<Path>, snapshots: Vec<Path>) -> Self {
        Self {
            name,
            with,
            events,
            snapshots,
        }
    }
}

impl Parse for TranscodeAttribute {
    fn parse(input: ParseStream) -> Result<Self> {
        let expressions = Punctuated::<Expr, Token![,]>::parse_terminated(input)?;
        let mut name = Ident::new("transcoder", Span::call_site());
        let mut with = None;
        let mut events = Vec::new();
        let mut snapshots = Vec::new();

        for expression in expressions {
            match expression {
                Expr::Assign(assign) => {
                    let Expr::Path(left) = &*assign.left else {
                        return Err(Error::new(assign.left.span(), "unknown argument"));
                    };

                    if left.path.is_ident("name") {
                        let location = assign.right.span();

                        if let Expr::Path(right) = *assign.right {
                            name = right.path.require_ident()?.clone();
                        } else {
                            return Err(Error::new(location, "invalid name"));
                        }
                    } else if left.path.is_ident("with") {
                        let location = assign.right.span();

                        if let Expr::Path(right) = *assign.right {
                            with = Some(right.path);
                        } else {
                            return Err(Error::new(location, "invalid encoding"));
                        }
                    } else {
                        return Err(Error::new(left.span(), "unknown argument"));
                    }
                }
                Expr::Call(call) => {
                    let Expr::Path(function) = &*call.func else {
                        return Err(Error::new(call.func.span(), "unknown argument"));
                    };
                    let paths = if function.path.is_ident("events") {
                        &mut events
                    } else if function.path.is_ident("snapshots") {
                        &mut snapshots
                    } else {
                        return Err(Error::new(function.span(), "unknown argument"));
                    };

                    for argument in call.args {
                        if let Expr::Path(path) = argument {
                            paths.push(path.path);
                        } else {
                            return Err(Error::new(argument.span(), "invalid type"));
                        }
                    }
                }
                _ => return Err(Error::new(expression.span(), "invalid argument")),
            }
        }

        if let Some(with) = with {
            Ok(Self::new(name, with, events, snapshots))
        } else {
            Err(Error::new(input.span(), "the 'with' argument is required"))
        }
    }
}

/// Represents a message registered with a transcoder.
#[derive(Clone)]
struct Registration {
    /// The path of the message type, relative to the scope the module is expanded into.
    path: TokenStream,

    /// The `cfg` attributes the message is gated behind, if any.
    cfg: Vec<Attribute>,
}

impl Registration {
    fn new(path: TokenStream, cfg: Vec<Attribute>) -> Self {
        Self { path, cfg }
    }

    fn expand(&self, encoding: &TokenStream) -> TokenStream {
        let Self { path, cfg } = self;
        quote! { #(#cfg)* transcoder.register(#encoding::<#path>::new()).unwrap(); }
    }
}

/// Collects the `cfg` attributes, which determine whether an item exists at all.
fn cfg(attrs: &[Attribute]) -> impl Iterator<Item = Attribute> {
    attrs.iter().filter(|attr| attr.path().is_ident("cfg")).cloned()
}

/// Matches the last path segment so that a qualified attribute, such as `#[cqrs::event]`, is recognized just like
/// its imported form.
fn marker(attrs: &[Attribute], name: &str) -> bool {
    attrs
        .iter()
        .filter_map(|attr| attr.path().segments.last())
        .any(|segment| segment.ident == name)
}

/// Collects the messages defined by a module, including any nested module.
fn scan(
    items: &[Item],
    prefix: &mut Vec<Ident>,
    gates: &mut Vec<Attribute>,
    events: &mut Vec<Registration>,
    snapshots: &mut Vec<Registration>,
) {
    for item in items {
        let (ident, attrs) = match item {
            Item::Struct(struct_) => (&struct_.ident, &struct_.attrs),
            Item::Enum(enum_) => (&enum_.ident, &enum_.attrs),
            Item::Union(union_) => (&union_.ident, &union_.attrs),
            Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    let depth = gates.len();

                    gates.extend(cfg(&module.attrs));
                    prefix.push(module.ident.clone());
                    scan(items, prefix, gates, events, snapshots);
                    prefix.pop();
                    gates.truncate(depth);
                }

                continue;
            }
            _ => continue,
        };
        let snapshot = marker(attrs, "snapshot");

        if !snapshot && !marker(attrs, "event") {
            continue;
        }

        let mut cfg = gates.clone();

        cfg.extend(self::cfg(attrs));

        let registration = Registration::new(quote! { #(#prefix::)* #ident }, cfg);

        if snapshot {
            snapshots.push(registration.clone());
        }

        // a snapshot is an event as well and is registered with both transcoders
        events.push(registration);
    }
}

/// Expands the registrations for a transcoder, discarding any duplicate.
fn expand_all(registrations: &[Registration], encoding: &TokenStream) -> Vec<TokenStream> {
    let mut seen = HashSet::new();

    registrations
        .iter()
        .filter(|registration| seen.insert(registration.path.to_string()))
        .map(|registration| registration.expand(encoding))
        .collect()
}

pub(crate) fn expand(attribute: TranscodeAttribute, input: TokenStream) -> TokenStream {
    let span = input.span();
    let Ok(module) = parse2::<ItemMod>(input) else {
        return Error::new(span, "#[transcode] can only be applied to a module.").to_compile_error();
    };
    let Some((_, items)) = module.content else {
        return Error::new(
            module.span(),
            "#[transcode] can only be applied to an inline module; \
             the module contents must be declared in braces.",
        )
        .to_compile_error();
    };

    // the module is erased, so its attributes are applied to each of the items it contained. documentation has no such
    // home and is forwarded to the generated module, which is the only artifact of the module that survives expansion
    let mut docs = Vec::new();
    let mut inherited = Vec::new();

    for attr in &module.attrs {
        let path = attr.path();

        if path.is_ident("doc") {
            docs.push(attr.clone());
        } else if let Some(name) = path
            .get_ident()
            .and_then(|ident| MODULE_ONLY.iter().find(|name| ident == *name))
        {
            return Error::new(
                attr.span(),
                format!("#[{name}] cannot be applied to a #[transcode] module because the module is erased."),
            )
            .to_compile_error();
        } else {
            inherited.push(attr.clone());
        }
    }

    let mut events = Vec::new();
    let mut snapshots = Vec::new();

    scan(
        &items,
        &mut Vec::new(),
        &mut cfg(&module.attrs).collect(),
        &mut events,
        &mut snapshots,
    );

    // a snapshot declared elsewhere is an event too, just as it is when scanned
    events.extend(
        attribute
            .events
            .iter()
            .chain(attribute.snapshots.iter())
            .map(|path| Registration::new(path.to_token_stream(), Vec::new())),
    );
    snapshots.extend(
        attribute
            .snapshots
            .iter()
            .map(|path| Registration::new(path.to_token_stream(), Vec::new())),
    );

    let name = attribute.name;
    let with = attribute.with;
    let encoding = if with.is_ident("ProtoBuf")
        || with.is_ident("Json")
        || with.is_ident("MessagePack")
        || with.is_ident("Cbor")
    {
        quote! { cqrs::encoding::#with }
    } else {
        with.to_token_stream()
    };
    let events = expand_all(&events, &encoding);
    let snapshots = expand_all(&snapshots, &encoding);
    let separator = if docs.is_empty() {
        TokenStream::new()
    } else {
        quote! { #[doc = ""] }
    };
    let transcoders = quote! {
        #(#docs)*
        #separator
        /// Provides [transcoders](cqrs::message::Transcoder) for
        /// [events](cqrs::event::Event) and [snapshots](cqrs::snapshot::Snapshot).
        #(#inherited)*
        pub mod #name {
            use super::*;

            /// Creates and returns a new [transcoder](cqrs::message::Transcoder)
            /// for all [events](cqrs::event::Event).
            #[allow(unused_mut)]
            pub fn events() -> cqrs::message::Transcoder<dyn cqrs::event::Event> {
                let mut transcoder = cqrs::event::transcoder();
                #(#events)*
                transcoder
            }

            /// Creates and returns a new [transcoder](cqrs::message::Transcoder)
            /// for all [snapshots](cqrs::snapshot::Snapshot).
            #[allow(unused_mut)]
            pub fn snapshots() -> cqrs::message::Transcoder<dyn cqrs::snapshot::Snapshot> {
                let mut transcoder = cqrs::snapshot::transcoder();
                #(#snapshots)*
                transcoder
            }
        }
    };

    let mut output = TokenStream::new();

    output.extend(items.into_iter().map(|item| quote! { #(#inherited)* #item }));
    output.extend(transcoders);
    output
}
