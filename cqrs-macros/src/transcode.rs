use parse::{Parse, ParseStream};
use proc_macro2::{Span, TokenStream};
use punctuated::Punctuated;
use quote::{ToTokens, quote};
use syn::{spanned::Spanned, *};

pub(crate) struct TranscodeAttribute {
    name: Ident,
    with: Path,
}

impl TranscodeAttribute {
    fn new(name: Ident, with: Path) -> Self {
        Self { name, with }
    }
}

impl Parse for TranscodeAttribute {
    fn parse(input: ParseStream) -> Result<Self> {
        let expressions = Punctuated::<Expr, Token![,]>::parse_terminated(input)?;
        let mut name = Ident::new("transcoder", Span::call_site());
        let mut with = None;

        for expression in expressions {
            if let Expr::Assign(assign) = expression {
                if let Expr::Path(left) = &*assign.left {
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
                        let location = assign.left.span();
                        return Err(Error::new(location, "unknown argument"));
                    }
                }
            } else {
                return Err(Error::new(expression.span(), "invalid argument"));
            }
        }

        if let Some(with) = with {
            Ok(Self::new(name, with))
        } else {
            Err(Error::new(input.span(), "the 'with' argument is required"))
        }
    }
}

pub(crate) fn expand(attribute: TranscodeAttribute, input: TokenStream) -> TokenStream {
    let span = input.span();

    if let Ok(module) = parse2::<ItemMod>(input) {
        if let Some((_, items)) = module.content {
            let event = items
                .iter()
                .filter_map(|item| {
                    if let Item::Struct(struct_) = item
                        && struct_.attrs.iter().any(|attr| {
                            attr.path().is_ident("event") || attr.path().is_ident("snapshot")
                        })
                    {
                        return Some(struct_);
                    }

                    None
                })
                .map(|struct_| &struct_.ident);
            let snapshot = items
                .iter()
                .filter_map(|item| {
                    if let Item::Struct(struct_) = item
                        && struct_
                            .attrs
                            .iter()
                            .any(|attr| attr.path().is_ident("snapshot"))
                    {
                        return Some(struct_);
                    }

                    None
                })
                .map(|struct_| &struct_.ident);
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
            let transcoders = quote! {
                /// Provides [transcoders](cqrs::message::Transcoder) for
                /// [events](cqrs::event::Event) and [snapshots](cqrs::snapshot::Snapshot).
                pub mod #name {
                    use super::*;

                    /// Creates and returns a new [transcoder](cqrs::message::Transcoder)
                    /// for all [events](cqrs::event::Event).
                    pub fn events() -> cqrs::message::Transcoder<dyn cqrs::event::Event> {
                        let mut transcoder = cqrs::event::transcoder();
                        #(transcoder.register(#encoding::<#event>::new()).unwrap();)*
                        transcoder
                    }

                    /// Creates and returns a new [transcoder](cqrs::message::Transcoder)
                    /// for all [snapshots](cqrs::snapshot::Snapshot).
                    pub fn snapshots() -> cqrs::message::Transcoder<dyn cqrs::snapshot::Snapshot> {
                        let mut transcoder = cqrs::snapshot::transcoder();
                        #(transcoder.register(#encoding::<#snapshot>::new()).unwrap();)*
                        transcoder
                    }
                }
            };

            let mut output = TokenStream::new();

            output.extend(items.into_iter().map(|item| item.to_token_stream()));
            output.extend(transcoders);
            output
        } else {
            module.to_token_stream()
        }
    } else {
        Error::new(span, "#[transcode] can only be applied to a module.").to_compile_error()
    }
}
