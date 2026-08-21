use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{DeriveInput, Error, LitInt, LitStr, Result, meta::ParseNestedMeta, parse2};

pub(crate) struct EventAttribute {
    kind: Option<LitStr>,
    version: u8,
}

impl Default for EventAttribute {
    fn default() -> Self {
        Self { kind: None, version: 1 }
    }
}

impl EventAttribute {
    fn parse(&mut self, args: ParseNestedMeta) -> Result<()> {
        if args.path.is_ident("kind") {
            self.kind = Some(args.value()?.parse()?);
            Ok(())
        } else if args.path.is_ident("version") {
            let version: Option<LitInt> = args.value()?.parse()?;

            if let Some(ver) = version {
                let value = ver.base10_parse::<u8>()?;

                // version 0 matches any version, which is meaningless for a message that is encoded and stored
                if value == 0 {
                    return Err(Error::new(ver.span(), "version must be greater than 0"));
                }

                self.version = value;
            }

            Ok(())
        } else {
            Err(args.error("unsupported argument"))
        }
    }
}

impl TryFrom<proc_macro::TokenStream> for EventAttribute {
    type Error = proc_macro::TokenStream;

    fn try_from(value: proc_macro::TokenStream) -> std::result::Result<Self, Self::Error> {
        let mut me = Self::default();
        let parser = syn::meta::parser(|args| me.parse(args));

        if let Err(error) = syn::parse::Parser::parse(parser, value) {
            Err(error.to_compile_error().into())
        } else {
            Ok(me)
        }
    }
}

pub(crate) fn expand(attribute: EventAttribute, input: TokenStream) -> TokenStream {
    if let Ok(derive) = parse2::<DeriveInput>(input.clone()) {
        let kind = attribute
            .kind
            .map(|kind| kind.to_token_stream())
            .unwrap_or_else(|| quote! { std::any::type_name::<Self>() });
        let version = attribute.version;
        let name = &derive.ident;
        let impl_ = quote! {
            impl cqrs::event::Event for #name {
                fn name(&self) -> &str {
                    std::any::type_name::<Self>().rsplit_once("::").unwrap().1
                }

                fn as_any(&self) -> &dyn std::any::Any {
                    self
                }
            }

            impl AsRef<dyn cqrs::event::Event + 'static> for #name {
                fn as_ref(&self) -> &(dyn cqrs::event::Event + 'static) {
                    self
                }
            }

            impl cqrs::message::Message for #name {
                fn schema(&self) -> cqrs::message::Schema {
                    <Self as cqrs::message::Encoded>::schema()
                }
            }

            impl cqrs::message::Encoded for #name {
                fn schema() -> cqrs::message::Schema {
                    cqrs::message::Schema::version::<#version>(#kind)
                }
            }
        };

        let mut output = quote! { #derive };

        output.extend(impl_);
        output
    } else {
        input
    }
}
