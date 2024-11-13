use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{meta::ParseNestedMeta, spanned::Spanned, *};

pub(crate) struct EventAttribute {
    kind: Option<LitStr>,
    version: u8,
}

impl Default for EventAttribute {
    fn default() -> Self {
        Self {
            kind: None,
            version: 1,
        }
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
                self.version = ver.base10_parse::<u8>()?;
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
    let mut derive = parse2::<DeriveInput>(input).unwrap();
    let mut invalid = true;

    if let Data::Struct(ref mut struct_) = derive.data {
        if let Fields::Named(ref mut fields) = struct_.fields {
            fields.named.push(crate::new_version_field());
            invalid = false;
        }
    }

    if invalid {
        return Error::new(derive.span(), "unable to implement the Event trait.")
            .to_compile_error();
    }

    let kind = attribute
        .kind
        .map(|kind| kind.to_token_stream())
        .unwrap_or_else(|| quote! { std::any::type_name::<Self>() });
    let version = attribute.version;
    let name = &derive.ident;
    let getter = crate::new_version_getter();
    let setter = crate::new_version_setter();
    let impl_ = quote! {
        impl cqrs::event::Event for #name {
            fn name(&self) -> &str {
                std::any::type_name::<Self>().rsplit_once("::").unwrap().1
            }

            #getter

            #setter

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
                cqrs::message::Schema::new(#kind, #version)
            }
        }
    };

    let mut output = quote! { #derive };

    output.extend(impl_);
    output
}
