use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream, Parser},
    spanned::Spanned,
    *,
};

struct AggregateAttribute {
    id: Path,
}

impl Parse for AggregateAttribute {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            id: input
                .parse::<Path>()
                .ok()
                .unwrap_or_else(|| parse_str::<Path>("uuid::Uuid").unwrap()),
        })
    }
}

pub(crate) fn expand(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let original = input.clone();
    let result = match parse2::<AggregateAttribute>(metadata) {
        Ok(attribute) => {
            if let Ok(struct_) = parse2::<ItemStruct>(input.clone()) {
                Ok(implement_struct(struct_, &attribute))
            } else if let Ok(impl_) = parse2::<ItemImpl>(input.clone()) {
                implement_trait(original, &impl_, &attribute)
            } else {
                Err(Error::new(
                    original.span(),
                    "#[aggregate] can only be applied to a structure or a structure implementation block",
                ))
            }
        }
        Err(error) => Err(error),
    };

    match result {
        Ok(output) => output,
        Err(error) => error.to_compile_error(),
    }
}

fn implement_struct(mut struct_: ItemStruct, attribute: &AggregateAttribute) -> TokenStream {
    if let Fields::Named(ref mut fields) = struct_.fields {
        let id = &attribute.id;
        fields
            .named
            .push(Field::parse_named.parse2(quote! { id: #id }).unwrap());
        fields.named.push(
            Field::parse_named
                .parse2(quote! { version: cqrs::Version })
                .unwrap(),
        );
        fields.named.push(
            Field::parse_named
                .parse2(quote! { events: Vec<Box<dyn cqrs::event::Event>> })
                .unwrap(),
        );
        fields.named.push(
            Field::parse_named
                .parse2(quote! { clock: cqrs::ClockHolder })
                .unwrap(),
        );
    }

    quote! { #struct_ }
}

fn implement_trait(
    mut output: TokenStream,
    impl_: &ItemImpl,
    attribute: &AggregateAttribute,
) -> Result<TokenStream> {
    if let Type::Path(type_) = &*impl_.self_ty {
        let struct_ = &type_.path;
        let id = &attribute.id;
        let map = new_function_to_event_map(impl_)?;
        let catch_all = if let Some(method) = get_catch_all(impl_)? {
            quote! {
                self.#method(event);
            }
        } else {
            quote! {
                panic!(
                    "aggregate {} does not handle the {} event",
                    std::any::type_name::<Self>().rsplit_once("::").unwrap().1,
                    event.name());
            }
        };
        let func = map.iter().map(|i| i.0);
        let event = map.iter().map(|i| i.1);
        let snapshot = if let Some(body) = get_snapshot_function(impl_)? {
            quote! {
                fn snapshot(&self) -> Option<Box<dyn cqrs::snapshot::Snapshot>> {
                    #body
                }
            }
        } else {
            TokenStream::new()
        };
        let trait_ = quote! {
            impl #struct_ {
                /// Records a new event.
                ///
                /// # Arguments
                ///
                /// * `event` - the [event](cqrs::event::Event) to record.
                fn record<E: cqrs::event::Event + 'static>(&mut self, event: E) {
                    use cqrs::Aggregate;
                    self.replay(&event);
                    self.events.push(Box::new(event));
                }
            }

            impl cqrs::Aggregate for #struct_ {
                type ID = #id;

                fn id(&self) -> &Self::ID {
                    &self.id
                }

                fn version(&self) -> cqrs::Version {
                    self.version
                }

                fn changes(&mut self) -> cqrs::ChangeSet<'_> {
                    cqrs::ChangeSet::new(&mut self.events, &mut self.version)
                }

                fn set_clock(&mut self, clock: std::sync::Arc<dyn cqrs::Clock>) {
                    self.clock.hold(clock)
                }

                fn replay(&mut self, event: &dyn cqrs::event::Event) {
                    let any = event.as_any();
                    let id = any.type_id();

                    #(if id == std::any::TypeId::of::<#event>() {
                        self.#func(any.downcast_ref::<#event>().unwrap());
                        return;
                    })*

                    #catch_all
                }

                // implementing this correctly requires the consuming crate to depend on 'async_trait' which may not
                // be obvious. if they don't, then #[async_trait] has no effect. this will be the same for all
                // implementations so is used verbatim from #[async_trait]. to regenerate this:
                //
                // 1. 'cargo add async_trait' to the source crate (ex: tests)
                // 2. apply '#[async_trait::async_trait]' to this 'impl' block
                // 3. 'cargo expand <module> --package <crate>'
                //    a. if expand isn't available, 'cargo install expand'

                #[allow(
                    elided_named_lifetimes,
                    clippy::async_yields_async,
                    clippy::diverging_sub_expression,
                    clippy::let_unit_value,
                    clippy::no_effect_underscore_binding,
                    clippy::shadow_same,
                    clippy::type_complexity,
                    clippy::type_repetition_in_bounds,
                    clippy::used_underscore_binding
                )]
                fn replay_all<'life0, 'life1, 'async_trait>(
                    &'life0 mut self,
                    history: &'life1 mut cqrs::EventHistory,
                ) -> ::core::pin::Pin<
                    Box<
                        dyn ::core::future::Future<
                            Output = Result<(), Box<dyn std::error::Error + Send>>,
                        > + ::core::marker::Send + 'async_trait,
                    >,
                >
                where
                    'life0: 'async_trait,
                    'life1: 'async_trait,
                    Self: 'async_trait,
                {
                    Box::pin(async move {
                        if let ::core::option::Option::Some(__ret) = ::core::option::Option::None::<
                            Result<(), Box<dyn std::error::Error + Send>>,
                        > {
                            #[allow(unreachable_code)] return __ret;
                        }
                        let mut __self = self;
                        let __ret: Result<(), Box<dyn std::error::Error + Send>> = {
                            use futures::StreamExt;
                            while let Some(result) = history.next().await {
                                let saved = result?;
                                __self.replay(saved.message().as_ref());
                                __self.version = saved.version().unwrap_or_default();
                            }
                            let uncommitted = std::mem::take(&mut __self.events);
                            for event in &uncommitted {
                                __self.replay(event.as_ref());
                            }
                            __self.events = uncommitted;
                            Ok(())
                        };
                        #[allow(unreachable_code)] __ret
                    })
                }

                #snapshot
            }
        };

        output.extend(trait_);
        Ok(output)
    } else {
        Err(Error::new(
            impl_.span(),
            "invalid or unexpected structure implementation block for #[aggregate]",
        ))
    }
}

fn new_function_to_event_map(impl_: &ItemImpl) -> Result<Vec<(&Ident, &Ident)>> {
    let mut map = Vec::new();

    for item in &impl_.items {
        if let ImplItem::Fn(func) = item {
            let signature = &func.sig;
            let args: Vec<_> = signature.inputs.iter().collect();

            if func.attrs.iter().any(|a| a.path().is_ident("when")) {
                if let Some(event) = get_mapped_function(&args) {
                    map.push((&signature.ident, event));
                } else {
                    return Err(Error::new(
                        signature.span(),
                        "#[when] can only be applied to a function with the signature: fn <name>(&mut self, &<type>)",
                    ));
                }
            }
        }
    }

    Ok(map)
}

fn get_mapped_function<'a>(args: &[&'a FnArg]) -> Option<&'a Ident> {
    if args.len() == 2 {
        if let FnArg::Receiver(self_) = args[0] {
            if self_.reference.is_some() && self_.mutability.is_some() {
                if let FnArg::Typed(event) = args[1] {
                    if let Type::Reference(ref_) = event.ty.as_ref() {
                        if ref_.mutability.is_none() {
                            if let Type::Path(type_) = ref_.elem.as_ref() {
                                return type_.path.get_ident();
                            }
                        }
                    }
                }
            }
        }
    }

    None
}

fn get_catch_all(impl_: &ItemImpl) -> Result<Option<&Ident>> {
    let mut catch_all = None;

    for item in &impl_.items {
        if let ImplItem::Fn(func) = item {
            let signature = &func.sig;
            let args: Vec<_> = signature.inputs.iter().collect();

            if args.len() != 2 {
                continue;
            }

            let event_type = Ident::new("Event", Span::call_site());

            if let FnArg::Receiver(self_) = args[0] {
                if self_.reference.is_some() && self_.mutability.is_some() {
                    if let FnArg::Typed(event) = args[1] {
                        if let Type::Reference(ref_) = event.ty.as_ref() {
                            if ref_.mutability.is_none() {
                                if let Type::TraitObject(trait_) = ref_.elem.as_ref() {
                                    if trait_.bounds.len() == 1 {
                                        if let TypeParamBound::Trait(type_) =
                                            trait_.bounds.first().unwrap()
                                        {
                                            if catch_all.is_none()
                                                && type_.path.segments.last().unwrap().ident
                                                    == event_type
                                            {
                                                catch_all = Some(&signature.ident);
                                            } else {
                                                return Err(Error::new(
                                                    signature.span(),
                                                    "only a single catch-all function can be declared with the signature: fn <name>(&mut self, &dyn Event)",
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(catch_all)
}

fn get_snapshot_function(impl_: &ItemImpl) -> Result<Option<TokenStream>> {
    let mut snapshot = None;
    let mut matched = false;

    for item in &impl_.items {
        if let ImplItem::Fn(func) = item {
            let name = &func.sig.ident;

            if func.attrs.iter().any(|a| a.path().is_ident("new_snapshot")) {
                if matched {
                    return Err(Error::new(
                        func.span(),
                        "#[snapshot] can only be applied to a single function",
                    ));
                } else {
                    snapshot = Some(func);
                    matched = true;
                }
            } else if snapshot.is_none() && name == &Ident::new("new_snapshot", func.span()) {
                snapshot = Some(func);
            }
        }
    }

    if let Some(func) = snapshot {
        if func.sig.inputs.len() > 1 {
            return Err(Error::new(
                func.span(),
                "snapshot function cannot have parameters",
            ));
        }

        if let ReturnType::Type(_, ty) = &func.sig.output {
            let name = &func.sig.ident;

            if let Type::Path(ty) = ty.as_ref() {
                if let Some(segment) = ty.path.segments.last() {
                    if segment.ident == Ident::new("Option", ty.span()) {
                        if let PathArguments::AngleBracketed(generics) = &segment.arguments {
                            if let Some(GenericArgument::Type(Type::Path(arg))) =
                                generics.args.first()
                            {
                                if let Some(segment) = arg.path.segments.last() {
                                    if segment.ident == Ident::new("Box", ty.span()) {
                                        // Option<Box<dyn Snapshot>>
                                        return Ok(Some(quote! { self.#name() }));
                                    } else {
                                        // Option<T> where T: Snapshot
                                        return Ok(Some(quote! {
                                            self.#name().map(|s| Box::new(s) as Box<dyn cqrs::snapshot::Snapshot>)
                                        }));
                                    }
                                }
                            }
                        }
                    } else if segment.ident == Ident::new("Box", ty.span()) {
                        // Box<dyn Snapshot>
                        return Ok(Some(quote! { Some(self.#name()) }));
                    } else {
                        // T: Snapshot
                        return Ok(Some(
                            quote! { Some(Box::new(self.#name()) as Box<dyn cqrs::snapshot::Snapshot>) },
                        ));
                    }
                }
            }

            Err(Error::new(
                func.span(),
                "unexpected snapshot function return value",
            ))
        } else {
            Err(Error::new(
                func.span(),
                "snapshot function must return a value",
            ))
        }
    } else {
        Ok(None)
    }
}
