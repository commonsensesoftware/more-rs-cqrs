use proc_macro2::TokenStream;
use punctuated::Punctuated;
use quote::{ToTokens, quote};
use std::{borrow::Cow, collections::HashSet};
use syn::{spanned::Spanned, *};
use token::Plus;

struct Metadata {
    index: usize,
    id: Option<Path>,
    output: Option<usize>,
    store: Option<usize>,
}

impl Metadata {
    fn new(index: usize) -> Self {
        Self {
            index,
            id: Default::default(),
            output: Default::default(),
            store: Default::default(),
        }
    }
}

struct Context<'a> {
    target: &'a ItemStruct,
    metadata: Metadata,
}

impl<'a> Context<'a> {
    fn new(target: &'a ItemStruct, metadata: Metadata) -> Self {
        Self { target, metadata }
    }

    fn target(&self) -> &ItemStruct {
        self.target
    }

    fn output(&self) -> Option<StructField<'a>> {
        self.metadata.output.map(|index| {
            StructField(
                index,
                match &self.target.fields {
                    Fields::Named(fields) => fields.named.get(index).unwrap(),
                    Fields::Unnamed(fields) => fields.unnamed.get(index).unwrap(),
                    _ => unreachable!(),
                },
            )
        })
    }

    fn store(&self) -> Option<StructField<'a>> {
        self.metadata.store.map(|index| {
            StructField(
                index,
                match &self.target.fields {
                    Fields::Named(fields) => fields.named.get(index).unwrap(),
                    Fields::Unnamed(fields) => fields.unnamed.get(index).unwrap(),
                    _ => unreachable!(),
                },
            )
        })
    }
}

struct StructField<'a>(usize, &'a Field);

impl<'a> StructField<'a> {
    fn ident(&self) -> Cow<'a, Ident> {
        if let Some(ident) = &self.1.ident {
            Cow::Borrowed(ident)
        } else {
            Cow::Owned(Ident::new(&self.0.to_string(), self.1.span()))
        }
    }

    fn ty(&self) -> &Type {
        &self.1.ty
    }
}

struct Projector<'a> {
    context: Context<'a>,
    events: Vec<&'a Type>,
}

impl<'a> Projector<'a> {
    fn ty(&self) -> &Ident {
        &self.context.target().ident
    }

    fn generics(&self) -> &Generics {
        &self.context.target().generics
    }

    fn output(&self) -> Option<StructField<'_>> {
        self.context.output()
    }

    fn store(&self) -> StructField<'_> {
        self.context.store().unwrap()
    }

    fn id(&self) -> Cow<'_, Type> {
        if let Some(path) = &self.context.metadata.id {
            Cow::Owned(Type::Path(TypePath {
                qself: None,
                path: path.clone(),
            }))
        } else {
            match_store_field(self.store().1)
                .or_else(|| match_id_type_arg(&self.context.target().generics))
                .map_or_else(
                    || {
                        Cow::Owned(Type::Path(TypePath {
                            qself: None,
                            path: parse_str::<Path>("uuid::Uuid").unwrap(),
                        }))
                    },
                    Cow::Borrowed,
                )
        }
    }
}

impl<'a> TryFrom<Context<'a>> for Projector<'a> {
    type Error = Error;

    fn try_from(context: Context<'a>) -> std::result::Result<Self, Self::Error> {
        if context.store().is_some() {
            Ok(Self {
                context,
                events: Default::default(),
            })
        } else {
            Err(Error::new(
                context.target().span(),
                "missing expected event store",
            ))
        }
    }
}

impl<'a> From<Projector<'a>> for TokenStream {
    fn from(value: Projector<'a>) -> Self {
        let struct_ = value.ty();
        let (impl_generics, ty_generics, where_) = value.generics().split_for_impl();
        let (ret, output) = if let Some(output) = &value.output() {
            let field = output.ident();
            let ty = output.ty();
            (
                quote! { std::mem::replace(&mut __self.#field, Default::default()) },
                quote! { #ty },
            )
        } else {
            (quote! { () }, quote! { () })
        };
        let store = value.store().ident();
        let id = value.id();
        let event = &value.events;

        quote! {
            impl #impl_generics cqrs::projection::Projector<#id, #output> for #struct_ #ty_generics #where_ {
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
                fn run<'life0, 'life1, 'async_trait>(
                    &'life0 mut self,
                    filter: Option<&'life1 cqrs::projection::Filter<#id>>,
                ) -> ::core::pin::Pin<
                    Box<
                        dyn ::core::future::Future<
                            Output = Result<#output, Box<dyn std::error::Error + Send>>,
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
                            Result<#output, Box<dyn std::error::Error + Send>>,
                        > {
                            #[allow(unreachable_code)] return __ret;
                        }
                        let mut __self = self;
                        let filter = filter;
                        let __ret: Result<#output, Box<dyn std::error::Error + Send>> = {
                            use cqrs::message::Encoded;
                            use futures::StreamExt;
                            let predicate = if let Some(filter) = filter {
                                let builder: cqrs::event::PredicateBuilder<_> = filter.into();
                                builder
                                    #(.add_type(#event::schema()))*
                                    .build()
                            } else {
                                cqrs::event::Predicate::default()
                            };
                            let predicate = if filter.is_some() {
                                Some(&predicate)
                            } else {
                                None
                            };
                            let mut stream = __self.#store.load(predicate).await;
                            while let Some(result) = stream.next().await {
                                let saved = result
                                    .map_err(|e| {
                                        Box::new(e) as Box<dyn std::error::Error + Send>
                                    })?;
                                let any = saved.message().as_any();
                                let id = any.type_id();
                                #(if id == std::any::TypeId::of::<#event>() {
                                    <Self as cqrs::event::Receiver<#event>>::receive(
                                        __self,
                                        any.downcast_ref::<#event>().unwrap()).await?;
                                    continue;
                                })*
                            }
                            Ok(#ret)
                        };
                        #[allow(unreachable_code)] __ret
                    })
                }
            }
        }
    }
}

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let span = input.span();

    if let Ok(mut module) = parse2::<ItemMod>(input) {
        if let Some((_, items)) = &mut module.content {
            let metadata = match get_metadata(items) {
                Ok(metadata) => metadata,
                Err(error) => return error.to_compile_error(),
            };
            let mut new_items = Vec::new();

            match get_projectors(items, metadata) {
                Ok(projectors) => {
                    for projector in projectors {
                        let stream = TokenStream::from(projector);
                        let item = Item::Impl(parse2::<ItemImpl>(stream).unwrap());
                        new_items.push(item);
                    }
                }
                Err(error) => return error.to_compile_error(),
            }

            let mut output = TokenStream::new();

            output.extend(items.iter_mut().map(|item| item.to_token_stream()));
            output.extend(new_items.into_iter().map(|item| item.to_token_stream()));
            output
        } else {
            module.to_token_stream()
        }
    } else {
        Error::new(span, "#[projectors] can only be applied to a module.").to_compile_error()
    }
}

fn remove_attribute(attrs: &mut Vec<Attribute>, name: &str) -> Option<Attribute> {
    for i in 0..attrs.len() {
        let path = attrs[i].path();

        if path.is_ident(name) {
            return Some(attrs.remove(i));
        }
    }

    None
}

fn get_metadata(items: &mut [Item]) -> Result<Vec<Metadata>> {
    let mut metadata = Vec::new();

    for (i, item) in items.iter_mut().enumerate() {
        if let Item::Struct(struct_) = item
            && let Some(attr) = remove_attribute(&mut struct_.attrs, "projector")
        {
            let mut meta = Metadata::new(i);

            if let Ok(path) = attr.parse_args::<Path>() {
                meta.id = Some(path);
            }

            for (j, field) in struct_.fields.iter_mut().enumerate() {
                if remove_attribute(&mut field.attrs, "output").is_some() {
                    if meta.output.is_some() {
                        return Err(Error::new(field.span(), "encountered multiple #[output]"));
                    }

                    meta.output = Some(j);
                } else if remove_attribute(&mut field.attrs, "store").is_some() {
                    if meta.store.is_some() {
                        return Err(Error::new(field.span(), "encountered multiple #[store]"));
                    }

                    meta.store = Some(j);
                }
            }

            if meta.output.is_none() || meta.store.is_none() {
                for (i, field) in struct_.fields.iter().enumerate() {
                    if let Some(name) = &field.ident {
                        if meta.output.is_none() && name == &Ident::new("output", name.span()) {
                            meta.output = Some(i);
                        } else if meta.store.is_none() && name == &Ident::new("store", name.span())
                        {
                            meta.store = Some(i);
                        }
                    }
                }
            }

            metadata.push(meta);
        }
    }

    Ok(metadata)
}

fn get_projectors(items: &[Item], metadata: Vec<Metadata>) -> Result<Vec<Projector<'_>>> {
    let indexes: HashSet<_> = metadata.iter().map(|m| m.index).collect();
    let mut projectors = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            if indexes.contains(&index)
                && let Item::Struct(struct_) = item
            {
                return Some(struct_);
            }

            None
        })
        .zip(metadata.into_iter())
        .map(|(item, meta)| Context::new(item, meta))
        .map(Projector::try_from)
        .collect::<Result<Vec<_>>>()?;

    let receivers = get_receivers(items);

    for (ty, event) in receivers {
        if let Some(projector) = projectors.iter_mut().find(|p| p.ty() == ty) {
            projector.events.push(event);
        }
    }

    Ok(projectors)
}

#[inline]
fn get_receivers(items: &[Item]) -> impl Iterator<Item = (&Ident, &Type)> {
    items.iter().filter_map(|item| {
        if let Item::Impl(block) = item
            && let Some((_, path, _)) = &block.trait_
            && let Some(segment) = path.segments.last()
            && segment.ident == Ident::new("Receiver", segment.span())
            && let PathArguments::AngleBracketed(generic) = &segment.arguments
            && let Some(GenericArgument::Type(event)) = generic.args.first()
            && let Type::Path(ty) = &*block.self_ty
            && let Some(struct_) = ty.path.get_ident()
        {
            return Some((struct_, event));
        }
        None
    })
}

#[inline]
fn match_id_type_arg(generics: &Generics) -> Option<&Type> {
    for param in &generics.params {
        if let GenericParam::Type(ty) = param {
            let result = match_id_bound(&ty.bounds);

            if result.is_some() {
                return result;
            }
        }
    }

    if let Some(clause) = &generics.where_clause {
        for predicate in &clause.predicates {
            if let WherePredicate::Type(ty) = predicate {
                let result = match_id_bound(&ty.bounds);

                if result.is_some() {
                    return result;
                }
            }
        }
    }

    None
}

fn match_id_bound(bounds: &Punctuated<TypeParamBound, Plus>) -> Option<&Type> {
    for bound in bounds {
        if let TypeParamBound::Trait(trait_) = bound
            && let Some(segment) = trait_.path.segments.last()
            && segment.ident == Ident::new("Store", bound.span())
            && let PathArguments::AngleBracketed(generic) = &segment.arguments
            && let Some(GenericArgument::Type(id)) = generic.args.first()
        {
            return Some(id);
        }
    }

    None
}

#[inline]
fn match_store_field(field: &Field) -> Option<&Type> {
    resolve_store_id_type(&field.ty, &Ident::new("Store", field.span()))
}

fn resolve_store_id_type<'a>(ty: &'a Type, name: &Ident) -> Option<&'a Type> {
    match ty {
        Type::Path(ty) => {
            if let Some(ty) = ty.path.segments.last() {
                if ty.ident == *name {
                    if let PathArguments::AngleBracketed(generic) = &ty.arguments
                        && let Some(GenericArgument::Type(id)) = generic.args.first()
                    {
                        return Some(id);
                    }
                } else if let PathArguments::AngleBracketed(generic) = &ty.arguments {
                    for arg in &generic.args {
                        if let GenericArgument::Type(ty) = arg
                            && let Some(id) = resolve_store_id_type(ty, name)
                        {
                            return Some(id);
                        }
                    }
                }
            }
        }
        Type::TraitObject(trait_) => {
            for bound in &trait_.bounds {
                if let TypeParamBound::Trait(bound) = bound
                    && let Some(ty) = bound.path.segments.last()
                {
                    if ty.ident == *name {
                        if let PathArguments::AngleBracketed(generic) = &ty.arguments
                            && let Some(GenericArgument::Type(id)) = generic.args.first()
                        {
                            return Some(id);
                        }
                    } else if let PathArguments::AngleBracketed(generic) = &ty.arguments {
                        for arg in &generic.args {
                            if let GenericArgument::Type(ty) = arg
                                && let Some(id) = resolve_store_id_type(ty, name)
                            {
                                return Some(id);
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }

    None
}
