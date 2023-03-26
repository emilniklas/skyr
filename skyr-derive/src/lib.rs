use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Result;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{
    parse_macro_input, Data, DataEnum, DataUnion, DeriveInput, Expr, Field, Fields, FieldsNamed,
    FieldsUnnamed,
};

#[proc_macro_attribute]
pub fn skyr(_attribute: TokenStream, input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);

    TokenStream::from(quote! {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        #input
    })
}

#[proc_macro_derive(TypeOf)]
pub fn type_of_derive(input: TokenStream) -> TokenStream {
    let DeriveInput {
        ident,
        generics,
        data,
        attrs,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let typedef = type_of_data(data).unwrap();

    let type_name = attrs
        .iter()
        .find(|a| a.path.is_ident("skyr"))
        .map(|a| {
            let assignment: syn::ExprAssign = a.parse_args().unwrap();

            match *assignment.left {
                syn::Expr::Path(syn::ExprPath { path: p, .. }) if p.is_ident("module") => {
                    match *assignment.right {
                        syn::Expr::Lit(syn::ExprLit { lit, .. }) => lit,
                        v => panic!("unsupported skyr `module` attribute value {:?}", v),
                    }
                }
                p => panic!("unsupported skyr attribute {:?}", p),
            }
        })
        .map(|prefix| {
            quote! {
                concat!(stringify!(#ident), ".", #prefix)
            }
        })
        .unwrap_or(quote! {
            stringify!(#ident)
        });

    TokenStream::from(quote! {
        impl skyr::TypeOf for #ident #generics {
            fn type_of() -> skyr::analyze::Type {
                skyr::analyze::Type::named(
                    #type_name,
                    #typedef
                )
            }
        }
    })
}

fn type_of_data(data: Data) -> Result<Expr> {
    match data {
        Data::Struct(s) => type_of_fields(s.fields),
        Data::Enum(e) => type_of_enum(e),
        Data::Union(u) => type_of_union(u),
    }
}

fn type_of_fields(fields: Fields) -> Result<Expr> {
    match fields {
        Fields::Unit => syn::parse2(quote!(skyr::analyze::Type::Primitive(
            skyr::analyze::PrimitiveType::Void
        ))),
        Fields::Unnamed(f) => type_of_fields_unnamed(f),
        Fields::Named(f) => type_of_fields_named(f),
    }
}

fn type_of_fields_unnamed(fields: FieldsUnnamed) -> Result<Expr> {
    let fields = fields
        .unnamed
        .into_iter()
        .map(type_of_field)
        .collect::<Result<Punctuated<Expr, Comma>>>()?;

    syn::parse2(quote! (
        skyr::analyze::Type::Composite(skyr::analyze::CompositeType::list([
            #fields
        ]))
    ))
}

fn type_of_fields_named(fields: FieldsNamed) -> Result<Expr> {
    let fields = fields
        .named
        .into_iter()
        .map(type_of_field)
        .collect::<Result<Punctuated<Expr, Comma>>>()?;

    syn::parse2(quote! (
        skyr::analyze::Type::Composite(skyr::analyze::CompositeType::record([
            #fields
        ]))
    ))
}

fn type_of_field(Field { ident, ty, .. }: Field) -> Result<Expr> {
    if let Some(ident) = ident {
        let ident = ident.to_string().to_case(Case::Camel);
        syn::parse2(quote!(
            (#ident, <#ty as skyr::TypeOf>::type_of())
        ))
    } else {
        syn::parse2(quote!(
            <#ty as skyr::TypeOf>::type_of()
        ))
    }
}

fn type_of_enum(_data: DataEnum) -> Result<Expr> {
    syn::parse2(quote!(skyr::analyze::Type::Primitive(
        skyr::analyze::PrimitiveType::Void
    )))
}

fn type_of_union(_data: DataUnion) -> Result<Expr> {
    syn::parse2(quote!(skyr::analyze::Type::Primitive(
        skyr::analyze::PrimitiveType::Void
    )))
}
