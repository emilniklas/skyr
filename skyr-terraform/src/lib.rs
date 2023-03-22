pub mod tfplugin6 {
    include!(concat!(env!("OUT_DIR"), "/tfplugin6.rs"));
}
pub mod tfplugin5 {
    include!(concat!(env!("OUT_DIR"), "/tfplugin5.rs"));
}

mod provider_client;
pub use provider_client::*;

mod provider_plugin;
pub use provider_plugin::*;

mod download;
pub use download::*;

#[macro_export]
macro_rules! terraform_plugin {
    ($($resource:ident ( $($identity_field:ident),* ))*) => {
        const EMBEDDED_PROVIDER_EXECUTABLE: &'static [u8] = include_bytes!(
            concat!(env!("OUT_DIR"), "/provider")
        );

        skyr::export_plugin! {
            skyr_terraform::ProviderPlugin::new(env!("PROVIDER_NAME"), EMBEDDED_PROVIDER_EXECUTABLE).unwrap()
            $(
                .with_identity_fields(stringify!($resource), [
                    $(
                        stringify!($identity_field)
                    ),*
                ])
            )*
        }
    }
}
