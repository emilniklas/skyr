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
