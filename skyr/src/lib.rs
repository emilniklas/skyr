mod source;
pub use source::*;

mod state;
pub use state::*;

mod plan;
pub use plan::*;

mod program;
pub use program::*;

mod plugin;
pub use plugin::*;

mod diff;
pub use diff::*;

mod value;
pub use value::*;

mod serde;
pub use crate::serde::*;

mod flyweight;
pub use flyweight::*;

pub mod analyze;
pub mod compile;
pub mod execute;

pub(crate) struct DisplayAsDebug<T>(T);

use std::fmt;
impl<T: fmt::Display> fmt::Debug for DisplayAsDebug<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub use skyr_derive::*;
