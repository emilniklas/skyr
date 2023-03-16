use std::collections::BTreeSet;
use std::fmt;

use crate::{Collection, DisplayAsDebug, Value};

pub enum Diff<'a> {
    Unchanged(&'a Value),
    Changed(&'a Value, &'a Value),
    Added(&'a Value),
    Removed(&'a Value),

    Record(Vec<(&'a str, Diff<'a>)>),
    List(Vec<Diff<'a>>),
    Tuple(Vec<Diff<'a>>),
}

impl<'a> Diff<'a> {
    pub fn calculate(old: &'a Value, new: &'a Value) -> Self {
        match (old, new) {
            (
                Value::Collection(Collection::Record(old)),
                Value::Collection(Collection::Record(new)),
            ) => {
                let mut diff = vec![];
                let mut seen = BTreeSet::new();

                'olds: for (old_name, old_value) in old {
                    for (new_name, new_value) in new {
                        if old_name == new_name {
                            diff.push((old_name.as_ref(), Diff::calculate(old_value, new_value)));
                            seen.insert(old_name);
                            continue 'olds;
                        }
                    }

                    diff.push((old_name.as_ref(), Diff::Removed(old_value)));
                }

                for (new_name, new_value) in new {
                    if !seen.contains(new_name) {
                        diff.push((new_name.as_ref(), Diff::Added(new_value)));
                    }
                }

                Diff::Record(diff)
            }
            (
                Value::Collection(Collection::List(old)),
                Value::Collection(Collection::List(new)),
            ) => {
                let mut diff = vec![];
                for i in 0..old.len().max(new.len()) {
                    match (old.get(i), new.get(i)) {
                        (None, None) => {}
                        (Some(old), Some(new)) => diff.push(Diff::calculate(old, new)),
                        (None, Some(new)) => diff.push(Diff::Added(new)),
                        (Some(old), None) => diff.push(Diff::Removed(old)),
                    }
                }
                Diff::List(diff)
            }
            (
                Value::Collection(Collection::Tuple(old)),
                Value::Collection(Collection::Tuple(new)),
            ) => {
                let mut diff = vec![];
                for i in 0..old.len().max(new.len()) {
                    match (old.get(i), new.get(i)) {
                        (None, None) => {}
                        (Some(old), Some(new)) => diff.push(Diff::calculate(old, new)),
                        (None, Some(new)) => diff.push(Diff::Added(new)),
                        (Some(old), None) => diff.push(Diff::Removed(old)),
                    }
                }
                Diff::Tuple(diff)
            }

            (Value::Primitive(o), Value::Primitive(n)) if o == n => Diff::Unchanged(new),

            (old, new) => Diff::Changed(old, new),
        }
    }
}

impl<'a> fmt::Debug for Diff<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Diff::Unchanged(new) => new.fmt(f),
            Diff::Changed(old, new) => {
                old.fmt(f)?;
                write!(f, " => ")?;
                new.fmt(f)
            }
            Diff::Added(new) => {
                write!(f, "+ ")?;
                new.fmt(f)
            }
            Diff::Removed(old) => {
                write!(f, "- ")?;
                old.fmt(f)
            }
            Diff::Record(r) => {
                let mut m = f.debug_map();
                for (n, d) in r {
                    m.entry(&DisplayAsDebug(n), d);
                }
                m.finish()
            }
            Diff::Tuple(l) => {
                let mut m = f.debug_tuple("");
                for d in l {
                    m.field(d);
                }
                m.finish()
            }
            Diff::List(l) => {
                let mut m = f.debug_list();
                for d in l {
                    m.entry(d);
                }
                m.finish()
            }
        }
    }
}
