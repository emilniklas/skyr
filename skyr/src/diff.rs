use std::collections::BTreeSet;
use std::fmt;

use crate::execute::Value;
use crate::{ResourceValue, DisplayAsDebug};

pub enum Diff<'v, 'a> {
    Unchanged(&'v Value<'a>),
    Changed(&'v ResourceValue, &'v Value<'a>),
    Added(&'v Value<'a>),
    Removed(&'v ResourceValue),

    Record(Vec<(&'v str, Diff<'v, 'a>)>),
    List(Vec<Diff<'v, 'a>>),
}

impl<'v, 'a> Diff<'v, 'a> {
    pub fn calculate(old: &'v ResourceValue, new: &'v Value<'a>) -> Self {
        match (old, new) {
            (ResourceValue::Record(old), Value::Record(new)) => {
                let mut diff = vec![];
                let mut seen = BTreeSet::new();

                'olds: for (old_name, old_value) in old {
                    for (new_name, new_value) in new {
                        if old_name == new_name {
                            diff.push((old_name.as_str(), Diff::calculate(old_value, new_value)));
                            seen.insert(old_name);
                            continue 'olds;
                        }
                    }

                    diff.push((old_name.as_str(), Diff::Removed(old_value)));
                }

                for (new_name, new_value) in new {
                    if !seen.contains(new_name) {
                        diff.push((new_name.as_str(), Diff::Added(new_value)));
                    }
                }

                Diff::Record(diff)
            }
            (ResourceValue::List(old), Value::List(new)) => {
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

            (ResourceValue::String(o), Value::String(n)) if o == n => Diff::Unchanged(new),
            (ResourceValue::Integer(o), Value::Integer(n)) if o == n => Diff::Unchanged(new),
            (ResourceValue::Boolean(o), Value::Boolean(n)) if o == n => Diff::Unchanged(new),
            (ResourceValue::Nil, Value::Nil) => Diff::Unchanged(new),

            (old, new) => Diff::Changed(old, new),
        }
    }
}

impl<'v, 'a> fmt::Debug for Diff<'v, 'a> {
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
