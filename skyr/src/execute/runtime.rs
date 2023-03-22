use std::fmt;

use futures::Future;

use crate::{Collection, ResourceId, Value};

use super::{Executor, RuntimeValue};

#[macro_export]
macro_rules! known {
    ($value:expr) => {{
        match $value.into_result() {
            Err(error) => return skyr::execute::DependentValue::pending(error.dependencies),
            Ok(v) => v,
        }
    }};
}

#[derive(Debug)]
pub struct PendingError {
    pub dependencies: Vec<ResourceId>,
}

#[derive(Clone)]
pub struct DependentValue<T> {
    dependencies: Vec<ResourceId>,
    inner: InnerDependentValue<T>,
}

impl<T: fmt::Debug> fmt::Debug for DependentValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            InnerDependentValue::Pending => write!(f, "<pending>"),
            InnerDependentValue::Resolved(t) => t.fmt(f),
        }
    }
}

impl<'a> DependentValue<RuntimeValue<'a>> {
    pub fn into_value(self) -> DependentValue<Value> {
        self.flat_map(RuntimeValue::into_value)
    }
}

impl<T> DependentValue<T> {
    pub fn new(value: T) -> Self {
        Self {
            dependencies: vec![],
            inner: InnerDependentValue::Resolved(value),
        }
    }

    pub fn with_dependency(mut self, dependency: ResourceId) -> Self {
        self.dependencies.push(dependency);
        self
    }

    pub fn pending(dependencies: Vec<ResourceId>) -> Self {
        Self {
            dependencies,
            inner: InnerDependentValue::Pending,
        }
    }

    pub fn as_ref(&self) -> DependentValue<&T> {
        DependentValue {
            dependencies: self.dependencies.clone(),
            inner: match &self.inner {
                InnerDependentValue::Pending => InnerDependentValue::Pending,
                InnerDependentValue::Resolved(t) => InnerDependentValue::Resolved(t),
            },
        }
    }

    pub fn as_resolved(&self) -> Option<&T> {
        match &self.inner {
            InnerDependentValue::Pending => None,
            InnerDependentValue::Resolved(t) => Some(t),
        }
    }

    pub fn into_result(self) -> Result<T, PendingError> {
        match self.inner {
            InnerDependentValue::Pending => Err(PendingError {
                dependencies: self.dependencies,
            }),
            InnerDependentValue::Resolved(t) => Ok(t),
        }
    }

    pub fn when_resolved<F, U>(&self, f: F) -> Option<U>
    where
        F: FnOnce(&T) -> U,
    {
        match &self.inner {
            InnerDependentValue::Pending => None,
            InnerDependentValue::Resolved(t) => Some(f(t)),
        }
    }

    pub async fn when_resolved_async<F, R, U>(&self, f: F) -> Option<U>
    where
        F: FnOnce(&T) -> R,
        R: Future<Output = U>,
    {
        match &self.inner {
            InnerDependentValue::Pending => None,
            InnerDependentValue::Resolved(t) => Some(f(t).await),
        }
    }

    pub fn map<F, U>(self, f: F) -> DependentValue<U>
    where
        F: FnOnce(T) -> U,
    {
        match self.inner {
            InnerDependentValue::Pending => DependentValue::pending(self.dependencies),
            InnerDependentValue::Resolved(t) => DependentValue {
                dependencies: self.dependencies,
                inner: InnerDependentValue::Resolved(f(t)),
            },
        }
    }

    pub async fn map_async<F, R, U>(self, f: F) -> DependentValue<U>
    where
        F: FnOnce(T) -> R,
        R: Future<Output = U>,
    {
        match self.inner {
            InnerDependentValue::Pending => DependentValue::pending(self.dependencies),
            InnerDependentValue::Resolved(t) => DependentValue {
                dependencies: self.dependencies,
                inner: InnerDependentValue::Resolved(f(t).await),
            },
        }
    }

    pub fn flat_map<F, U>(self, f: F) -> DependentValue<U>
    where
        F: FnOnce(T) -> DependentValue<U>,
    {
        match self.inner {
            InnerDependentValue::Pending => DependentValue::pending(self.dependencies),
            InnerDependentValue::Resolved(t) => {
                let DependentValue {
                    mut dependencies,
                    inner,
                } = f(t);
                dependencies.extend(self.dependencies);
                match inner {
                    InnerDependentValue::Pending => DependentValue::pending(dependencies),
                    InnerDependentValue::Resolved(u) => DependentValue {
                        dependencies,
                        inner: InnerDependentValue::Resolved(u),
                    },
                }
            }
        }
    }

    pub async fn flat_map_async<'e, F, R, U>(self, f: F) -> DependentValue<U>
    where
        F: FnOnce(T, Vec<ResourceId>) -> R,
        R: Future<Output = DependentValue<U>>,
    {
        match self.inner {
            InnerDependentValue::Pending => DependentValue::pending(self.dependencies),
            InnerDependentValue::Resolved(t) => {
                let DependentValue {
                    mut dependencies,
                    inner,
                } = f(t, self.dependencies.clone()).await;
                dependencies.extend(self.dependencies);
                match inner {
                    InnerDependentValue::Pending => DependentValue::pending(dependencies),
                    InnerDependentValue::Resolved(u) => DependentValue {
                        dependencies,
                        inner: InnerDependentValue::Resolved(u),
                    },
                }
            }
        }
    }
}

impl<'a> DependentValue<RuntimeValue<'a>> {
    pub async fn resolve<'e>(self, executor: &'e Executor<'a>) -> DependentValue<RuntimeValue<'a>> {
        self.flat_map_async(|rtv, _| async move { rtv.resolve(executor).await })
            .await
    }
}

#[derive(Clone)]
enum InnerDependentValue<T> {
    Resolved(T),
    Pending,
}

impl<T, U> From<(DependentValue<T>, DependentValue<U>)> for DependentValue<(T, U)> {
    fn from((a, b): (DependentValue<T>, DependentValue<U>)) -> Self {
        let mut dependencies = a.dependencies;
        dependencies.extend(b.dependencies);

        match (a.inner, b.inner) {
            (InnerDependentValue::Pending, _) | (_, InnerDependentValue::Pending) => Self {
                dependencies,
                inner: InnerDependentValue::Pending,
            },
            (InnerDependentValue::Resolved(a), InnerDependentValue::Resolved(b)) => Self {
                dependencies,
                inner: InnerDependentValue::Resolved((a, b)),
            },
        }
    }
}

impl<T> From<Vec<DependentValue<T>>> for DependentValue<Vec<T>> {
    fn from(value: Vec<DependentValue<T>>) -> Self {
        let mut x = DependentValue::new(vec![]);
        for dv in value {
            x = x.flat_map(|mut list| {
                dv.map(|t| {
                    list.push(t);
                    list
                })
            })
        }
        x
    }
}

impl<T> From<Collection<DependentValue<T>>> for DependentValue<Collection<T>> {
    fn from(value: Collection<DependentValue<T>>) -> Self {
        match value {
            Collection::List(l) => Into::<DependentValue<Vec<T>>>::into(l).map(Collection::List),
            Collection::Tuple(t) => Into::<DependentValue<Vec<T>>>::into(t).map(Collection::Tuple),
            Collection::Record(r) => {
                let fields: DependentValue<Vec<(_, _)>> = r
                    .into_iter()
                    .map(|(n, dv)| dv.map(|v| (n, v)))
                    .collect::<Vec<_>>()
                    .into();

                fields.map(Collection::Record)
            }
            Collection::Dict(d) => {
                let fields: DependentValue<Vec<(_, _)>> = d
                    .into_iter()
                    .map(|(dk, dv)| dk.flat_map(|k| dv.map(|v| (k, v))))
                    .collect::<Vec<_>>()
                    .into();

                fields.map(Collection::Dict)
            }
        }
    }
}

impl<T, E> Into<Result<DependentValue<T>, E>> for DependentValue<Result<T, E>> {
    fn into(self) -> Result<DependentValue<T>, E> {
        match self.inner {
            InnerDependentValue::Pending => Ok(DependentValue::pending(self.dependencies)),
            InnerDependentValue::Resolved(result) => Ok(DependentValue::new(result?)),
        }
    }
}

impl<T> From<T> for DependentValue<T> {
    fn from(value: T) -> Self {
        DependentValue::new(value)
    }
}
