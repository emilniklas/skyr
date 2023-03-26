use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;

use futures::Future;
use serde::{Deserialize, Serialize};

use crate::compile::Span;
use crate::DisplayAsDebug;

use super::TypeId;

#[derive(Debug, PartialEq)]
pub enum TypeError {
    Mismatch {
        span: Span,
        expected: Type,
        assigned: Type,
    },
}

#[derive(Default, Debug)]
pub struct TypeCheckResult<T> {
    value: T,
    errors: Vec<TypeError>,
}

impl TypeCheckResult<()> {
    pub fn new() -> Self {
        Self::success(())
    }
}

impl IntoIterator for TypeCheckResult<()> {
    type Item = TypeError;
    type IntoIter = std::vec::IntoIter<TypeError>;

    fn into_iter(self) -> Self::IntoIter {
        self.errors.into_iter()
    }
}

impl<T> TypeCheckResult<T> {
    pub fn success(value: T) -> Self {
        Self {
            value,
            errors: Default::default(),
        }
    }

    pub fn error(value: T, error: TypeError) -> Self {
        Self::errors(value, [error])
    }

    pub fn errors(value: T, errors: impl IntoIterator<Item = TypeError>) -> Self {
        Self {
            value,
            errors: errors.into_iter().collect(),
        }
    }

    pub fn into_inner(self, errors: &mut Vec<TypeError>) -> T {
        errors.extend(self.errors);
        self.value
    }

    pub fn map<F, U>(self, f: F) -> TypeCheckResult<U>
    where
        F: FnOnce(T) -> U,
    {
        TypeCheckResult {
            value: f(self.value),
            errors: self.errors,
        }
    }

    pub async fn map_async<F, R>(self, f: F) -> TypeCheckResult<R::Output>
    where
        F: FnOnce(T) -> R,
        R: Future,
    {
        TypeCheckResult {
            value: f(self.value).await,
            errors: self.errors,
        }
    }

    pub fn flat_map<F, U>(self, f: F) -> TypeCheckResult<U>
    where
        F: FnOnce(T) -> TypeCheckResult<U>,
    {
        let mut r = f(self.value);
        r.errors.extend(self.errors);
        r
    }

    pub async fn flat_map_async<F, R, U>(self, f: F) -> TypeCheckResult<U>
    where
        F: FnOnce(T) -> R,
        R: Future<Output = TypeCheckResult<U>>,
    {
        let mut r = f(self.value).await;
        r.errors.extend(self.errors);
        r
    }
}

impl<T> CompositeType<TypeCheckResult<T>> {
    pub fn flip(self) -> TypeCheckResult<CompositeType<T>> {
        let mut errors = vec![];
        let c = self.map(|res| res.into_inner(&mut errors));
        TypeCheckResult::errors(c, errors)
    }
}

#[derive(PartialEq, Clone)]
pub enum SpanType {
    Primitive(Span, PrimitiveType),
    Composite(Span, CompositeType<SpanType>),
    Referenced(Span, Type, Option<Cow<'static, str>>),
    Open(Span, TypeId),
}

impl SpanType {
    pub fn span(&self) -> &Span {
        match self {
            SpanType::Primitive(s, _) => s,
            SpanType::Composite(s, _) => s,
            SpanType::Referenced(s, _, _) => s,
            SpanType::Open(s, _) => s,
        }
    }

    pub fn return_type(self) -> SpanType {
        if let Self::Composite(_, CompositeType::Function(_, r)) = self {
            *r
        } else if let Self::Referenced(span, t, name) = self {
            Self::Referenced(span, t.return_type(), name)
        } else {
            SpanType::Open(self.span().clone(), Default::default())
        }
    }

    pub fn when_not_named(self) -> Self {
        if let Self::Referenced(s, t, _) = self {
            Self::Referenced(s, t.when_not_named(), None)
        } else {
            self
        }
    }
}

#[derive(Clone, Copy)]
enum UnificationDirection {
    Leftward,
    Equal,
    Rightward,
}

impl UnificationDirection {
    pub fn contravariant(self) -> Self {
        match self {
            Self::Leftward => Self::Rightward,
            Self::Equal => Self::Equal,
            Self::Rightward => Self::Leftward,
        }
    }
}

#[derive(Default, Clone)]
pub struct TypeEnvironment<'a> {
    parent: Option<&'a TypeEnvironment<'a>>,
    bindings: BTreeMap<TypeId, Type>,
}

impl<'a> TypeEnvironment<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn inner(&self) -> TypeEnvironment {
        TypeEnvironment {
            parent: Some(self),
            ..Default::default()
        }
    }

    fn get_binding(&self, id: TypeId) -> Option<&Type> {
        self.bindings
            .get(&id)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get_binding(id)))
    }

    fn unify_composite<A, B, C, F>(
        &mut self,
        lhs: CompositeType<A>,
        rhs: CompositeType<B>,
        direction: UnificationDirection,
        mut f: F,
    ) -> Option<CompositeType<C>>
    where
        F: FnMut(&mut Self, A, B, UnificationDirection) -> C,
    {
        Some(match (lhs, rhs) {
            (CompositeType::Tuple(mut lhs), CompositeType::Tuple(mut rhs)) => {
                let len = lhs.len().max(rhs.len());

                lhs.reverse();
                rhs.reverse();

                let mut unified = vec![];

                for _ in 0..len {
                    let lhs = lhs.pop();
                    let rhs = rhs.pop();

                    match (lhs, rhs) {
                        (None, None) => {}
                        (Some(_lhs), None) => todo!("too few elements in tuple"),
                        (None, Some(_rhs)) => todo!("too many elements in tuple"),
                        (Some(lhs), Some(rhs)) => {
                            let t = f(self, lhs, rhs, direction);
                            unified.push(t);
                        }
                    }
                }

                CompositeType::Tuple(unified)
            }

            (CompositeType::List(lhs), CompositeType::List(rhs)) => {
                CompositeType::List(Box::new(f(self, *lhs, *rhs, direction)))
            }

            (CompositeType::Optional(lhs), CompositeType::Optional(rhs)) => {
                CompositeType::Optional(Box::new(f(self, *lhs, *rhs, direction)))
            }

            (CompositeType::Dict(lhs_k, lhs_v), CompositeType::Dict(rhs_k, rhs_v)) => {
                let k = f(self, *lhs_k, *rhs_k, direction);
                let v = f(self, *lhs_v, *rhs_v, direction);
                CompositeType::Dict(Box::new(k), Box::new(v))
            }

            (CompositeType::Record(lhs), CompositeType::Record(rhs)) => {
                let mut fields = vec![];
                let mut rhs: BTreeMap<_, B> = rhs.into_iter().collect();

                'fields: for (n, lhs_v) in lhs {
                    if let Some(rhs_v) = rhs.remove(n.as_ref()) {
                        fields.push((n, f(self, lhs_v, rhs_v, direction)));
                        continue 'fields;
                    }

                    todo!("missing field");
                }

                if rhs.len() > 0 {
                    todo!("extraneous fields");
                }

                CompositeType::Record(fields)
            }

            (
                CompositeType::Function(mut lhs_a, lhs_r),
                CompositeType::Function(mut rhs_a, rhs_r),
            ) => {
                let arity = lhs_a.len().max(rhs_a.len());

                lhs_a.reverse();
                rhs_a.reverse();

                let mut parameters = vec![];

                for _ in 0..arity {
                    let lhs = lhs_a.pop();
                    let rhs = rhs_a.pop();

                    match (lhs, rhs) {
                        (None, None) => {}
                        (Some(_lhs), None) => todo!("too few arguments"),
                        (None, Some(_rhs)) => todo!("too many arguments"),
                        (Some(lhs), Some(rhs)) => {
                            parameters.push(f(self, lhs, rhs, direction.contravariant()));
                        }
                    }
                }

                CompositeType::Function(parameters, Box::new(f(self, *lhs_r, *rhs_r, direction)))
            }

            _ => return None,
        })
    }

    fn match_type(
        &mut self,
        lhs: Type,
        rhs: Type,
        direction: UnificationDirection,
    ) -> (bool, Type) {
        match (self.resolve(lhs), self.resolve(rhs), direction) {
            (Type::Open(id), t, _) | (t, Type::Open(id), _) => {
                self.bindings.insert(id, t.clone());
                (true, t)
            }

            (
                Type::Composite(CompositeType::Optional(lhs)),
                Type::Composite(CompositeType::Optional(rhs)),
                _,
            ) => {
                let (m, t) = self.match_type(*lhs, *rhs, direction);
                (m, Type::Composite(CompositeType::optional(t)))
            }
            (
                Type::Composite(CompositeType::Optional(lhs)),
                rhs,
                UnificationDirection::Leftward,
            ) => {
                let (m, t) = self.match_type(*lhs, rhs, direction);
                (m, Type::Composite(CompositeType::optional(t)))
            }
            (
                lhs,
                Type::Composite(CompositeType::Optional(rhs)),
                UnificationDirection::Rightward,
            ) => {
                let (m, t) = self.match_type(lhs, *rhs, direction);
                (m, t)
            }

            (Type::Primitive(lhs), Type::Primitive(rhs), _) => (lhs == rhs, Type::Primitive(lhs)),

            (Type::Composite(lhs), Type::Composite(rhs), _) => {
                let mut matches = true;
                let unified = self
                    .unify_composite(lhs.clone(), rhs, direction, |env, lhs, rhs, direction| {
                        let (m, t) = env.match_type(lhs, rhs, direction);
                        matches = matches && m;
                        t
                    })
                    .unwrap_or_else(|| {
                        matches = false;
                        lhs
                    });
                (matches, Type::Composite(unified))
            }

            (lhs, _, _) => (false, lhs),
        }
    }

    pub fn unify(&mut self, lhs: Type, rhs: SpanType) -> TypeCheckResult<SpanType> {
        self.unify_with_direction(lhs, rhs, UnificationDirection::Leftward)
    }

    fn unify_with_direction(
        &mut self,
        lhs: Type,
        rhs: SpanType,
        direction: UnificationDirection,
    ) -> TypeCheckResult<SpanType> {
        if let Type::Named(_n, t) = lhs {
            return self.unify_with_direction(*t, rhs, direction);
        }

        if let SpanType::Referenced(span, rhs, name) = rhs {
            let (matches, t) = self.match_type(lhs.clone(), rhs.clone(), direction);

            if matches {
                return TypeCheckResult::success(SpanType::Referenced(span, t, name));
            } else {
                let st = SpanType::Referenced(span.clone(), t.clone(), name.clone());
                return TypeCheckResult::error(
                    st.clone(),
                    TypeError::Mismatch {
                        span,
                        expected: lhs,
                        assigned: rhs.maybe_named(name),
                    },
                );
            }
        }

        match (self.resolve(lhs), rhs, direction) {
            (Type::Open(id), t, _) => {
                self.bindings.insert(id, t.clone().into());
                TypeCheckResult::success(t)
            }
            (t, SpanType::Open(span, id), _) => {
                self.bindings.insert(id, t.clone());
                TypeCheckResult::success(SpanType::Referenced(span, t, None))
            }

            (Type::Primitive(lhs), SpanType::Primitive(span, rhs), _) if lhs == rhs => {
                TypeCheckResult::success(SpanType::Primitive(span, lhs))
            }

            (Type::Primitive(lhs), SpanType::Primitive(span, rhs), _) => TypeCheckResult::error(
                SpanType::Primitive(span.clone(), lhs.clone()),
                TypeError::Mismatch {
                    expected: Type::Primitive(lhs),
                    assigned: Type::Primitive(rhs),
                    span,
                },
            ),

            (
                Type::Composite(CompositeType::Optional(lhs)),
                SpanType::Composite(span, CompositeType::Optional(rhs)),
                _,
            ) => self
                .unify_with_direction(*lhs, *rhs, UnificationDirection::Leftward)
                .map(CompositeType::optional)
                .map(|c| SpanType::Composite(span, c)),
            (
                Type::Composite(CompositeType::Optional(lhs)),
                rhs,
                UnificationDirection::Leftward,
            ) => {
                let span = rhs.span().clone();
                self.unify_with_direction(*lhs, rhs, UnificationDirection::Leftward)
                    .map(CompositeType::optional)
                    .map(|c| SpanType::Composite(span, c))
            }
            (
                lhs,
                SpanType::Composite(_, CompositeType::Optional(rhs)),
                UnificationDirection::Rightward,
            ) => self.unify_with_direction(lhs, *rhs, UnificationDirection::Rightward),

            (Type::Composite(lhs), SpanType::Composite(span, rhs), _) => self
                .unify_composite(lhs.clone(), rhs.clone(), direction, TypeEnvironment::unify_with_direction)
                .map(|u| u.flip().map(|c| SpanType::Composite(span.clone(), c)))
                .unwrap_or_else(|| {
                    TypeCheckResult::error(
                        SpanType::Referenced(
                            span.clone(),
                            Type::Composite(lhs.clone()),
                            None,
                        ),
                        TypeError::Mismatch {
                            expected: Type::Composite(lhs),
                            span: span.clone(),
                            assigned: Type::Composite(rhs.map(Into::into)),
                        },
                    )
                }),

            (lhs, rhs, _) => TypeCheckResult::error(
                SpanType::Referenced(rhs.span().clone(), lhs.clone(), None),
                TypeError::Mismatch {
                    expected: lhs,
                    span: rhs.span().clone(),
                    assigned: rhs.into(),
                },
            ),
        }
    }

    pub fn resolve(&self, type_: Type) -> Type {
        match type_ {
            Type::Open(id) => match self.get_binding(id) {
                None => Type::Open(id),
                Some(t) => t.clone(),
            },
            Type::Composite(c) => Type::Composite(c.map(|t| self.resolve(t))),
            t @ Type::Primitive(_) => t,
            Type::Named(n, t) => Type::Named(n, Box::new(self.resolve(*t))),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Type {
    Primitive(PrimitiveType),
    Composite(CompositeType),
    Open(TypeId),
    Named(Cow<'static, str>, Box<Type>),
}

impl Type {
    pub const VOID: Type = Type::Primitive(PrimitiveType::Void);
    pub const STRING: Type = Type::Primitive(PrimitiveType::String);
    pub const INTEGER: Type = Type::Primitive(PrimitiveType::Integer);

    pub fn named(n: impl Into<Cow<'static, str>>, t: impl Into<Type>) -> Self {
        Self::Named(n.into(), Box::new(t.into()))
    }

    pub fn maybe_named(self, n: Option<impl Into<Cow<'static, str>>>) -> Self {
        match n {
            None => self,
            Some(n) => Self::named(n, self),
        }
    }

    pub fn into_named(self, n: impl Into<Cow<'static, str>>) -> Self {
        Self::named(n, self.when_not_named())
    }

    pub fn record(
        i: impl IntoIterator<Item = (impl Into<Cow<'static, str>>, impl Into<Type>)>,
    ) -> Self {
        Self::Composite(CompositeType::record(i))
    }

    pub fn list(t: impl Into<Type>) -> Self {
        Self::Composite(CompositeType::list(t))
    }

    pub fn optional(t: impl Into<Type>) -> Self {
        Self::Composite(CompositeType::optional(t))
    }

    pub fn open() -> Self {
        Default::default()
    }

    pub fn function(a: impl IntoIterator<Item = impl Into<Type>>, r: impl Into<Type>) -> Self {
        Self::Composite(CompositeType::function(a, r))
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Type::Composite(CompositeType::Optional(_)))
    }

    pub fn when_not_named(self) -> Self {
        if let Self::Named(_, t) = self {
            t.when_not_named()
        } else {
            self
        }
    }

    pub fn return_type(self) -> Self {
        match self.when_not_named() {
            Self::Composite(CompositeType::Function(_, r)) => *r,
            _ => Default::default(),
        }
    }
}

impl From<SpanType> for Type {
    fn from(value: SpanType) -> Self {
        match value {
            SpanType::Primitive(_, p) => Type::Primitive(p),
            SpanType::Composite(_, c) => Type::Composite(c.map(Into::into)),
            SpanType::Open(_, id) => Type::Open(id),
            SpanType::Referenced(_, t, n) => t.maybe_named(n),
        }
    }
}

impl Default for Type {
    fn default() -> Self {
        Self::Open(Default::default())
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PrimitiveType {
    Void,
    String,
    Integer,
    Float,
    Boolean,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum CompositeType<T = Type> {
    List(Box<T>),
    Optional(Box<T>),
    Function(Vec<T>, Box<T>),
    Record(Vec<(Cow<'static, str>, T)>),
    Tuple(Vec<T>),
    Dict(Box<T>, Box<T>),
}

impl<T> CompositeType<T> {
    pub fn list(t: impl Into<T>) -> Self {
        Self::List(Box::new(t.into()))
    }

    pub fn dict(k: impl Into<T>, v: impl Into<T>) -> Self {
        Self::Dict(Box::new(k.into()), Box::new(v.into()))
    }

    pub fn optional(t: impl Into<T>) -> Self {
        Self::Optional(Box::new(t.into()))
    }

    pub fn record(
        i: impl IntoIterator<Item = (impl Into<Cow<'static, str>>, impl Into<T>)>,
    ) -> Self {
        Self::Record(i.into_iter().map(|(n, v)| (n.into(), v.into())).collect())
    }

    pub fn function(p: impl IntoIterator<Item = impl Into<T>>, r: impl Into<T>) -> Self {
        Self::Function(
            p.into_iter().map(|i| i.into()).collect(),
            Box::new(r.into()),
        )
    }

    pub fn tuple(i: impl IntoIterator<Item = impl Into<T>>) -> Self {
        Self::Tuple(i.into_iter().map(|i| i.into()).collect())
    }

    pub fn map<F, U>(self, mut f: F) -> CompositeType<U>
    where
        F: FnMut(T) -> U,
    {
        match self {
            Self::List(t) => CompositeType::List(Box::new(f(*t))),
            Self::Optional(t) => CompositeType::Optional(Box::new(f(*t))),
            Self::Function(a, r) => {
                CompositeType::Function(a.into_iter().map(&mut f).collect(), Box::new(f(*r)))
            }
            Self::Tuple(t) => CompositeType::Tuple(t.into_iter().map(f).collect()),
            Self::Record(r) => {
                CompositeType::Record(r.into_iter().map(|(n, v)| (n, f(v))).collect())
            }
            Self::Dict(k, v) => CompositeType::Dict(Box::new(f(*k)), Box::new(f(*v))),
        }
    }
}

struct PrettyTypeDebug<'a, T> {
    type_: &'a T,
    open_types: &'a RefCell<Vec<TypeId>>,
}

impl<'a, T> PrettyTypeDebug<'a, T> {
    const TYPE_VAR_CHARS: [char; 26] = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];

    pub fn inner<'b, U>(&self, type_: &'b U) -> PrettyTypeDebug<'b, U>
    where
        'a: 'b,
    {
        PrettyTypeDebug {
            type_,
            open_types: self.open_types,
        }
    }
}

impl<'a> fmt::Debug for PrettyTypeDebug<'a, PrimitiveType> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_ {
            PrimitiveType::Void => write!(f, "Void"),
            PrimitiveType::String => write!(f, "String"),
            PrimitiveType::Integer => write!(f, "Integer"),
            PrimitiveType::Float => write!(f, "Float"),
            PrimitiveType::Boolean => write!(f, "Boolean"),
        }
    }
}

impl<'a, T> fmt::Debug for PrettyTypeDebug<'a, CompositeType<T>>
where
    PrettyTypeDebug<'a, T>: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_ {
            CompositeType::List(e) => {
                write!(f, "[")?;
                self.inner(e.as_ref()).fmt(f)?;
                write!(f, "]")
            }
            CompositeType::Optional(e) => {
                self.inner(e.as_ref()).fmt(f)?;
                write!(f, "?")
            }
            CompositeType::Dict(k, v) => {
                write!(f, "{{")?;
                self.inner(k.as_ref()).fmt(f)?;
                write!(f, ": ")?;
                self.inner(v.as_ref()).fmt(f)?;
                write!(f, "}}")
            }
            CompositeType::Tuple(t) => {
                write!(f, "(")?;
                for (i, e) in t.into_iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    self.inner(e).fmt(f)?;
                }
                write!(f, ")")
            }
            CompositeType::Function(a, r) => {
                write!(f, "(")?;
                for (i, e) in a.into_iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    self.inner(e).fmt(f)?;
                }
                write!(f, ")")?;

                write!(f, " -> ")?;
                self.inner(r.as_ref()).fmt(f)
            }
            CompositeType::Record(r) => {
                let mut m = f.debug_map();
                for (n, v) in r {
                    m.entry(&DisplayAsDebug(n), &self.inner(v));
                }
                m.finish()
            }
        }
    }
}

impl<'a> fmt::Debug for PrettyTypeDebug<'a, Type> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_ {
            Type::Primitive(p) => self.inner(p).fmt(f),
            Type::Composite(c) => self.inner(c).fmt(f),

            Type::Named(n, _) => write!(f, "{}", n),

            Type::Open(id) => {
                let mut open_types = self.open_types.borrow_mut();
                let idx = open_types
                    .iter()
                    .enumerate()
                    .find(|(_, oid)| *oid == id)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| {
                        let idx = open_types.len();
                        open_types.push(*id);
                        idx
                    });
                let idx = idx % Self::TYPE_VAR_CHARS.len();
                let ch = Self::TYPE_VAR_CHARS[idx];
                write!(f, "{}{:?}", ch, id)
            }
        }
    }
}

impl<'a> fmt::Debug for PrettyTypeDebug<'a, SpanType> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_ {
            SpanType::Primitive(_, p) => self.inner(p).fmt(f),
            SpanType::Composite(_, c) => self.inner(c).fmt(f),
            SpanType::Referenced(_, t, n) => match n {
                None => self.inner(t).fmt(f),
                Some(n) => write!(f, "{}", n),
            },
            SpanType::Open(_, id) => {
                let mut open_types = self.open_types.borrow_mut();
                let idx = open_types
                    .iter()
                    .enumerate()
                    .find(|(_, oid)| *oid == id)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| {
                        let idx = open_types.len();
                        open_types.push(*id);
                        idx
                    });
                let idx = idx % Self::TYPE_VAR_CHARS.len();
                let ch = Self::TYPE_VAR_CHARS[idx];
                write!(f, "{}{:?}", ch, id)
            }
        }
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        PrettyTypeDebug {
            type_: self,
            open_types: &Default::default(),
        }
        .fmt(f)
    }
}

impl fmt::Debug for SpanType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        PrettyTypeDebug {
            type_: self,
            open_types: &Default::default(),
        }
        .fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn primitive() {
        let span = Span {
            source_name: Arc::new("test".into()),
            range: Default::default(),
        };

        let lhs = Type::Primitive(PrimitiveType::String);
        let rhs = SpanType::Primitive(span.clone(), PrimitiveType::Integer);

        let mut env = TypeEnvironment::new();
        let result = env.unify(lhs, rhs);

        assert_eq!(
            result.errors,
            [TypeError::Mismatch {
                span: span.clone(),
                expected: Type::Primitive(PrimitiveType::String),
                assigned: Type::Primitive(PrimitiveType::Integer),
            }]
        );

        let lhs = Type::Primitive(PrimitiveType::String);
        let rhs = SpanType::Primitive(span.clone(), PrimitiveType::String);

        let mut env = TypeEnvironment::new();
        let result = env.unify(lhs, rhs);

        assert_eq!(result.errors, [])
    }

    #[test]
    fn primitive_open_assignment() {
        let span = Span {
            source_name: Arc::new("test".into()),
            range: Default::default(),
        };

        let lhs = Type::default();
        let rhs = SpanType::Primitive(span.clone(), PrimitiveType::Float);

        let mut env = TypeEnvironment::new();

        let result = env.unify(lhs, rhs);

        assert_eq!(
            result.value,
            SpanType::Primitive(span, PrimitiveType::Float)
        );
        assert_eq!(result.errors, []);
    }

    #[test]
    fn tuple_cross_open_types() {
        let span = Span {
            source_name: Arc::new("test".into()),
            range: Default::default(),
        };

        let a = Type::default();
        let b = Type::default();
        let c = Type::default();

        // (String, a, a)
        let lhs = Type::Composite(CompositeType::tuple([
            Type::Primitive(PrimitiveType::String),
            a.clone(),
            a.clone(),
        ]));

        // (b, Integer, c)
        let rhs = SpanType::Referenced(
            span.clone(),
            Type::Composite(CompositeType::tuple([
                b.clone(),
                Type::Primitive(PrimitiveType::Integer),
                c.clone(),
            ])),
            None,
        );

        let mut env = TypeEnvironment::new();

        let result = env.unify(lhs, rhs);

        assert_eq!(
            result.value,
            // (String, Integer, Integer)
            SpanType::Referenced(
                span,
                Type::Composite(CompositeType::tuple([
                    Type::Primitive(PrimitiveType::String),
                    Type::Primitive(PrimitiveType::Integer),
                    Type::Primitive(PrimitiveType::Integer),
                ])),
                None,
            )
        );
        assert_eq!(result.errors, []);
    }

    #[test]
    fn tuple_cross_open_types_inner_span() {
        let source_name = Arc::new("test".to_string());
        let first_element_span = Span {
            source_name: source_name.clone(),
            range: (1, 1).into()..(1, 2).into(),
        };
        let second_element_span = Span {
            source_name: source_name.clone(),
            range: (1, 2).into()..(1, 3).into(),
        };
        let tuple_span = first_element_span.through(&second_element_span);

        // (a, Integer)
        let lhs = Type::Composite(CompositeType::tuple([
            Type::default(),
            Type::Primitive(PrimitiveType::Integer),
        ]));

        // (Integer, String)
        let rhs = SpanType::Composite(
            tuple_span.clone(),
            CompositeType::tuple([
                SpanType::Primitive(first_element_span.clone(), PrimitiveType::Integer),
                SpanType::Primitive(second_element_span.clone(), PrimitiveType::String),
            ]),
        );

        let mut env = TypeEnvironment::new();
        let result = env.unify(lhs, rhs);

        assert_eq!(
            result.value,
            // (Integer, Integer)
            SpanType::Composite(
                tuple_span,
                CompositeType::tuple([
                    SpanType::Primitive(first_element_span, PrimitiveType::Integer),
                    SpanType::Primitive(second_element_span.clone(), PrimitiveType::Integer),
                ])
            )
        );

        assert_eq!(
            result.errors,
            [TypeError::Mismatch {
                span: second_element_span,
                expected: Type::Primitive(PrimitiveType::Integer),
                assigned: Type::Primitive(PrimitiveType::String),
            }]
        );
    }

    #[test]
    fn function() {
        let source_name = Arc::new("test".to_string());
        let fn_span = Span {
            source_name: source_name.clone(),
            range: (1, 1).into()..(1, 2).into(),
        };
        let arg_span = Span {
            source_name: source_name.clone(),
            range: (1, 2).into()..(1, 3).into(),
        };
        let call_span = fn_span.through(&arg_span);

        let a = Type::default();

        // a -> (a, Integer)
        let lhs = Type::Composite(CompositeType::function(
            [a.clone()],
            Type::Composite(CompositeType::tuple([
                a,
                Type::Primitive(PrimitiveType::Integer),
            ])),
        ));

        // String -> b
        let rhs = SpanType::Composite(
            fn_span.clone(),
            CompositeType::function(
                [SpanType::Primitive(arg_span.clone(), PrimitiveType::String)],
                SpanType::Open(call_span.clone(), Default::default()),
            ),
        );

        let mut env = TypeEnvironment::new();
        let result = env.unify(lhs, rhs);

        assert_eq!(
            result.value,
            // String -> (String, Integer)
            SpanType::Composite(
                fn_span.clone(),
                CompositeType::function(
                    [SpanType::Primitive(arg_span, PrimitiveType::String)],
                    SpanType::Referenced(
                        call_span,
                        Type::Composite(CompositeType::tuple([
                            Type::Primitive(PrimitiveType::String),
                            Type::Primitive(PrimitiveType::Integer),
                        ])),
                        None,
                    )
                )
            )
        );
        assert_eq!(result.errors, []);
    }

    #[test]
    fn optional_subsumption() {
        let span = Span {
            source_name: Arc::new("test".to_string()),
            range: Default::default(),
        };

        // a?
        let lhs = Type::Composite(CompositeType::optional(Type::default()));

        // String
        let rhs = SpanType::Primitive(span.clone(), PrimitiveType::String);

        let mut env = TypeEnvironment::new();
        let result = env.unify(lhs, rhs);

        assert_eq!(
            result.value,
            // String?
            SpanType::Composite(
                span.clone(),
                CompositeType::optional(SpanType::Primitive(span, PrimitiveType::String))
            )
        );
        assert_eq!(result.errors, []);
    }

    #[test]
    fn contravariant_function_parameters() {
        let source_name = Arc::new("test".to_string());
        let fn_span = Span {
            source_name: source_name.clone(),
            range: (1, 1).into()..(1, 2).into(),
        };
        let arg_span = Span {
            source_name: source_name.clone(),
            range: (1, 2).into()..(1, 3).into(),
        };
        let call_span = fn_span.through(&arg_span);

        // String -> String
        let lhs = Type::Composite(CompositeType::function(
            [Type::Primitive(PrimitiveType::String)],
            Type::Primitive(PrimitiveType::String),
        ));

        // String? -> String
        let rhs = SpanType::Composite(
            fn_span.clone(),
            CompositeType::function(
                [SpanType::Composite(
                    arg_span.clone(),
                    CompositeType::optional(SpanType::Primitive(
                        arg_span.clone(),
                        PrimitiveType::String,
                    )),
                )],
                SpanType::Primitive(call_span.clone(), PrimitiveType::String),
            ),
        );

        let mut env = TypeEnvironment::new();
        let result = env.unify(lhs, rhs);

        assert_eq!(
            result.value,
            // String -> String
            SpanType::Composite(
                fn_span.clone(),
                CompositeType::function(
                    [SpanType::Primitive(arg_span, PrimitiveType::String)],
                    SpanType::Primitive(call_span, PrimitiveType::String)
                )
            )
        );
        assert_eq!(result.errors, []);
    }
}
