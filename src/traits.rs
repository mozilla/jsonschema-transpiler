use super::Context;

/// A translation between two schema formats that may fail under certain
/// conditions.
///
/// This is the similar to the `TryFrom` trait, but also requires the
/// implementor to include a context struct for run-time modifications to the
/// schema. The most concrete use of the context struct is to provide an
/// appropriate error handling mechanism when the JSON Schema contains an empty
/// field. Depending on the use-case, it may be more appropriate to signal
/// immediate failure over something like dropping the field.
///
/// https://doc.rust-lang.org/src/core/convert.rs.html#478-486
pub trait TranslateFrom<T>: Sized {
    type Error;

    fn translate_from(value: T, context: Context) -> Result<Self, Self::Error>;
}

/// A translation between two schema formats. It is the reciprocal of
/// [`TranslateFrom']
pub trait TranslateInto<T>: Sized {
    type Error;

    fn translate_into(self, context: Context) -> Result<T, Self::Error>;
}

// TranslateFrom implies TranslateInto
impl<T, U> TranslateInto<U> for T
where
    U: TranslateFrom<T>,
{
    type Error = U::Error;
    fn translate_into(self, context: Context) -> Result<U, U::Error> {
        U::translate_from(self, context)
    }
}
