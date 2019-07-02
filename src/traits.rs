use super::Context;

/// A translation between two schema formats that may fail under certain
/// conditions.
///
/// This is similar to the `TryFrom` trait, but requires the implementor to pass
/// a Context struct for runtime modifications to the schema. A concrete use
/// context is to decide on an appropriate error handling mechanism when a JSON
/// Schema contains an empty field. Given a use-case, it may be more appropriate
/// to fail fast and panic over dropping or casting the field.
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
