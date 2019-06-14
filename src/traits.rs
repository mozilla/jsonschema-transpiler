use super::Context;

// https://doc.rust-lang.org/src/core/convert.rs.html#478-486
pub trait Translate<T>: Sized {
    type Error;

    fn translate(value: T, context: Option<Context>) -> Result<Self, Self::Error>;
}

pub trait TranslateInto<T>: Sized {
    type Error;

    fn translate_into(self, context: Option<Context>) -> Result<T, Self::Error>;
}

// Translate implies TranslateInto
impl<T, U> TranslateInto<U> for T
where
    U: Translate<T>,
{
    type Error = U::Error;
    fn translate_into(self, context: Option<Context>) -> Result<U, U::Error> {
        U::translate(self, context)
    }
}
