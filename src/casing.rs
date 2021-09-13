#[cfg(not(feature = "oniguruma"))]
use heck::SnakeCase;
#[cfg(feature = "oniguruma")]
use lazy_static::lazy_static;
#[cfg(feature = "oniguruma")]
use onig::Regex;

/// Normalize the case of a string to be `snake_case`.
///
/// This function produces internally-consistent snake-casing that performs well
/// in many situations. The rule-set for word boundaries are consistent with the
/// withoutboats/heck crate. Several benefits include treating embedded
/// mnemonics like `RAM` and `XMLHttpRequest` in an intuitive fashion. See
/// `tests/resources/casing/mps-diff-integration.csv` in the test sources for
/// empirical use of this casing logic.
///
/// Underscores are considered word boundaries alongside the standard `\b`
/// pattern. Boundaries in `camelCasing` are found by instances of a lowercase
/// followed by an uppercase. Digits can be either lowercase or uppercase
/// depending on the case of the most recent letter. Sequences of underscores
/// are not significant and therefore cannot be used to encode other characters
/// e.g. `-` cannot be represented via `__` because `_` is a word boundary.
///
/// ## References
///
/// * [Reference Python3 implementation](https://github.com/acmiyaguchi/test-casing/blob/8ca3d68db512fd3a17868c0b08cc84909ebebbc7/src/main.py#L1-L34)
/// * [[withoutboats/heck] - Definition of a word boundary](https://github.com/withoutboats/heck/blob/093d56fbf001e1506e56dbfa38631d99b1066df1/src/lib.rs#L7-L17)
/// * [[RexEgg] - Regex Boundaries and Delimiters—Standard and Advanced](https://www.rexegg.com/regex-boundaries.html)
/// * [[StackOverflow] - RegEx to split camelCase or TitleCase (advanced)](https://stackoverflow.com/a/7599674)
/// * [[StackOverflow] - What's the technical reason for “lookbehind assertion MUST be fixed length” in regex?](https://stackoverflow.com/a/40078049)
#[cfg(not(feature = "oniguruma"))]
pub fn to_snake_case(input: &str) -> String {
    input.to_snake_case()
}
#[cfg(feature = "oniguruma")]
pub fn to_snake_case(input: &str) -> String {
    lazy_static! {
        static ref EXTRA_SYMBOL: Regex = Regex::new(r"[^\w]|_").unwrap();
        // This regex matches camelCase in reverse, since the lookbehind
        // operation only accepts patterns of fixed length. This "inverted"
        // lookahead can help determine whether a digit is lowercase or
        // uppercase.
        static ref REV_WORD_BOUNDARY: Regex = Regex::new(
            r"(?x)
            \b                              # standard word boundary
            |(?<=[a-z][A-Z])(?=\d*[A-Z])    # break on runs of uppercase e.g. A7Aa -> A7|Aa
            |(?<=[a-z][A-Z])(?=\d*[a-z])    # break on runs of lowercase e.g a7Aa -> a7|Aa
            |(?<=[A-Z])(?=\d*[a-z])         # break on final uppercase e.g. a7A -> a7|A
            ",
        )
        .unwrap();
    }
    let subbed: String = EXTRA_SYMBOL.replace_all(input, " ").chars().rev().collect();
    let words: Vec<&str> = REV_WORD_BOUNDARY
        .split(&subbed)
        .filter(|s| !s.trim().is_empty())
        .collect();
    words.join("_").to_lowercase().chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! case {
        ($test:expr, $expect:expr) => {
            assert_eq!(to_snake_case($test), $expect)
        };
    }
    #[test]
    fn test_to_snake_case() {
        // one word
        case!("Aa", "aa");
        // two words
        case!("aA", "a_a");
        // underscores are word boundaries
        case!("_a__a_", "a_a");
        // mnemonics are considered words
        case!("RAM", "ram");
        // numbers can be lowercase
        case!("a7aAa", "a7a_aa");
        // numbers can be uppercase
        case!("A7AAa", "a7a_aa");
    }
}
