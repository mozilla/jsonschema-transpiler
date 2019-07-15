use onig::Regex;

/// Normalize the casing of a string to be `snake_case`.
///
/// This function produces strings that are transformed consistently from a
/// variety of different input casing. The rule-set for word boundaries are
/// derived from the withoutboats/heck crate. Underscores are considered word
/// boundaries in addition to the standard pattern e.g. `\b`. `camelCasing` is
/// detected by a lowercase followed by an uppercase. Numbers can take on either
/// case depending on the preceeding symbol.
///
/// See: https://github.com/withoutboats/heck/blob/master/src/lib.rs#L7-L17
pub fn to_snake_case(input: &str) -> String {
    lazy_static! {
        static ref EXTRA_SYMBOL: Regex = Regex::new(r"[^\w]|_").unwrap();
        // This regex matches camelCase in reverse, since the lookbehind
        // operation only accepts patterns of fixed length. Reversing let's us
        // determine whether several digits will be uppercase or lowercase.
        static ref REV_WORD_BOUNDARY: Regex = Regex::new(
            r"(?x)
            \b                              # standard word boundary
            |(?<=[a-z][A-Z])(?=\d*[A-Z])    # break on runs of uppercase e.g. A7Aa -> A7|Aa
            |(?<=[a-z][A-Z])(?=\d*[a-z])    # break in runs of lowercase e.g a7Aa -> a7|Aa
            |(?<=[A-Z])(?=\d*[a-z])         # ends with an uppercase e.g. a7A -> a7|A
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
