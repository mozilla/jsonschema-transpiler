use onig::Regex;

pub fn to_snake_case(input: &str) -> String {
    lazy_static! {
        static ref EXTRA_SYMBOL: Regex = Regex::new(r"[^\w]|_").unwrap();
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
