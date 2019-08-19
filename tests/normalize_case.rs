use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use pretty_assertions::assert_eq;
use serde_json::Value;

use jst::casing::to_snake_case;
use jst::{convert_avro, convert_bigquery};
use jst::{Context, ResolveMethod};

fn test_data() -> Value {
    serde_json::from_str(
        r#"
    {
        "type": "object",
        "properties": {
            "test_snake_case": {"type": "boolean"},
            "testCamelCase": {"type": "boolean"},
            "TestPascalCase": {"type": "boolean"},
            "TEST_SCREAMING_SNAKE_CASE": {"type": "boolean"}
        },
        "required": [
            "test_snake_case",
            "testCamelCase",
            "TestPascalCase",
            "TEST_SCREAMING_SNAKE_CASE"
        ]
    }
    "#,
    )
    .unwrap()
}

/// Get the resource path for all the casing tests
fn resource_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/resources/casing");
    path
}

/// Test the `to_snake_case` method against a test file in the format
/// `reference,expected`
fn snake_case_test(case_name: &str) {
    let mut path = resource_path();
    path.push(case_name);
    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line.unwrap().to_string();
        let cols: Vec<&str> = line.split(",").collect();
        assert_eq!(cols.len(), 2);
        assert_eq!(to_snake_case(cols[0]), cols[1]);
    }
}

#[test]
fn test_snake_casing_alphanum_3() {
    // all strings of length 3 drawn from the alphabet "aA7"
    snake_case_test("alphanum_3.csv");
}

#[test]
fn test_snake_casing_word_4() {
    // all strings of length 4 drawn from the alphabet "aA7_"
    snake_case_test("word_4.csv");
}

#[test]
fn test_snake_casing_mps_diff_integration() {
    // all column names from mozilla-pipeline-schemas affected by snake_casing
    // https://github.com/mozilla/jsonschema-transpiler/pull/79#issuecomment-509839572
    // https://gist.github.com/acmiyaguchi/3f526c440b67ebe469bcb6ab2da5123f#file-readme-md
    snake_case_test("mps-diff-integration.csv");
}

#[test]
fn test_bigquery_normalize_snake_casing() {
    let context = Context {
        normalize_case: true,
        resolve_method: ResolveMethod::Panic,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
        [
            {
                "mode": "REQUIRED",
                "name": "test_camel_case",
                "type": "BOOL"
            },
            {
                "mode": "REQUIRED",
                "name": "test_pascal_case",
                "type": "BOOL"
            },
            {
                "mode": "REQUIRED",
                "name": "test_screaming_snake_case",
                "type": "BOOL"
            },
            {
                "mode": "REQUIRED",
                "name": "test_snake_case",
                "type": "BOOL"
            }
        ]
        "#,
    )
    .unwrap();

    assert_eq!(expected, convert_bigquery(&test_data(), context));
}

#[test]
fn test_avro_normalize_snake_casing() {
    let context = Context {
        normalize_case: true,
        resolve_method: ResolveMethod::Panic,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
        {
        "fields": [
            {
                "name": "test_camel_case",
                "type": {"type": "boolean"}
            },
            {
                "name": "test_pascal_case",
                "type": {"type": "boolean"}
            },
            {
                "name": "test_screaming_snake_case",
                "type": {"type": "boolean"}
            },
            {
                "name": "test_snake_case",
                "type": {"type": "boolean"}
            }
        ],
        "name": "root",
        "type": "record"
        }
        "#,
    )
    .unwrap();

    assert_eq!(expected, convert_avro(&test_data(), context));
}
