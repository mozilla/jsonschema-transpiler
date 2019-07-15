#[macro_use]
extern crate serde;
extern crate serde_json;

use serde_json::Value;
use std::env;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{BufReader, Write};

#[derive(Serialize, Deserialize, Debug)]
struct TestData {
    avro: Value,
    bigquery: Value,
    json: Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct TestCase {
    #[serde(default, skip_serializing_if = "Value::is_null")]
    description: Value,
    name: String,
    // True if the schema does not involve ambiguous sections
    #[serde(default)]
    compatible: bool,
    test: TestData,
}

#[derive(Serialize, Deserialize, Debug)]
struct TestSuite {
    #[serde(default, skip_serializing_if = "Value::is_null")]
    description: Value,
    name: String,
    tests: Vec<TestCase>,
}

const TRUTHY_ENV_VALUES: [&str; 5] = ["y", "yes", "t", "true", "1"];

fn get_env_var_as_bool(var: &str, default: bool) -> bool {
    match env::var(var) {
        Ok(val) => TRUTHY_ENV_VALUES.contains(&val.to_lowercase().as_ref()),
        _ => default,
    }
}

fn format_json(obj: Value) -> String {
    let pretty = serde_json::to_string_pretty(&obj).unwrap();
    // 4 spaces
    pretty.replace("\n", "\n    ")
}

fn write_backup(path: &std::path::PathBuf) {
    let mut backup = path.to_path_buf();
    let mut extension = OsString::new();
    if let Some(s) = backup.extension() {
        extension.push(s);
        extension.push(".");
    };
    extension.push("bak");
    backup.set_extension(extension);
    println!("Backing up: {:?} -> {:?}", path, backup);
    fs::copy(path, backup).unwrap();
}

fn write_formatted_test(path: &std::path::PathBuf, suite: &TestSuite) {
    println!("Formatting test: {:?}", path);
    let formatted = serde_json::to_string_pretty(suite).unwrap();
    let fp_write = File::create(path).unwrap();
    write!(&fp_write, "{}\n", formatted).unwrap()
}

fn write_avro_tests(mut outfile: &File, suite: &TestSuite) {
    for case in &suite.tests {
        let formatted = format!(
            r##"
#[test]{should_panic}
fn avro_{name}() {{
    let input_data = r#"
    {input_data}
    "#;
    let expected_data = r#"
    {expected}
    "#;
    let mut context = Context {{
        ..Default::default()
    }};
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_avro(&input, context);
}}
"##,
            name = case.name,
            should_panic = if case.compatible {
                ""
            } else {
                "\n#[should_panic]"
            },
            input_data = format_json(case.test.json.clone()),
            expected = format_json(case.test.avro.clone()),
        );
        write!(outfile, "{}", formatted).unwrap()
    }
}

fn write_bigquery_tests(mut outfile: &File, suite: &TestSuite) {
    for case in &suite.tests {
        let formatted = format!(
            r##"
#[test]{should_panic}
fn bigquery_{name}() {{
    let input_data = r#"
    {input_data}
    "#;
    let expected_data = r#"
    {expected}
    "#;
    let mut context = Context {{
        ..Default::default()
    }};
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}}
"##,
            name = case.name,
            should_panic = if case.compatible {
                ""
            } else {
                "\n#[should_panic]"
            },
            input_data = format_json(case.test.json.clone()),
            expected = format_json(case.test.bigquery.clone()),
        );
        write!(outfile, "{}", formatted).unwrap()
    }
}

fn main() {
    let test_cases = "tests/resources/translate";
    let mut avro_fp = File::create("tests/transpile_avro.rs").unwrap();
    let mut bq_fp = File::create("tests/transpile_bigquery.rs").unwrap();
    let format_tests = get_env_var_as_bool("FORMAT_TESTS", true);
    let backup = get_env_var_as_bool("FORMAT_TESTS_BACKUP", false);

    write!(
        avro_fp,
        r#"use jst::convert_avro;
use jst::{{Context, ResolveMethod}};
use pretty_assertions::assert_eq;
use serde_json::Value;
"#
    )
    .unwrap();

    write!(
        bq_fp,
        r#"use jst::convert_bigquery;
use jst::{{Context, ResolveMethod}};
use pretty_assertions::assert_eq;
use serde_json::Value;
"#
    )
    .unwrap();

    let mut paths: Vec<_> = fs::read_dir(test_cases)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|e| match e.file_name() {
            Some(os_str) => !os_str.to_str().unwrap().starts_with("."),
            None => false,
        })
        .collect();
    paths.sort();
    for path in paths {
        println!("Test file: {:?}", path);
        let file = File::open(&path).unwrap();
        let reader = BufReader::new(file);
        let suite: TestSuite = serde_json::from_reader(reader).unwrap();
        write_avro_tests(&avro_fp, &suite);
        write_bigquery_tests(&bq_fp, &suite);
        if backup {
            write_backup(&path);
        }
        if format_tests {
            write_formatted_test(&path, &suite);
        }
    }
}
