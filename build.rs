#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

use serde::Deserialize;
use serde_json::{json, Map, Result, Value};
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Deserialize, Debug)]
struct TestData {
    avro: Value,
    bigquery: Value,
    json: Value,
}

#[derive(Deserialize, Debug)]
struct TestCase {
    name: String,
    test: TestData,
}

#[derive(Deserialize, Debug)]
struct TestSuite {
    name: String,
    tests: Vec<TestCase>,
}

fn format_json(obj: Value) -> String {
    let pretty = serde_json::to_string_pretty(&obj).unwrap();
    // 4 spaces
    pretty.replace("\n", "\n    ")
}

fn write_avro_tests(mut outfile: File, suite: &TestSuite) {
    write!(
        outfile,
        r#"
use converter::convert_avro_direct;
use serde_json::Value;
"#
    );
    for case in &suite.tests {
        let formatted = format!(
            r##"
#[test]
fn avro_{name}() {{
    let input_data = r#"
    {input_data}
    "#;
    let expected_data = r#"
    {expected}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}}
"##,
            name = case.name,
            input_data = format_json(case.test.json.clone()),
            expected = format_json(case.test.avro.clone()),
        );
        write!(outfile, "{}", formatted).unwrap()
    }
}

fn write_bigquery_tests(mut outfile: File, suite: &TestSuite) {
    write!(
        outfile,
        r#"
use converter::convert_bigquery_direct;
use serde_json::Value;
"#
    );
    for case in &suite.tests {
        let formatted = format!(
            r##"
#[test]
fn bigquery_{name}() {{
    let input_data = r#"
    {input_data}
    "#;
    let expected_data = r#"
    {expected}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}}
"##,
            name = case.name,
            input_data = format_json(case.test.json.clone()),
            expected = format_json(case.test.bigquery.clone()),
        );
        write!(outfile, "{}", formatted).unwrap()
    }
}

fn generate_tests(input: PathBuf, output: &Path) {
    let file = File::open(input).unwrap();
    let reader = BufReader::new(file);
    let suite: TestSuite = serde_json::from_reader(reader).unwrap();
    println!("{:?}", suite);

    let avro_dst = output.join(format!("avro_{}.rs", suite.name));
    let avro_file = File::create(&avro_dst).unwrap();
    write_avro_tests(avro_file, &suite);

    let bq_dst = output.join(format!("bigquery_{}.rs", suite.name));
    let bq_file = File::create(&bq_dst).unwrap();
    write_bigquery_tests(bq_file, &suite);
}

fn main() {
    let test_directory = "tests";
    let test_cases = "tests/resources";
    for entry in fs::read_dir(test_cases).unwrap() {
        let path = entry.unwrap().path();
        println!("Test file: {:?}", path);
        generate_tests(path, Path::new(test_directory));
    }
}
