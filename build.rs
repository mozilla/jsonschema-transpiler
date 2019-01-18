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

fn generate_tests(input: PathBuf, output: &Path) {
    let file = File::open(input).unwrap();
    let reader = BufReader::new(file);
    let suite: TestSuite = serde_json::from_reader(reader).unwrap();
    println!("{:?}", suite);

    let destination = output.join(format!("{}.rs", suite.name));
    let mut outfile = File::create(&destination).unwrap();
    write!(
        outfile,
r#"
use converter::convert_avro_direct;
use serde_json::Value;
"#
    );
    for case in suite.tests {
        let formatted = format!(
            r##"
#[test]
fn {name}() {{
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
            input_data = format_json(case.test.json),
            expected = format_json(case.test.avro),
        );
        write!(outfile, "{}", formatted).unwrap()
    }
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
