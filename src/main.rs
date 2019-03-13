extern crate clap;
extern crate jst;

use clap::{App, Arg};
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;

fn main() {
    let matches = App::new("jst")
        .version("0.1")
        .author("Anthony Miyaguchi <amiyaguchi@mozilla.com>")
        .arg(
            Arg::with_name("from-file")
                .short("f")
                .long("from-file")
                .value_name("FILE")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("type")
                .long("type")
                .takes_value(true)
                .possible_values(&["avro", "bigquery"])
                .required(true),
        )
        .get_matches();

    let path = matches.value_of("from-file").unwrap();
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    let data: Value = serde_json::from_reader(reader).unwrap();

    let output = match matches.value_of("type").unwrap() {
        "avro" => jst::convert_avro(&data),
        "bigquery" => jst::convert_bigquery(&data),
        _ => panic!(),
    };
    let pretty = serde_json::to_string_pretty(&output).unwrap();
    println!("{}", pretty);
}
