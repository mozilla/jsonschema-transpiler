extern crate clap;
extern crate env_logger;
extern crate jst;

use clap::{App, Arg};
use serde_json::Value;
use std::fs::File;
use std::io::{self, BufReader};

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("FILE")
                .help("Sets the input file to use")
                .takes_value(true)
                .index(1),
        )
        .arg(
            Arg::with_name("type")
                .help("The output schema format")
                .short("t")
                .long("type")
                .takes_value(true)
                .possible_values(&["avro", "bigquery"])
                .default_value("avro"),
        )
        .get_matches();

    let reader: Box<io::Read> = match matches.value_of("FILE") {
        Some(path) if path == "-" => Box::new(io::stdin()),
        Some(path) => {
            let file = File::open(path).unwrap();
            Box::new(BufReader::new(file))
        }
        None => Box::new(io::stdin()),
    };
    let data: Value = serde_json::from_reader(reader).unwrap();

    let output = match matches.value_of("type").unwrap() {
        "avro" => jst::convert_avro(&data),
        "bigquery" => jst::convert_bigquery(&data),
        _ => panic!("Unknown type!"),
    };
    let pretty = serde_json::to_string_pretty(&output).unwrap();
    println!("{}", pretty);
}
