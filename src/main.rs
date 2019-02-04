extern crate clap;
extern crate converter;

use clap::{App, Arg};

fn main() {
    let matches = App::new("jst")
        .version("0.1")
        .author("Anthony Miyaguchi <amiyaguchi@mozilla.com>")
        .arg(
            Arg::with_name("from_file")
                .short("f")
                .long("from-file")
                .value_name("FILE")
                .takes_value(true),
        )
        .get_matches();

    println!("Value of input: {}", matches.value_of("from_file").unwrap());
}
