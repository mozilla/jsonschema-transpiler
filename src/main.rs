use clap::{Parser, ValueEnum};
use jst::{Context, ResolveMethod};
use serde_json::Value;
use std::fs::File;
use std::io::{self, BufReader};

/// Output schema format
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Type {
    /// Avro format
    #[default]
    Avro,
    /// BigQuery format
    Bigquery,
}

/// Resolution strategy
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Resolve {
    /// Cast incompatible/under-specified schemas
    #[default]
    Cast,
    /// Panic on incompatible/under-specified schemas
    Panic,
    /// Drop incompatible/under-specified schemas
    Drop,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Sets the input file to use
    file: Option<String>,

    /// The output schema format
    #[arg(short, long = "type", value_enum, default_value_t = Type::Avro, value_name = "TYPE")]
    typ: Type,

    /// The resolution strategy for incompatible or under-specified schema
    #[arg(short, long, value_enum, default_value_t = Resolve::Cast)]
    resolve: Resolve,

    /// snake_case column-names for consistent behavior between SQL engines
    #[arg(short = 'c', long)]
    normalize_case: bool,

    /// Treats all columns as NULLABLE, ignoring the required section in the JSON Schema object
    #[arg(short = 'n', long)]
    force_nullable: bool,

    /// Treats tuple validation as an anonymous struct
    #[arg(long)]
    tuple_struct: bool,

    /// Produces maps without a value field for incompatible or under-specified value schema
    #[arg(short = 'w', long)]
    allow_maps_without_value: bool,
}

fn main() {
    env_logger::init();

    let args = Args::parse();

    let reader: Box<dyn io::Read> = match &args.file {
        Some(path) if path == "-" => Box::new(io::stdin()),
        Some(path) => {
            let file = File::open(path).unwrap();
            Box::new(BufReader::new(file))
        }
        None => Box::new(io::stdin()),
    };
    let data: Value = serde_json::from_reader(reader).unwrap();
    let context = Context {
        resolve_method: match args.resolve {
            Resolve::Cast => ResolveMethod::Cast,
            Resolve::Panic => ResolveMethod::Panic,
            Resolve::Drop => ResolveMethod::Drop,
        },
        normalize_case: args.normalize_case,
        force_nullable: args.force_nullable,
        tuple_struct: args.tuple_struct,
        allow_maps_without_value: args.allow_maps_without_value,
    };

    let output = match args.typ {
        Type::Avro => jst::convert_avro(&data, context),
        Type::Bigquery => jst::convert_bigquery(&data, context),
    };
    let pretty = serde_json::to_string_pretty(&output).unwrap();
    println!("{}", pretty);
}
