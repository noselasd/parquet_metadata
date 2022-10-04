use clap::ArgGroup;
use clap::Parser;
mod printer;
use printer::InfoType;

fn get_info_type(args: &Args) -> InfoType {
    match (
        args.metadata,
        args.schema,
        args.schema_descr,
        args.arrow_schema,
    ) {
        (true, _, _, _) => InfoType::Metadata,
        (_, true, _, _) => InfoType::Schema,
        (_, _, true, _) => InfoType::SchemaDescr,
        (_, _, _, true) => InfoType::ArrowSchema,
        _ => InfoType::Parquet, // Default is --parquet
    }
}

#[derive(Parser)]
#[clap(version)]
#[clap(about = "Prints out parquet metadata for each file")]
#[clap()]
#[clap(group(
    ArgGroup::new("info_type")
        .required(false)
        .args(&["parquet","metadata", "schema", "schema-descr", "arrow-schema"]),
))]
#[clap(arg_required_else_help = true)]
struct Args {
    #[clap(long)]
    #[clap(help = "Print all parquet metadata (default)")]
    parquet: bool,

    #[clap(long)]
    #[clap(help = "Print file metadata only")]
    metadata: bool,

    #[clap(long)]
    #[clap(help = "Print parquet schema only")]
    schema: bool,

    #[clap(long)]
    #[clap(help = "Print internal parquet schema description")]
    schema_descr: bool,

    #[clap(long)]
    #[clap(help = "Print ARROW:schema")]
    arrow_schema: bool,

    #[clap(value_parser, required = true)]
    #[clap(help = "parquet file(s)")]
    files: Vec<String>,
}

fn main() {
    let args = Args::parse();

    for arg in &args.files {
        if let Err(why) = printer::print_metedata(arg, get_info_type(&args)) {
            println!("Can't open {} : {}", arg, why);
        }
    }
}
