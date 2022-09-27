use clap::Parser;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::printer;
use std::fs::File;

#[derive(Parser)]
#[clap(version)]
#[clap(about = "Prints out parquet metadata for each file")]
struct Args {
    #[clap(short, long)]
    #[clap(help = "Print schema only")]
    schema: bool,

    #[clap(value_parser, required(true))]
    #[clap(help = "parquet file(s)")]
    files: Vec<String>,
}

fn print_metedata(filename: &str, schema_only: bool) -> Result<(), std::io::Error> {
    let reader_options = ReadOptionsBuilder::new().with_page_index().build();
    let file = File::open(filename)?;

    let reader = SerializedFileReader::new_with_options(file, reader_options).unwrap();
    let metadata = reader.metadata();

    let output = &mut std::io::stdout();
    if schema_only {
        println!("Schema for {}", filename);
        printer::print_schema(output, metadata.file_metadata().schema())
    } else {
        println!("Metadata for  {}", filename);
        printer::print_parquet_metadata(output, metadata);
    }

    Ok(())
}

fn main() {
    let flags = Args::parse();
    for arg in &flags.files {
        if let Err(why) = print_metedata(arg, flags.schema) {
            println!("Can't open {} : {}", arg, why);
        }
    }
}
