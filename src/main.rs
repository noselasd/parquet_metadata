use clap::Parser;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::printer;
use std::fs::File;

#[derive(clap::ValueEnum, Clone, Debug)]
enum InfoType {
    All,
    Metadata,
    Schema,
}

#[derive(Parser)]
#[clap(version)]
#[clap(about = "Prints out parquet metadata for each file")]
struct Args {
    #[clap(long)]
    #[clap(help = "What to print")]
    #[clap(value_enum, default_value_t=InfoType::All)]
    info: InfoType,

    #[clap(value_parser, required(true))]
    #[clap(help = "parquet file(s)")]
    files: Vec<String>,
}

fn print_metedata(filename: &str, info_type: &InfoType) -> Result<(), std::io::Error> {
    let reader_options = ReadOptionsBuilder::new().with_page_index().build();
    let file = File::open(filename)?;

    let reader = SerializedFileReader::new_with_options(file, reader_options).unwrap();
    let metadata = reader.metadata();

    let output = &mut std::io::stdout();
    match info_type {
        InfoType::Schema => {
            println!("Schema for {}", filename);
            printer::print_schema(output, metadata.file_metadata().schema());
        }
        InfoType::Metadata => {
            println!("File metadata for {}", filename);
            printer::print_file_metadata(output, metadata.file_metadata());
        }
        InfoType::All => {
            println!("Metadata for  {}", filename);
            printer::print_parquet_metadata(output, metadata);
        }
    }

    Ok(())
}

fn main() {
    let args = Args::parse();
    for arg in &args.files {
        if let Err(why) = print_metedata(arg, &args.info) {
            println!("Can't open {} : {}", arg, why);
        }
    }
}
