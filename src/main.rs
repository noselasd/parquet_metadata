use clap::ArgGroup;
use clap::Parser;
use parquet::file::metadata::FileMetaData;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::printer;
use parquet::schema::types::SchemaDescriptor;
use std::fs::File;
use std::io;

#[allow(unused_must_use)]
fn print_arrow_schema(out: &mut dyn io::Write, metadata: &FileMetaData) {
    let kv = match metadata.key_value_metadata() {
        Some(x) => x,
        None => {
            writeln!(out, "File contains no arrow schema");
            return;
        }
    };
    let arrow_schema = kv
        .iter()
        .find(|&k| k.key == parquet::arrow::ARROW_SCHEMA_META_KEY);

    let v = match arrow_schema {
        Some(k) => k.value.as_ref(),
        None => {
            writeln!(out, "File contains no arrow schema");
            return;
        }
    };
    writeln!(out, "Arrow schema:");
    let empty = &String::new();
    let arrow_schema = v.unwrap_or(empty);

    let decoded = base64::decode(arrow_schema);
    match decoded {
        Ok(bytes) => {
            let slice = if bytes[0..4] == [255u8; 4] {
                &bytes[8..]
            } else {
                bytes.as_slice()
            };

            match arrow::ipc::root_as_message(slice) {
                Ok(message) => writeln!(out, "Deoded arrow schema{:#?}", message),
                Err(err) => writeln!(out, "Error {}", err),
            };
        }
        Err(err) => {
            writeln!(out, "Error {}", err);
        }
    };
}
#[allow(unused_must_use)]
fn print_parquet_schema_descriptor(out: &mut dyn io::Write, schema_descr: &SchemaDescriptor) {
    writeln!(out, "{:#?}", schema_descr);
}
fn print_metedata(filename: &str, info_type: InfoType) -> Result<(), std::io::Error> {
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
        InfoType::SchemaDescr => {
            println!("Internal schema descriptor for  {}", filename);
            let schema_descr = metadata.file_metadata().schema_descr();
            print_parquet_schema_descriptor(output, schema_descr);
        }
        InfoType::ArrowSchema => {
            println!("ARROW:schema for {}", filename);
            print_arrow_schema(output, metadata.file_metadata())
        }
    }

    Ok(())
}

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
        _ => InfoType::All, // Default is --all
    }
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum InfoType {
    All,
    Metadata,
    Schema,
    SchemaDescr,
    ArrowSchema,
}

#[derive(Parser)]
#[clap(version)]
#[clap(about = "Prints out parquet metadata for each file")]
#[clap()]
#[clap(group(
    ArgGroup::new("info_type")
        .required(false)
        .args(&["all","metadata", "schema", "schema-descr", "arrow-schema"]),
))]
#[clap(arg_required_else_help = true)]
struct Args {
    #[clap(long)]
    #[clap(help = "Print all info")]
    all: bool,

    #[clap(long)]
    #[clap(help = "Print file metadata")]
    metadata: bool,

    #[clap(long)]
    #[clap(help = "Print schema")]
    schema: bool,

    #[clap(long)]
    #[clap(help = "Print schema description")]
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
        if let Err(why) = print_metedata(arg, get_info_type(&args)) {
            println!("Can't open {} : {}", arg, why);
        }
    }
}
