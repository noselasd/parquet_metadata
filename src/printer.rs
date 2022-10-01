use parquet::file::metadata::FileMetaData;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet::schema::printer;
use parquet::schema::types::SchemaDescriptor;
use std::fs::File;
use std::io;

#[derive(Clone, Debug)]
pub(crate) enum InfoType {
    All,
    Metadata,
    Schema,
    SchemaDescr,
    ArrowSchema,
}

fn find_arrow_schema(metadata: &FileMetaData) -> Option<&String> {
    if let Some(kv_vec) = metadata.key_value_metadata() {
      kv_vec
          .iter()
          .find(|&kv| kv.key == parquet::arrow::ARROW_SCHEMA_META_KEY)?
          .value
          .as_ref()
    } else {
        None
    }
}

#[allow(unused_must_use)]
fn print_arrow_schema(out: &mut dyn io::Write, metadata: &FileMetaData) {
    if let Some(arrow_schema) = find_arrow_schema(metadata) {
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
    } else {
        writeln!(out, "File contains no arrow schema");
    }
}

#[allow(unused_must_use)]
fn print_parquet_schema_descriptor(out: &mut dyn io::Write, schema_descr: &SchemaDescriptor) {
    writeln!(out, "{:#?}", schema_descr);
}

pub(crate) fn print_metedata(filename: &str, info_type: InfoType) -> Result<(), std::io::Error> {
    let reader_options = ReadOptionsBuilder::new().with_page_index().build();
    let file = File::open(filename)?;
    let reader = SerializedFileReader::new_with_options(file, reader_options)?;
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
