use parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData, ColumnChunkMetaData};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet::schema::printer::{self, print_file_metadata};
use parquet::schema::types::SchemaDescriptor;
use std::fs::File;
use std::io;

#[derive(Clone, Debug)]
pub(crate) enum InfoType {
    Parquet,
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
                    Ok(message) => writeln!(out, "Decoded arrow schema{:#?}", message),
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

#[allow(unused_must_use)]
fn print_column_chunk_metadata(out : &mut dyn io::Write, cc_metadata : &ColumnChunkMetaData) {
    writeln!(out, "column type: {}", cc_metadata.column_type());
    writeln!(out, "column path: {}", cc_metadata.column_path());
    let encoding_strs: Vec<_> = cc_metadata
        .encodings()
        .iter()
        .map(|e| format!("{}", e))
        .collect();
    writeln!(out, "encodings: {}", encoding_strs.join(" "));
    let file_path_str = cc_metadata.file_path().unwrap_or("N/A");
    writeln!(out, "file path: {}", file_path_str);
    writeln!(out, "file offset: {}", cc_metadata.file_offset());
    writeln!(out, "num of values: {}", cc_metadata.num_values());
    writeln!(
        out,
        "total compressed size (in bytes): {}",
        cc_metadata.compressed_size()
    );
    writeln!(
        out,
        "total uncompressed size (in bytes): {}",
        cc_metadata.uncompressed_size()
    );
    writeln!(out, "compression: {}", cc_metadata.compression());
    writeln!(out, "data page offset: {}", cc_metadata.data_page_offset());
    let index_page_offset_str = match cc_metadata.index_page_offset() {
        None => "N/A".to_owned(),
        Some(ipo) => ipo.to_string(),
    };
    writeln!(out, "index page offset: {}", index_page_offset_str);
    let dict_page_offset_str = match cc_metadata.dictionary_page_offset() {
        None => "N/A".to_owned(),
        Some(dpo) => dpo.to_string(),
    };
    writeln!(out, "dictionary page offset: {}", dict_page_offset_str);
    let statistics_str = match cc_metadata.statistics() {
        None => "N/A".to_owned(),
        Some(stats) => stats.to_string(),
    };
    writeln!(out, "statistics: {}", statistics_str);
    let bloom_filter_offset_str = match cc_metadata.bloom_filter_offset() {
        None => "N/A".to_owned(),
        Some(bfo) => bfo.to_string(),
    };
    writeln!(out, "bloom filter offset: {}", bloom_filter_offset_str);
    let offset_index_offset_str = match cc_metadata.offset_index_offset() {
        None => "N/A".to_owned(),
        Some(oio) => oio.to_string(),
    };
    writeln!(out, "offset index offset: {}", offset_index_offset_str);
    let offset_index_length_str = match cc_metadata.offset_index_length() {
        None => "N/A".to_owned(),
        Some(oil) => oil.to_string(),
    };
    writeln!(out, "offset index length: {}", offset_index_length_str);
    let column_index_offset_str = match cc_metadata.column_index_offset() {
        None => "N/A".to_owned(),
        Some(cio) => cio.to_string(),
    };
    writeln!(out, "column index offset: {}", column_index_offset_str);
    let column_index_length_str = match cc_metadata.column_index_length() {
        None => "N/A".to_owned(),
        Some(cil) => cil.to_string(),
    };
    writeln!(out, "column index length: {}", column_index_length_str);
    writeln!(out);
}

#[allow(unused_must_use)]
fn print_row_group_metadata(out: &mut dyn io::Write, rg_metadata: &RowGroupMetaData) {
    writeln!(out, "total byte size: {}", rg_metadata.total_byte_size());
    writeln!(out, "num of rows: {}", rg_metadata.num_rows());
    writeln!(out);
    writeln!(out, "num of columns: {}", rg_metadata.num_columns());
    writeln!(out, "columns: ");
    for (i, cc) in rg_metadata.columns().iter().enumerate() {
        writeln!(out);
        writeln!(out, "column {}:", i);
        print_column_chunk_metadata(out, cc);
    }
}

#[allow(unused_must_use)]
fn print_parquet_metadata(out: &mut dyn io::Write, metadata: &ParquetMetaData) {
    print_file_metadata(out, metadata.file_metadata());
    writeln!(out);
    writeln!(out, "number of row groups: {}", metadata.num_row_groups());
    writeln!(out);
    for (i, rg) in metadata.row_groups().iter().enumerate() {
        writeln!(out, "row group {}:", i);
        print_row_group_metadata(out, rg);
    }
}

#[allow(unused_must_use)]
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
        InfoType::Parquet => {
            println!("Metadata for  {}", filename);
            print_parquet_metadata(output, metadata);
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
