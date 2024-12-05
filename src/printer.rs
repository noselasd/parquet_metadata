use base64::prelude::*;
use comfy_table::Table;
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet::schema::printer::{self, print_schema};
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

fn add_row<T: ToString>(table: &mut Table, name: &str, val: T) {
    table.add_row(vec![name, &val.to_string()]);
}
fn add_row_opt<T: ToString>(table: &mut Table, name: &str, val: Option<T>) {
    let v = match val {
        Some(x) => x.to_string(),
        None => "N/A".to_owned(),
    };
    table.add_row(vec![name, &v]);
}

#[allow(unused_must_use)]
fn print_arrow_schema(out: &mut dyn io::Write, metadata: &FileMetaData) {
    if let Some(arrow_schema) = find_arrow_schema(metadata) {
        let decoded = BASE64_STANDARD.decode(arrow_schema);
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
fn print_column_chunk_metadata(
    out: &mut dyn io::Write,
    cc_metadata: &ColumnChunkMetaData,
    col_no: usize,
) {
    let mut table = Table::new();
    table.set_header(vec![format!("column {}", col_no)]);
    add_row(&mut table, "column type", cc_metadata.column_type());

    add_row(&mut table, "column path", cc_metadata.column_path());
    let encoding_strs: Vec<_> = cc_metadata
        .encodings()
        .iter()
        .map(|e| format!("{}", e))
        .collect();
    add_row(&mut table, "encodings", encoding_strs.join(" "));
    add_row_opt(&mut table, "file path", cc_metadata.file_path());
    add_row(&mut table, "file offset", cc_metadata.file_offset());
    add_row(&mut table, "num of values", cc_metadata.num_values());
    add_row(
        &mut table,
        "total compressed size (in bytes)",
        cc_metadata.compressed_size(),
    );
    add_row(
        &mut table,
        "total uncompressed size (in bytes)",
        cc_metadata.uncompressed_size(),
    );
    add_row(&mut table, "compression", cc_metadata.compression());
    add_row(
        &mut table,
        "data page offset",
        cc_metadata.data_page_offset(),
    );
    add_row_opt(
        &mut table,
        "index page offset",
        cc_metadata.index_page_offset(),
    );
    add_row_opt(
        &mut table,
        "dictionary page offset",
        cc_metadata.dictionary_page_offset(),
    );
    add_row_opt(
        &mut table,
        "bloom filter offset",
        cc_metadata.bloom_filter_offset(),
    );
    add_row_opt(
        &mut table,
        "offset index offset",
        cc_metadata.offset_index_offset(),
    );
    add_row_opt(
        &mut table,
        "offset index length",
        cc_metadata.offset_index_length(),
    );
    add_row_opt(
        &mut table,
        "column index offset",
        cc_metadata.column_index_offset(),
    );
    add_row_opt(
        &mut table,
        "column index length",
        cc_metadata.column_index_length(),
    );
    add_row(
        &mut table,
        "number of values in chunk",
        cc_metadata.num_values(),
    );
    if let Some(stats) = cc_metadata.statistics() {
        add_row_opt(&mut table, "distinct count", stats.distinct_count_opt());
        add_row_opt(&mut table, "null count", stats.null_count_opt());
        add_row(&mut table, "physical type", stats.physical_type());
        add_row(
            &mut table,
            "min-max is backwards compatible",
            stats.is_min_max_backwards_compatible(),
        );

        add_row(
            &mut table,
            "min-max is deprecated",
            stats.is_min_max_deprecated(),
        );
        add_row(&mut table, "min is exact", stats.min_is_exact());
        add_row(&mut table, "max is exact", stats.max_is_exact());
    }

    writeln!(out, "{table}");
    writeln!(out);
}

#[allow(unused_must_use)]
fn print_row_group_metadata(
    out: &mut dyn io::Write,
    rg_metadata: &RowGroupMetaData,
    group_num: usize,
) {
    let mut table = Table::new();
    table.set_header(vec![format!("row group {}", group_num)]);

    add_row(&mut table, "total byte size", rg_metadata.total_byte_size());
    add_row(
        &mut table,
        "compressed byte size",
        rg_metadata.compressed_size(),
    );
    add_row(&mut table, "num of rows", rg_metadata.num_rows());
    add_row(&mut table, "num of columns", rg_metadata.num_columns());

    writeln!(out, "{table}\n\ncolumns:");
    for (i, cc) in rg_metadata.columns().iter().enumerate() {
        writeln!(out);
        print_column_chunk_metadata(out, cc, i);
    }
}

#[allow(unused_must_use)]
fn print_parquet_metadata(out: &mut dyn io::Write, metadata: &ParquetMetaData) {
    print_file_metadata(out, metadata.file_metadata());
    writeln!(out);
    writeln!(out, "number of row groups: {}", metadata.num_row_groups());
    for (i, rg) in metadata.row_groups().iter().enumerate() {
        writeln!(out);
        print_row_group_metadata(out, rg, i);
    }
}

#[allow(unused_must_use)]
pub fn print_file_metadata(out: &mut dyn io::Write, file_metadata: &FileMetaData) {
    let mut table = Table::new();
    add_row(&mut table, "version", file_metadata.version());
    add_row(&mut table, "num of rows", file_metadata.num_rows());
    add_row_opt(&mut table, "created by", file_metadata.created_by());

    writeln!(out, "{table}");
    if let Some(metadata) = file_metadata.key_value_metadata() {
        writeln!(out, "metadata:");
        for kv in metadata.iter() {
            writeln!(
                out,
                "  {}: {}",
                &kv.key,
                kv.value.as_ref().unwrap_or(&"".to_owned())
            );
        }
    }
    let schema = file_metadata.schema();
    print_schema(out, schema);
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
