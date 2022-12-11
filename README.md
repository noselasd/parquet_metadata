# parquet_metadata

Command line tool to inspect and show metadata of parquet files.
- Shows parquet schema
- Shows Arrow schema
- Shows parquet metadata
- Shows column group metadata

# Build
Install a rust toolchain and run

    cargo build


# Example Output


```

$ parquet_metadata /tmp/test.parquet

Metadata for  /tmp/test.parquet
version: 1
num of rows: 41715
created by: fastparquet-python version 2022.12.0 (build 0)
metadata:
  pandas: {"column_indexes": [{"field_name": null, "metadata": null, "name": null, "numpy_type": "object", "pandas_type": "mixed-integer"}], "columns": [{"field_name": "Year", "metadata": null, "name": "Year", "numpy_type": "int64", "pandas_type": "int64"}, {"field_name": "Industry_aggregation_NZSIOC", "metadata": null, "name": "Industry_aggregation_NZSIOC", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Industry_code_NZSIOC", "metadata": null, "name": "Industry_code_NZSIOC", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Industry_name_NZSIOC", "metadata": null, "name": "Industry_name_NZSIOC", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Units", "metadata": null, "name": "Units", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Variable_code", "metadata": null, "name": "Variable_code", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Variable_name", "metadata": null, "name": "Variable_name", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Variable_category", "metadata": null, "name": "Variable_category", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Value", "metadata": null, "name": "Value", "numpy_type": "object", "pandas_type": "unicode"}, {"field_name": "Industry_code_ANZSIC06", "metadata": null, "name": "Industry_code_ANZSIC06", "numpy_type": "object", "pandas_type": "unicode"}], "creator": {"library": "fastparquet", "version": "2022.12.0"}, "index_columns": [{"kind": "range", "name": null, "start": 0, "step": 1, "stop": 41715}], "pandas_version": "1.5.2", "partition_columns": []}
message schema {
  OPTIONAL INT64 Year;
  OPTIONAL BYTE_ARRAY Industry_aggregation_NZSIOC (UTF8);
  OPTIONAL BYTE_ARRAY Industry_code_NZSIOC (UTF8);
  OPTIONAL BYTE_ARRAY Industry_name_NZSIOC (UTF8);
  OPTIONAL BYTE_ARRAY Units (UTF8);
  OPTIONAL BYTE_ARRAY Variable_code (UTF8);
  OPTIONAL BYTE_ARRAY Variable_name (UTF8);
  OPTIONAL BYTE_ARRAY Variable_category (UTF8);
  OPTIONAL BYTE_ARRAY Value (UTF8);
  OPTIONAL BYTE_ARRAY Industry_code_ANZSIC06 (UTF8);
}

number of row groups: 1

row group 0:
total byte size: 7738135
num of rows: 41715

num of columns: 10
columns:

column 0:
column type: INT64
column path: "Year"
encodings: PLAIN
file path: N/A
file offset: 15782
num of values: 41715
total compressed size (in bytes): 15778
total uncompressed size (in bytes): 333759
compression: SNAPPY
data page offset: 4
index page offset: N/A
dictionary page offset: N/A
statistics: {min: 2013, max: 2021, distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 1:
column type: BYTE_ARRAY
column path: "Industry_aggregation_NZSIOC"
encodings: PLAIN
file path: N/A
file offset: 39420
num of values: 41715
total compressed size (in bytes): 23638
total uncompressed size (in bytes): 458904
compression: SNAPPY
data page offset: 15782
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [76, 101, 118, 101, 108, 32, 49], max: [76, 101, 118, 101, 108, 32, 52], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 2:
column type: BYTE_ARRAY
column path: "Industry_code_NZSIOC"
encodings: PLAIN
file path: N/A
file offset: 61361
num of values: 41715
total compressed size (in bytes): 21941
total uncompressed size (in bytes): 347367
compression: SNAPPY
data page offset: 39420
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [57, 57, 57, 57, 57], max: [90, 90, 49, 49], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 3:
column type: BYTE_ARRAY
column path: "Industry_name_NZSIOC"
encodings: PLAIN
file path: N/A
file offset: 162786
num of values: 41715
total compressed size (in bytes): 101425
total uncompressed size (in bytes): 1613380
compression: SNAPPY
data page offset: 61361
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [65, 99, 99, 111, 109, 109, 111, 100, 97, 116, 105, 111, 110], max: [87, 111, 111, 100, 32, 80, 114, 111, 100, 117, 99, 116, 32, 77, 97, 110, 117, 102, 97, 99, 116, 117, 114, 105, 110, 103], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 4:
column type: BYTE_ARRAY
column path: "Units"
encodings: PLAIN
file path: N/A
file offset: 212011
num of values: 41715
total compressed size (in bytes): 49225
total uncompressed size (in bytes): 839559
compression: SNAPPY
data page offset: 162786
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [68, 111, 108, 108, 97, 114, 115], max: [80, 101, 114, 99, 101, 110, 116, 97, 103, 101], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 5:
column type: BYTE_ARRAY
column path: "Variable_code"
encodings: PLAIN
file path: N/A
file offset: 227145
num of values: 41715
total compressed size (in bytes): 15134
total uncompressed size (in bytes): 292044
compression: SNAPPY
data page offset: 212011
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [72, 48, 49], max: [72, 52, 49], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 6:
column type: BYTE_ARRAY
column path: "Variable_name"
encodings: PLAIN
file path: N/A
file offset: 290318
num of values: 41715
total compressed size (in bytes): 63173
total uncompressed size (in bytes): 1085287
compression: SNAPPY
data page offset: 227145
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [65, 100, 100, 105, 116, 105, 111, 110, 115, 32, 116, 111, 32, 102, 105, 120, 101, 100, 32, 97, 115, 115, 101, 116, 115], max: [84, 111, 116, 97, 108, 32, 105, 110, 99, 111, 109, 101, 32, 112, 101, 114, 32, 101, 109, 112, 108, 111, 121, 101, 101, 32, 99, 111, 117, 110, 116], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 7:
column type: BYTE_ARRAY
column path: "Variable_category"
encodings: PLAIN
file path: N/A
file offset: 344515
num of values: 41715
total compressed size (in bytes): 54197
total uncompressed size (in bytes): 962490
compression: SNAPPY
data page offset: 290318
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [70, 105, 110, 97, 110, 99, 105, 97, 108, 32, 112, 101, 114, 102, 111, 114, 109, 97, 110, 99, 101], max: [70, 105, 110, 97, 110, 99, 105, 97, 108, 32, 114, 97, 116, 105, 111, 115], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 8:
column type: BYTE_ARRAY
column path: "Value"
encodings: PLAIN
file path: N/A
file offset: 494932
num of values: 41715
total compressed size (in bytes): 150417
total uncompressed size (in bytes): 313717
compression: SNAPPY
data page offset: 344515
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [45, 49], max: [83], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A


column 9:
column type: BYTE_ARRAY
column path: "Industry_code_ANZSIC06"
encodings: PLAIN
file path: N/A
file offset: 582961
num of values: 41715
total compressed size (in bytes): 88029
total uncompressed size (in bytes): 1491628
compression: SNAPPY
data page offset: 494932
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [65, 78, 90, 83, 73, 67, 48, 54, 32, 71, 114, 111, 117, 112, 32, 70, 51, 56, 48], max: [65, 78, 90, 83, 73, 67, 48, 54, 32, 103, 114, 111, 117, 112, 115, 32, 83, 57, 53, 49, 44, 32, 83, 57, 53, 50, 44, 32, 97, 110, 100, 32, 83, 57, 53, 51], distinct_count: N/A, null_count: 0, min_max_deprecated: true}
bloom filter offset: N/A
offset index offset: N/A
offset index length: N/A
column index offset: N/A
column index length: N/A

```
