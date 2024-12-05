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
Metadata for  /tmp/test.parquet
+-------------+-------------------------------------------+
| version     | 2                                         |
|-------------+-------------------------------------------|
| num of rows | 50985                                     |
|-------------+-------------------------------------------|
| created by  | parquet-cpp-arrow version 18.0.0-SNAPSHOT |
+-------------+-------------------------------------------+
metadata:
  pandas: {"index_columns": [{"kind": "range", "name": null, "start": 0, "stop": 50985, "step": 1}],
  "column_indexes": [{"name": null, "field_name": null, "pandas_type": "unicode", "numpy_type": "object",
  "metadata": {"encoding": "UTF-8"}}], "columns": [{"name": "Year", "field_name": "Year", "pandas_type":
  "int64", "numpy_type": "int64", "metadata": null}, {"name": "Industry_aggregation_NZSIOC", "field_name":
  "Industry_aggregation_NZSIOC", "pandas_type": "unicode", "numpy_type": "object", "metadata": null},
  {"name": "Industry_code_NZSIOC", "field_name": "Industry_code_NZSIOC", "pandas_type": "unicode",
  "numpy_type": "object", "metadata": null}, {"name": "Industry_name_NZSIOC", "field_name":
  "Industry_name_NZSIOC", "pandas_type": "unicode", "numpy_type": "object", "metadata": null}, {"name":
  "Units", "field_name": "Units", "pandas_type": "unicode", "numpy_type": "object", "metadata": null},
  {"name": "Variable_code", "field_name": "Variable_code", "pandas_type": "unicode", "numpy_type":
  "object", "metadata": null}, {"name": "Variable_name", "field_name": "Variable_name", "pandas_type":
  "unicode", "numpy_type": "object", "metadata": null}, {"name": "Variable_category", "field_name":
  "Variable_category", "pandas_type": "unicode", "numpy_type": "object", "metadata": null}, {"name":
  "Value", "field_name": "Value", "pandas_type": "unicode", "numpy_type": "object", "metadata": null},
  {"name": "Industry_code_ANZSIC06", "field_name": "Industry_code_ANZSIC06", "pandas_type": "unicode",
  "numpy_type": "object", "metadata": null}], "creator": {"library": "pyarrow", "version": "18.0.0"},
  "pandas_version": "2.2.3"}

message schema {
  OPTIONAL INT64 Year;
  OPTIONAL BYTE_ARRAY Industry_aggregation_NZSIOC (STRING);
  OPTIONAL BYTE_ARRAY Industry_code_NZSIOC (STRING);
  OPTIONAL BYTE_ARRAY Industry_name_NZSIOC (STRING);
  OPTIONAL BYTE_ARRAY Units (STRING);
  OPTIONAL BYTE_ARRAY Variable_code (STRING);
  OPTIONAL BYTE_ARRAY Variable_name (STRING);
  OPTIONAL BYTE_ARRAY Variable_category (STRING);
  OPTIONAL BYTE_ARRAY Value (STRING);
  OPTIONAL BYTE_ARRAY Industry_code_ANZSIC06 (STRING);
}

number of row groups: 1

+----------------------+--------+
| row group 0          |        |
+===============================+
| total byte size: {}  | 330896 |
|----------------------+--------|
| compressed byte size | 179687 |
|----------------------+--------|
| num of rows          | 50985  |
|----------------------+--------|
| num of columns       | 10     |
+----------------------+--------+

columns:

+------------------------------------+--------------------------+
| column 0                           |                          |
+===============================================================+
| column type                        | INT64                    |
|------------------------------------+--------------------------|
| column path                        | "Year"                   |
|------------------------------------+--------------------------|
| encodings                          | PLAIN RLE RLE_DICTIONARY |
|------------------------------------+--------------------------|
| file path                          | N/A                      |
|------------------------------------+--------------------------|
| file offset                        | 0                        |
|------------------------------------+--------------------------|
| num of values                      | 50985                    |
|------------------------------------+--------------------------|
| total compressed size (in bytes)   | 178                      |
|------------------------------------+--------------------------|
| total uncompressed size (in bytes) | 208                      |
|------------------------------------+--------------------------|
| compression                        | SNAPPY                   |
|------------------------------------+--------------------------|
| data page offset                   | 75                       |
|------------------------------------+--------------------------|
| index page offset                  | N/A                      |
|------------------------------------+--------------------------|
| dictionary page offset             | 4                        |
|------------------------------------+--------------------------|
| bloom filter offset                | N/A                      |
|------------------------------------+--------------------------|
| offset index offset                | N/A                      |
|------------------------------------+--------------------------|
| offset index length                | N/A                      |
|------------------------------------+--------------------------|
| column index offset                | N/A                      |
|------------------------------------+--------------------------|
| column index length                | N/A                      |
|------------------------------------+--------------------------|
| number of values in chunk          | 50985                    |
|------------------------------------+--------------------------|
| distinct count                     | N/A                      |
|------------------------------------+--------------------------|
| null count                         | 0                        |
|------------------------------------+--------------------------|
| physical type                      | INT64                    |
|------------------------------------+--------------------------|
| min-max is backwards compatible    | false                    |
|------------------------------------+--------------------------|
| min-max is deprecated              | false                    |
|------------------------------------+--------------------------|
| min is exact                       | true                     |
|------------------------------------+--------------------------|
| max is exact                       | true                     |
+------------------------------------+--------------------------+

+------------------------------------+-------------------------------+
| column 1                           |                               |
+====================================================================+
| column type                        | BYTE_ARRAY                    |
|------------------------------------+-------------------------------|
| column path                        | "Industry_aggregation_NZSIOC" |
|------------------------------------+-------------------------------|
| encodings                          | PLAIN RLE RLE_DICTIONARY      |
|------------------------------------+-------------------------------|
| file path                          | N/A                           |
|------------------------------------+-------------------------------|
| file offset                        | 0                             |
|------------------------------------+-------------------------------|
| num of values                      | 50985                         |
|------------------------------------+-------------------------------|
| total compressed size (in bytes)   | 343                           |
|------------------------------------+-------------------------------|
| total uncompressed size (in bytes) | 2420                          |
|------------------------------------+-------------------------------|
| compression                        | SNAPPY                        |
|------------------------------------+-------------------------------|
| data page offset                   | 224                           |
|------------------------------------+-------------------------------|
| index page offset                  | N/A                           |
|------------------------------------+-------------------------------|
| dictionary page offset             | 182                           |
|------------------------------------+-------------------------------|
| bloom filter offset                | N/A                           |
|------------------------------------+-------------------------------|
| offset index offset                | N/A                           |
|------------------------------------+-------------------------------|
| offset index length                | N/A                           |
|------------------------------------+-------------------------------|
| column index offset                | N/A                           |
|------------------------------------+-------------------------------|
| column index length                | N/A                           |
|------------------------------------+-------------------------------|
| number of values in chunk          | 50985                         |
|------------------------------------+-------------------------------|
| distinct count                     | N/A                           |
|------------------------------------+-------------------------------|
| null count                         | 0                             |
|------------------------------------+-------------------------------|
| physical type                      | BYTE_ARRAY                    |
|------------------------------------+-------------------------------|
| min-max is backwards compatible    | false                         |
|------------------------------------+-------------------------------|
| min-max is deprecated              | false                         |
|------------------------------------+-------------------------------|
| min is exact                       | false                         |
|------------------------------------+-------------------------------|
| max is exact                       | false                         |
+------------------------------------+-------------------------------+
...

```
