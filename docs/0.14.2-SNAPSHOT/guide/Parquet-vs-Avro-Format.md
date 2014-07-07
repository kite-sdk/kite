---
layout: page
---
## Parquet versus Avro Format

Avro is a row-based storage format for Hadoop.

Parquet is a column-based storage format for Hadoop.

If your use case typically scans or retrieves all of the fields in a row in each query, Avro is usually the best choice.

If your dataset has many columns, and your use case typically involves working with a subset of those columns rather than entire records, Parquet is optimized for that kind of work.
