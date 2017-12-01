# hive_metadata_utils
Find Hive Tables by Table or Column Names.

These utilities use Spark SQL to interact with the Hive metastore and scala regex pattern matching to find relevant tables. 

## Usage
findTablesByName(".*airport.*") // this would find all tables with "airport" anywhere in the name.

findTablesByColumnName(".*passenger.*") // this would find all tables with "passenger" anywhere in any of the column names.
