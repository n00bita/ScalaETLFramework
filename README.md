# ScalaETLFramework
A Web based ETL Framework developed in scala,play and Javascript.


makeFile() ->Takes 3 Input 
dataFile : Absoulute path to where data is stored in HDFS
schemaJSON : Path to JSON Schema File in NoSQL DB or local file system.
tableName : The name of the View in SparkSQL to write query on.

feedJSONtoListTuple() -> Takes a JSON file as string .return tuple of 2 Strings:(fieldName,dataType)

dataTypeMapper() -> Converts "Hive specified" datatypes to SparkSQl data types to perform DataFrame Operations or SQl style
queries in Spark SQL.


