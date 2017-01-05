import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import play.api.libs.json._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._
object main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Count").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    /* Old Uncleaned
    def makeTable(table_data: String, table_schema: String) {
      //Import Spark SQL data types---------------------------------------------------- 
      import org.apache.spark.sql.types.{ StructType, StructField, StringType };

      //val schemaString = scala.io.Source.fromFile(table_schema).getLines.mkString
      //println(schemaString)

      // Generate the schema based on the string of schema
      /*
 *  StructType(Array(
    StructField("year", IntegerType, true),
    StructField("make", StringType, true),
    StructField("model", StringType, true),
    StructField("comment", StringType, true),
    StructField("blank", StringType, true)))
 * 
 */

  //   val fields = schemaString.split(" ")
  //      .map(fieldName => StructField(fieldName, StringType, nullable = true))
  //   val customSchema = StructType(fields)
      
      val fields = feedJSONToList("DEPARTMENTS").map(fieldName => StructField(fieldName, StringType, nullable = true))
      val customSchema = StructType(fields)
      //----------------------------------------------------------------------------------

      //Read the data from HDFS by applying Schema
      val myDataFrame = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(customSchema)
        .load(table_data + ".csv")

      myDataFrame.createOrReplaceTempView(table_data) //will not work not Global
      ///df.createGlobalTempView(table_data)
     //registertemptable ??
      val sqlDF = sqlContext.sql("SELECT * FROM " + table_data)

      sqlDF.show()
    }
*/

    def makeTable(dataFile: String, schemaJSON: String, tableName: String) {
      import org.apache.spark.sql.types.{ StructType, StructField, StringType };
      //use tuples
      //val fields = feedJSONToList(schemaJSON).map(fieldName => StructField(fieldName, StringType, nullable = true))
      val fields = feedJSONToListTuple(schemaJSON).map { case (fieldName, dataType) => StructField(fieldName, dataTypeMapper(dataType), nullable = true) }
      fields.foreach(println)
      val customSchema = StructType(fields)
      //Read the data from HDFS by applying Schema
      val myDataFrame = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(customSchema)
        .load(dataFile)

      //myDataFrame.count  
      myDataFrame.printSchema()
      //myDataFrame.dtypes

      myDataFrame.createOrReplaceTempView(tableName)
      val sqlDF = sqlContext.sql("SELECT * FROM " + tableName)
      sqlDF.show()
    }
    //-----------------------------------------------------------------------------------

    def joinTable(joinSources: Seq[(String, String, String)]) {
      joinSources.foreach { case (dataFile, schemaJSON, tableName) => makeTable(dataFile, schemaJSON, tableName) }
      //  val groupCheck = sqlContext.sql("SELECT count(*) FROM employees GROUP By DEPARTMENT_ID")
      // groupCheck.show()
      //InnerJoinColumn(table_one, table_two, "DEPARTMENT_ID")
    }
    //------------------------------------------------------------------------------------

    def InnerJoinColumn(source_one: String, source_two: String, primaryKey: String) {
      val targetTable = sqlContext.sql("SELECT * FROM " + source_one + "," + source_two + " WHERE " + source_one + "." + primaryKey + " = " + source_two + "." + primaryKey)

      targetTable.show()
    }
    //----------------------------------------------------------------------------------------------

    //Call Make table - Individually
    joinTable(Array(("departments.csv", "DEPARTMENTS.json", "DEPARTMENTS"), ("employees.csv", "EMPLOYEES.json", "EMPLOYEES")))
    //makeTable("departments.csv", "DEPARTMENTS.json", "DEPARTMENTS")
    //makeTable("employees.csv", "EMPLOYEES.json", "EMPLOYEES")
    //joinTable("employees", "departments")
    //JSON Implementation---------------------------------------------------------------------------

    def feedJSONToList(filename: String): Seq[String] = {

      val source = scala.io.Source.fromFile(filename)
      //another approach is : source.getLines mkString "\n"
      val lines = try source.mkString finally source.close()
      val jsonFromFile: JsValue = Json.parse(lines)
      val namesArray = ((jsonFromFile \ "feed" \ "schema" \ "fields") \\ "name").map(_.as[String])
      val datatypeArray = ((jsonFromFile \ "feed" \ "schema" \ "fields") \\ "dataType").map(_.as[String])
      val path = jsonFromFile \ "feed" \ "dataSource" \ "path"

      namesArray.foreach(println)
      datatypeArray.foreach(println)
      val coupledArray = namesArray zip datatypeArray
      //fields.map(fieldName => StructField(fieldName, StringType, nullable = true))
      return namesArray
    }

    def feedJSONToListTuple(filename: String): Seq[(String, String)] = {

      val source = scala.io.Source.fromFile(filename)
      //another approach is : source.getLines mkString "\n"
      val lines = try source.mkString finally source.close()
      val jsonFromFile: JsValue = Json.parse(lines)
      val namesArray = ((jsonFromFile \ "feed" \ "schema" \ "fields") \\ "name").map(_.as[String])
      val datatypeArray = ((jsonFromFile \ "feed" \ "schema" \ "fields") \\ "dataType").map(_.as[String])
      val path = jsonFromFile \ "feed" \ "dataSource" \ "path"

      namesArray.foreach(println)
      datatypeArray.foreach(println)
      val coupledArray = namesArray zip datatypeArray
      //fields.map(fieldName => StructField(fieldName, StringType, nullable = true))
      return coupledArray
    }
    //---------------------------------------------------------------------------------------------
    //List to Schema RDD
    def listToSchemaRDD(namesArray: Seq[String]) {
      val fields = namesArray.map(fieldName => StructField(fieldName, StringType, nullable = true))
      val customSchema = StructType(fields)
    }
    //------------------------------------------------------------------------------------------------  
    //dataType Mapper
    def dataTypeMapper(dataType: String): DataType = dataType match {
      case "TIME"      => TimestampType
      case "TIMESTAMP" => TimestampType
      case "DOUBLE"    => FloatType
      case "CHAR"      => StringType
      case "VARCHAR"   => StringType
      case "DECIMAL"   => DecimalType(10, 10) // fix later DecimalType Doesnt work.
      case "INTEGER"   => IntegerType
      case "SMALLINT"  => ShortType
      case "TINYINT"   => ShortType
      case "DATE"      => DateType
      case _           => StringType
    }
    //---------------------------------------------------------------------------------------------------
    def queriesExtract() {
    
    
    
    }
    //---------------------------------------------------------------------------------------------------

  }
}