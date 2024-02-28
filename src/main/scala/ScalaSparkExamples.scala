import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ScalaSparkExamples {
  def main(args: Array[String]): Unit =
  {
    val spark = SparkSession.builder().appName("HelloSpark").master("local[*]").getOrCreate()
    import spark.implicits._
    val df=spark.read.json("/Users/akiran/IdeaProjects/HelloSpark/src/main/resources/people.json")
    println("Hello")
//select some columns, rename some columns and delete some columns. dropped existing column named useless and even dropped a non-existing column without thowing an error
//new columns added through the select method as well as using the withColumn method
    df.select($"age", $"name".as("person_name"), upper($"name").as("uppercase_name")).drop("useless").withColumn("new_column_with_constant_value", lit("new_val")).drop("pqr").show()
    df.selectExpr("age").show()
    spark.stop()
  }
}