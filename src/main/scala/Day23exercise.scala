import org.apache.spark.sql.functions.{col, desc, lit}

object Day23exercise extends App {
  val spark = SparkUtil.getSpark("BasicSpark")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  //TODO is load 1st of March of 2011 into dataFrame

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //we let Spark determine schema
    .load(filePath)
  //TODO get all purchases that have been made from Finland

  df.where(col("Country").equalTo("Finland"))
    .show(5, false)


  //TODO sort by Unit Price and LIMIT 20
  val sortedPurchases = df.where(col("Country").equalTo("Finland"))
    .orderBy(desc("UnitPrice")).limit(20)
    .collect()
  sortedPurchases.foreach(println)
  //TODO collect results into an Array of Rows
  //print these 20 Rows


}
