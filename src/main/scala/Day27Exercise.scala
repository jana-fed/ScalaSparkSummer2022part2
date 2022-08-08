import Day27UserDefinedFunctions.spark
import SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, udf}

object Day27Exercise extends App {
  println("Ch6: UDFs - User Defined Functions")
  val spark = getSpark("Sparky")


  //TODO create a UDF which converts Fahrenheit to Celsius

  def celsius(fahrenheit:Int) =( 5 *(fahrenheit - 32.0)) / 9.0
  //TODO Create DF with column temperatureF with temperatures from -40 to 120 using range or something else if want

  val dfTemp = spark.range(-40,120).toDF("temperatureF")
  dfTemp.show()
  //TODO register your UDF function

  val celsiusUDF = udf(celsius(_:Int):Double)
  //TODO use your UDF to create temperatureC column with the actual conversion
  dfTemp.withColumn("temperatureC",celsiusUDF(col("temperatureF"))).show(5)
  //TODO show both columns starting with F temperature at 90 and ending at 110( both included)
  val dfTable = dfTemp.withColumn("temperatureC",celsiusUDF(col("temperatureF"))).where(col("temperatureF") >= 90 && col("temperatureF") <= 110)
  dfTable.show()
  //You probably want Double incoming and Double also as a return
}
