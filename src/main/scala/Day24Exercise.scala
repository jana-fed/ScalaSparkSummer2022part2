import SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, initcap, lit, lower, lpad, ltrim, regexp_replace, rpad, rtrim, trim, upper}

object Day24Exercise extends App {
  println("Day 24 Exercise")
  val spark = getSpark("Day 24")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  //TODO open up March 1st, of 2011 CSV

  val df = readCSVWithView(spark, filePath)


  //Select Capitalized Description Column
  df.select(col("Description"),
    initcap(col("Description"))).show(3, false)
  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  //ideally there would be even number of _______LATVIA__________ (30 total)

  df.select(
    col("Country"),
    lpad(rpad(col("Country"), 20, "_"), 25, "_").as("Padded country")
  ).show(5, false)

  //select Description column again with all occurences of metal or wood replaced with material
  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns


//  val materials = Seq("metal", "wood")
//  val regexString = materials.map(_.toUpperCase).mkString("|")

  df.where(col("Description").contains("METAL").or(col("Description").contains("WOOD"))).
    select(
    regexp_replace(col("Description"), "metal", "material").alias("metal"),
    regexp_replace(col("Description"),"wood", "material").alias("wood"),
    col("Description"))
    .show(10, false)



}
