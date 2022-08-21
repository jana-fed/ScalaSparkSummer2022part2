import SparkUtil.getSpark
import org.apache.spark.ml.feature.{StandardScaler, Tokenizer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, expr, size}

import scala.io.Source.fromURL

object Day33Exercise2 extends App {


  println("Day 33 exercise")
  val spark = getSpark("Sparky")


  val url = "https://www.gutenberg.org/files/11/11-0.txt"
  val dst = "src/resources/text/Alice.txt"
  Util.getTextFromWebAndSave(url, dst)

  //TODO create a DataFrame with a single column called text which contains above book line by line
  val df = spark.read.textFile(dst)
    .toDF("text")

  //TODO create new column called words with will contain Tokenized words of text column

  val tkn = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("tokenized")


  //TODO create column called textLen which will be a character count in text column //https://spark.apache.org/docs/2.3.0/api/sql/index.html#length can use or also length function from spark
  //TODO create column wordCount which will be a count of words in words column //can use count or length - words column will be Array type
  val df2 = tkn.transform(df.select("text"))
    .withColumn("textLen", expr("CHAR_LENGTH(text)"))
    .withColumn("wordCount", size(col("tokenized")))

  df2.show()


  //TODO create Vector Assembler which will transform textLen and wordCount into a column called features //features column will have a Vector with two of those values
  val va = new VectorAssembler()
    .setInputCols(Array("textLen", "wordCount"))
    .setOutputCol("features")
  val dfAssembled = va.transform(df2)
  //TODO create StandardScaler which will take features column and output column called scaledFeatures //it should be using mean and variance (so both true)
  val scalerWithMean = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(true )

  val finalDF = scalerWithMean.fit(dfAssembled).transform(dfAssembled)
  //TODO create a dataframe with all these columns - save to alice.csv file with all columns

  finalDF.show

  val bookCSV = finalDF
    .withColumn("tokenized", col("tokenized").cast("string"))
    .withColumn("Features", col("features").cast("string"))
    .withColumn("ScaledFeatures", col("scaledFeatures").cast("string"))
    .select("text", "tokenized", "textLen", "wordCount", "Features", "ScaledFeatures")

  bookCSV.write
    .format("csv")
    .mode("overwrite")
    .option("header", true)
//    .option("sep", "\t")
    .save("src/resources/book/alice.csv")



}
