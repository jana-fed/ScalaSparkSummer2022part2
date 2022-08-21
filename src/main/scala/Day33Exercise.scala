import SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.RFormula

object Day33Exercise extends App {
val spark = getSpark("sparky")
  //TODO load into dataframe from retail-data by-day December 1st

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"
  val df = readDataWithView(spark, filePath)

  //TODO create RFormula to use Country as label and only UnitPrice and Quantity as Features
  //TODO make sure they are numeric columns - we do not want one hot encoding here
  //you can leave column names at default

  //create output dataframe with the the formula peforming fit and transform
  val formula = new RFormula()
    .setFormula("Country ~ UnitPrice + Quantity")
    .setFeaturesCol("MYfeatures") //default is features which is fine
    .setLabelCol("MYlabel") //default is label which is usually fine

  val output = formula.fit(df).transform(df)
  output
    //    .select("features", "label")
    .show()



  //TODO BONUS try creating features from ALL columns in the Dec1st CSV except of course Country (using . syntax)
  //This should generate very sparse column of features because of one hot encoding

  val sillyFormula = new RFormula()
    .setFormula("Country ~ .")
    .setFeaturesCol("MYfeatures") //default is features which is fine
    .setLabelCol("MYlabel") //default is label which is usually fine

  val outputAgain = sillyFormula.fit(df).transform(df)
  outputAgain

    .show()
}
