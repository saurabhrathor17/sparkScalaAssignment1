import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{be, contain}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class TestSolution1 extends AnyFunSuite {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc: SparkSession = new SparkSession
  .Builder()
    .appName("solution1")
    .master("local[*]")
    .getOrCreate()

  val locationRow = List("Bhopal"
    , "Bhopal"
    , "Bhopal"
    , "Jabalpur")
  import sc.implicits._
  val LocationsDF: DataFrame = locationRow.toDF("Location")
  val outputDF: Dataset[Row] = Service.uniqueLocation(LocationsDF)

  assert(outputDF.count()===2)
  assert(outputDF.collect() === Array(Row("Jabalpur"),Row("Bhopal")))

outputDF.collect() should contain allElementsOf Array(Row("Jabalpur"),Row("Bhopal"))

val bhopalCount: Long = outputDF.filter(col("Location")==="Bhopal").count()
  bhopalCount should be (1)
}


