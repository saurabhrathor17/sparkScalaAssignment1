import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{be, contain}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestSolution2 extends AnyFunSuite{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("testing solution2")
    .getOrCreate()

  val productByUserRow = Seq (
    (101,1),
    (103,2),
    (101,2),
    (101,4)
  )

  import spark.implicits._
  val productByUserDF: DataFrame = productByUserRow.toDF("Product_ID","UserID")
  val outputDF: DataFrame = Service.boughtProduct(productByUserDF)

  // test cases
  val totalRowsUser: Long = outputDF.count()
  totalRowsUser should be (3)

  outputDF.collect() should contain allElementsOf Array(Row(1,1),Row(4,1),Row(2,2)) //

}
