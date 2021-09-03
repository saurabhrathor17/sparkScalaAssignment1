import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestSolution3 extends AnyFunSuite{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("testing solution3")
    .getOrCreate()

  val productByUserRow = Seq (
    (3,45,101),
    (2,25,103),
    (1,30,101),
    (1,30,101)
  )

  //select("UserID", "Price", "Product_ID")
  import spark.implicits._
  val productByUserDF: DataFrame = productByUserRow.toDF("UserID","Price","Product_ID")
  val outputDF: DataFrame = Service.totalSpent(productByUserDF)

  // test case
  val totalusers: Long = outputDF.count()
  totalusers should be (3)



}
