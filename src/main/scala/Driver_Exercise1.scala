import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Driver_Exercise1 extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val sc = new SparkSession
  .Builder()
    .appName("solution1")
    .master("local[*]")
    .getOrCreate()
  val transDF = Service.fileReader("src/main/resources/transactions.csv")

  val userDF = Service.fileReader("src/main/resources/users.csv")

  val joinedDF = Service.getjoinedDF(transDF,userDF,"UserID")


  // solution - 1
val resultUniqueLocations = Service.uniqueLocation(joinedDF)
  resultUniqueLocations.show(truncate = false)

  // solution - 2
  val resultBoughtProducts = Service.boughtProduct(transDF)
  resultBoughtProducts.show()

  // solution - 3

  val resultTotalSpent = Service.totalSpent(transDF)
  resultTotalSpent.show()

}
