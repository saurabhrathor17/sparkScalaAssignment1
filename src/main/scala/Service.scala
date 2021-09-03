import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Service {
  def fileReader(path: String)(implicit spark: SparkSession): DataFrame = {
    val a = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path = path)
    a
  }

  def getjoinedDF(transDF: DataFrame, userDF: DataFrame, joinCol: String): DataFrame = {
    val joinedDF = transDF.join(userDF, Seq(joinCol))
    joinedDF
  }

  def uniqueLocation(joinedDF: DataFrame): Dataset[Row] = {
    joinedDF.select("Location").distinct()
  }

  def boughtProduct(transDF: DataFrame): DataFrame = {
    transDF.select("Product_ID", "UserID")
      .groupBy("UserID")
      .count()
      .withColumnRenamed("count", "totalProduct")
  }

  def totalSpent(transDF: DataFrame): DataFrame = {

    transDF.select("UserID", "Price", "Product_ID")
      .groupBy("UserID", "Product_ID")
      .agg(sum("Price")).withColumnRenamed("sum(Price)", "totalSpent")
  }
}
