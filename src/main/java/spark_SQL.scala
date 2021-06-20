import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object spark_SQL {
  case class user(username: String,age: Long)


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas\\user.json")

    import spark.implicits._

    df.createOrReplaceTempView("user")

    val res: DataFrame = spark.sql("select username,age from user")

    res.show()




    spark.close()

  }
}
