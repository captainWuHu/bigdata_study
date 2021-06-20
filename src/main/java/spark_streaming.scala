import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object spark_streaming {
  def main(args: Array[String]): Unit = {
    val streamConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("stream")
    val spark: SparkSession = SparkSession.builder().config(streamConf).getOrCreate()
    import spark.implicits._
    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(4, 2, 5, 1))
    val df: DataFrame = rdd.toDF()
    df.show()
  }
}
