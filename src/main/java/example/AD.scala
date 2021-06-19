package example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AD {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ADD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("datas/2.txt")

    //时间戳，省份，城市，用户，广告
    val p_a: RDD[((String, String), Int)] = rdd.map(
      line => {
        val clickINF: Array[String] = line.split(" ")
        ((clickINF(1), clickINF(4)), 1)
      }
    )

    val p_aCount: RDD[((String, String), Int)] = p_a.reduceByKey(_ + _)


    val a_c: RDD[(String, (String, Int))] = p_aCount.map {
      case ((p, a), c) => (p, (a, c))
    }
    val a_cGroup: RDD[(String, Iterable[(String, Int)])] = a_c.groupByKey()

    val res: RDD[(String, List[(String, Int)])] = a_cGroup.mapValues(
      _.toList.sortWith(_._2 > _._2).take(3)
    )
    p_aCount.foreach(println)

  }
}
