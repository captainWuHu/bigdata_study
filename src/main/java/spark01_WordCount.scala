import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_WordCount {
  def main(args: Array[String]): Unit = {

    //连接spark
    //jdbc connection
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    //执行业务操作
    val lines: RDD[String] = sc.textFile("datas")


    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordMap: RDD[(String, Int)] = words.map((_, 1))

//    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordMap.groupBy(word => word._1)

    //TODO Scala
//    val wordCount: RDD[(String, Int)] = wordGroup.map {
//      case (word, list) => {
//        list.reduce(
//          (t1, t2)=>{
//            (t1._1, t1._2 + t2._2)
//        })
//      }
//    }

    //TODO spark

    //reduceByKey   相同Key对Value聚合

    val wordCount: RDD[(String, Int)] = wordMap.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordCount.collect()

    //关闭连接

    sc.stop()
  }
}
