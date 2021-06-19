import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, LongAccumulator}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object spark01_RDD_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_transform")
    val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("datas/2.txt",3)
    //    val request: RDD[String] = rdd.filter(
    //      line => {
    //        val time: String = line.split(" ")(3)
    ////        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    ////        val date: Date = sdf.parse(time)
    ////        val YMD: String = new SimpleDateFormat("dd/MM/yyyy").format(date)
    //        time.startsWith("17/05/2015")
    //      }
    //    )
    class myPartitioner extends Partitioner {
      override def numPartitions: Int = 4

      override def getPartition(key: Any): Int = {
        key match {
          case "a" => 0
          case "b" => 1
          case "c" => 2
          case "d" => 3
        }
      }
    }

    def mapAccumulator[K, V]:MapAccumulator[K,V] ={
      val acc = new MapAccumulator[K, V]
      sc.register(acc, "mapAcc")
      acc
    }

    class MapAccumulator[K, V] extends AccumulatorV2[K, mutable.Map[K, V]] {
      var map: mutable.Map[K, V] = mutable.Map[K, V]()

      override def isZero: Boolean = map.isEmpty

      override def copy(): AccumulatorV2[K, mutable.Map[K, V]] = {
        val acc = new MapAccumulator[K, V]
        acc.map = map.clone()
        acc
      }

      override def reset(): Unit = map.clear()

      override def add(v: K): Unit = Unit

      def add(v: K, opt: mutable.Map[K,V] =>Unit): Unit = {
        opt(map)
      }

      override def merge(other: AccumulatorV2[K, mutable.Map[K, V]]): Unit = {
        map ++= other.value
      }

      override def value: mutable.Map[K, V] = map


    }


    val mapAcc: MapAccumulator[String, Long] = mapAccumulator[String, Long]
    val wordCount: RDD[String] = rdd.map(
      line => {
        val words: Array[String] = line.split(" ")
        words.foreach(
          word => {
            mapAcc.add(word, map => map.put(word, map.getOrElse(word, 0L) + 1L))
          }
        )
        line
      }
    )
    wordCount.collect()

    print(mapAcc.value)



    sc.stop()
  }
}
