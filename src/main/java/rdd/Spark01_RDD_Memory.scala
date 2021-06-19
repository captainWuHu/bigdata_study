package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //sparkConf.set("spark.default.parallelism", "2")

    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //内存创建RDD，内存集合作为集合数据源
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    val rdd: RDD[Int] = sc.makeRDD(seq)

    //将多个区域存为文件
//    rdd.saveAsTextFile("output")
    rdd.collect().foreach(println)


    //文件中创建RDD,以行为单位
    val data: RDD[String] = sc.textFile("datas/1.txt",2)
    data.saveAsTextFile("output")
    data.collect().foreach(println)

    //以文件为单位产生元组（路径，文件内容）
    val data2: RDD[(String, String)] = sc.wholeTextFiles("datas")
    data2.collect().foreach(println)



    sc.stop()
  }

}
