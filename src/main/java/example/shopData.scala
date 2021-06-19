package example

import java.util.concurrent.atomic.LongAccumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object shopData {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("request")
    val sc = new SparkContext(sparkConf)

    val p = new ProductAccumulator
    sc.register(p, "qewe")


    //1. 读取原始日志文件
    //2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
    //    日期_用户ID_SessionID_页面ID_动作时间_搜索关键字_点击品类ID（6）_点击产品ID_?_下单品类ID_下单产品ID_支付品类ID_支付产品ID_城市
    val requestRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    requestRDD.cache()

    val preRDD: RDD[(String, (Int, Int, Int), String)] = requestRDD.flatMap(
      action => {
        val word: Array[String] = action.split("_")
        if (word(6) != "-1") {
          List((word(6), (1, 0, 0), word(2)))
        }
        else if (word(8) != "null") {
          word(8).split(",").map((_, (0, 1, 0), word(2)))
        }
        else if (word(10) != "null") {
          word(10).split(",").map((_, (0, 0, 1), word(2)))
        }
        else Nil
      }
    )
    preRDD.foreach(
      word => {
        p.add(word)
      }
    )
    val reverse: List[(String, ((Int, Int, Int), List[String]))] = p.value.toList.sortBy(_._2._1).takeRight(10).reverse
    val ReRDD: RDD[(String, ((Int, Int, Int), List[String]))] = sc.makeRDD(reverse)
    val res: RDD[(String, List[(String, Int)])] = ReRDD.map(
      value => {
        (value._1, value._2._2.map(
          session => {
            (session, 1)
          }
        ))
      }
    )
    val ress: RDD[(String, (String, Int))] = res.mapValues(
      session => {
        session.reduce(
          (s1, s2) =>{
            (s1._1, s1._2 + s2._2)
          }
        )
      }
    )
    print(ress.collect().mkString("Array(", ", ", ")"))






//    //2. 统计品类的点击数量 （品类ID,点击数量）
//    val requestRDDFilter: RDD[String] = requestRDD.filter(
//      action => {
//        action.split("_")(6) != "-1"
//      }
//    )
//    val clickCategory: RDD[(String, Int)] = requestRDDFilter.map(
//      line => {
//        val word: Array[String] = line.split("_")
//        (word(6), 1)
//      }
//    )
//
//    val allClickCount: RDD[(String, Int)] = clickCategory.reduceByKey(_ + _)
//
//
//    println("品类的点击数量:")
//    allClickCount.foreach(println)
//
//
//    //3. 统计品类的下单数量 （品类ID,下单数量）
//    val orderRDDFilter: RDD[String] = requestRDD.filter(
//      action => {
//        action.split("_")(8) != "null"
//      }
//    )
//    val orderRDD: RDD[String] = orderRDDFilter.flatMap(
//      line => {
//        line.split("_")(8).split(",")
//      }
//    )
//    val orderGroup: RDD[(String, Iterable[String])] = orderRDD.groupBy(word => word)
//    val allOrder: RDD[(String, Int)] = orderGroup.map(
//      order => {
//        (order._1, order._2.size)
//      }
//    )
//    println("品类的下单数量:")
//    allOrder.collect().toList.foreach(println)
//
//
//    //4. 统计品类的支付数量 （品类ID，支付数量）
//
//    val payRDDFilter: RDD[String] = requestRDD.filter(
//      action => {
//        action.split("_")(10) != "null"
//      }
//    )
//    val payMapRDD: RDD[Array[String]] = payRDDFilter.map(
//      action => {
//        action.split("_")(10).split(",")
//      }
//    )
//
//    val allPay: RDD[(String, Int)] = payMapRDD.flatMap(
//      word => word.map((_, 1))
//    ).reduceByKey(_ + _)
//
//
//    println("品类的支付数量:")
//    allPay.foreach(println)
//
//
//    //5. 品类排序取前十名
//    print("品类前十")
//
//
//    allClickCount.foreach(
//      count => {
//        p.add(count._1, (count._2, 0, 0))
//      }
//    )
//
//    allOrder.foreach(
//      count => {
//
//        p.add(count._1, (0, count._2, 0))
//      }
//    )
//
//    allPay.foreach(
//      count => {
//        p.add(count._1, (0, 0, count._2))
//      }
//    )
//    val res: List[(String, (Int, Int, Int))] = p.productID.toList.sortBy(_._2).takeRight(10)
//    print(res)

    sc.stop()
  }


  class ProductAccumulator extends AccumulatorV2[(String, (Int, Int, Int), String),
                                            mutable.Map[String, ((Int, Int, Int), List[String])]] {
    var productID: mutable.Map[String, ((Int, Int, Int), List[String])] = mutable.Map[String, ((Int, Int, Int), List[String])]()

    override def isZero: Boolean = productID.isEmpty

    override def copy(): ProductAccumulator = {
      val re = new ProductAccumulator()
      re.productID = productID.clone()
      re
    }

    override def reset(): Unit = productID.clear()

    def add(v: (String, (Int, Int, Int), String)): Unit = {
      if (productID.contains(v._1)) {
        val t3: ((Int, Int, Int), List[String]) = productID(v._1)
        productID.put(v._1, ((t3._1._1 + v._2._1, t3._1._2 + v._2._2, t3._1._3 + v._2._3), t3._2.:+(v._3)))

      }
      else productID.put(v._1, (v._2, List()))
    }

    override def merge(other: AccumulatorV2[(String, (Int, Int, Int), String),
                          mutable.Map[String, ((Int, Int, Int), List[String])]]): Unit = {
      other.value.foreach(
        kv => {
           if (productID.contains(kv._1)){
             val t3: ((Int, Int, Int), List[String]) = productID(kv._1)
             productID.put(kv._1, ((t3._1._1+kv._2._1._1,t3._1._2 + kv._2._1._2, t3._1._3 + kv._2._1._3), t3._2:::kv._2._2))
           }
          else{
             productID.put(kv._1,kv._2)
           }
        }
      )
    }

    override def value: mutable.Map[String, ((Int, Int, Int), List[String])] = productID
  }


}


case class UserVisitAction(
                            date: String, //用户点击行为日期
                            user_id: Long, //用户ID
                            session_id: String, //sessionID
                            page_id: Long, //某页面ID
                            action_time: String, //动作时间点
                            search_keyword: String, //搜索关键字
                            click_category_id: Long, //某分类ID
                            click_product_id: Long, //某商品ID
                            order_category_ids: String, //一次订单中所有品类的ID集合
                            order_product_ids: String, //一次订单中所有商品的ID集合
                            pay_category_ids: String, //一次支付中所有品类的ID集合
                            pay_product_ids: String, //一次支付中所有商品的ID集合
                            city_id: Long //城市ID
                          )