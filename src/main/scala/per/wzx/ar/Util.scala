package per.wzx.ar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Util {

  /**
   * 从文本中读取并转化为rdd形式的事务集
   *
   * @param sc   spark上下文
   * @param path 读取路径
   */
  def readAsRDD(sc: SparkContext, path: String): RDD[Array[String]] = {
    sc.textFile(path)
      .map(_.split(" "))
  }

  /**
   * 保存为文本形式，一个part
   *
   * @param path 输出路径
   * @param data 保存数据
   */
  def writeAsText(sc: SparkContext, data: RDD[(Array[String], Double)], path: String): Unit = {
    data.map { case (item, sup) =>
      // 支持度保留两位小数
      item.reduce(_ + " " + _) + "\t" + sup.formatted("%.2f")
    }.repartition(1).saveAsTextFile(path)
  }
}
