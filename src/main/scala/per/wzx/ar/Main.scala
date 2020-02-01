package per.wzx.ar

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: <in> <out> <minSupport>")
      System.exit(2)
    }
    val input = args(0)
    val output = args(1)
    val minSupport = args(2).toDouble

    val conf = new SparkConf().setAppName("Apriori")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val time = System.currentTimeMillis()
    val transactions = Util.readAsRDD(sc, input)

    val apriori = new Apriori(minSupport)
    val res = apriori.run(sc, transactions)

    Util.writeAsText(sc, res, output)
    println("-----time        : " + (System.currentTimeMillis() - time) + "ms-----")
  }

  /**
   * for test
   */
  def test(sc: SparkContext, input: String, output: String, minSupport: Double): Unit = {
    sc.setLogLevel("WARN")
    val transactions = Util.readAsRDD(sc, input)
    val apriori = new Apriori(minSupport)
    val res = apriori.run(sc, transactions)
    Util.writeAsText(sc, res, output)
  }

}