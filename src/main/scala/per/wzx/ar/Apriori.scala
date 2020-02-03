package per.wzx.ar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.control.Breaks

class Apriori(private val minSupport: Double) extends Serializable {

  /**
   * 由频繁k-1项集生成k阶候选集
   *
   * @param sc        SparkContext
   * @param freqItems 频繁k项集
   * @param k         阶数
   * @param minCount  最小支持度
   * @return 候选项集
   */
  private def genCandidates(sc: SparkContext,
                            freqItems: RDD[Set[Int]],
                            k: Int,
                            minCount: Int
                           ): RDD[Set[Int]] = {
    val kItem = freqItems.collect()
    val candidates = collection.mutable.ListBuffer.empty[Set[Int]]
    kItem.indices.foreach { i =>
      Range(i + 1, kItem.length).foreach { j =>
        if ((kItem(i) & kItem(j)).size == k - 2) {
          candidates.append(kItem(i) | kItem(j))
        }
      }
    }

    // 剪枝，频繁项集的子集也是频繁项集
    sc.parallelize(candidates.distinct.filter(x => kItem.exists(_.subsetOf(x))))
  }

  /**
   * 计算候选键每个元素的支持度计数
   *
   * @param sc         SparkContext
   * @param candidates 候选集
   * @param boolMatrix 布尔矩阵
   * @return candidatesWithCnt:候选集和事务数; newTransactions:压缩后的事务数据库; newCntMap:压缩后的事务计数表
   */
  private def calCount(sc: SparkContext,
                       candidates: RDD[Set[Int]],
                       boolMatrix: Array[Array[Boolean]],
                       cntMap: Array[Int]
                      ): (RDD[(Set[Int], Int)], Array[Array[Boolean]], Array[Int]) = {

    val boolMatrixBC = sc.broadcast(boolMatrix)
    val cntMapBC = sc.broadcast(cntMap)
    // 统计候选项对应事务的布尔向量
    val tmp = candidates.map { item =>
      val boolMatrix = boolMatrixBC.value
      val boolVector = item.map(x => boolMatrix.map(_ (x)))
        // 布尔向量间做位与
        .reduce(_.zip(_).map(x => x._1 & x._2))

      (item, boolVector)
    }.cache()

    // 支持度计数
    val candidatesWithCnt = tmp.map { case (item, vector) =>
      val cntMap = cntMapBC.value
      val count = vector.zip(cntMap).map { case (flag, count) =>
        if (flag) count else 0
      }.sum

      (item, count)
    }
    // 事务的使用表
    val usedMap = tmp.map(_._2)
      // 布尔向量间做位或
      .reduce(_.zip(_).map(x => x._1 | x._2))
    // 事务压缩
    val newBoolMatrix = boolMatrix.zip(usedMap).filter(_._2).map(_._1)
    val newCntMap = cntMap.zip(usedMap).filter(_._2).map(_._1)

    (candidatesWithCnt, newBoolMatrix, newCntMap)
  }

  /**
   * 迭代计算
   *
   * @param sc           SparkContext
   * @param transactions 事务数据库
   * @return 所有频繁项集
   */
  def run(sc: SparkContext,
          transactions: RDD[Array[String]]
         ): RDD[(Array[String], Double)] = {

    transactions.persist(StorageLevel.MEMORY_AND_DISK)
    val totalCount = transactions.count()
    println("-----transaction : " + totalCount + "-----")
    val minCount = math.ceil(minSupport * totalCount).toInt

    // 初始化事务集和生成频繁1项集
    val temp = transactions.flatMap(_.map((_, 1)))
      .reduceByKey(_ + _)
      .filter { case (_, count) => count >= minCount }
      // 根据支持度计数由大到小排列
      .sortBy(-_._2)
      .cache()

    // 频繁1项集，用作索引映射元素值
    val l1 = temp.map(_._1).collect()
    // 事务集中的元素值映射索引
    val item2Rank = l1.zipWithIndex.toMap
    // 频繁1项集(indexed)和支持度计数
    val l1WithCnt = temp.map { case (item, x) => (item2Rank(item), x) }

    val l1BC = sc.broadcast(l1)
    val tmp = transactions
      // 事务压缩，过滤掉不包含频繁1项集的事务项
      .filter(_.exists(l1BC.value.contains))
      // 过滤掉不包含频繁1项集的元素
      .map(_.filter(l1BC.value.contains))
      // 统计重复事务项
      .map(x => (x.toSet, 1)).reduceByKey(_ + _)
    transactions.unpersist()
    // 事务计数表
    var cntMap = tmp.map(_._2).collect()
    val item2RankBC = sc.broadcast(item2Rank)
    // 构造布尔矩阵
    var boolMatrix = tmp.map(_._1)
      .map { transaction =>
        val item2Rank = item2RankBC.value
        val boolRow = new Array[Boolean](l1.length)
        transaction.foreach { x =>
          boolRow(item2Rank(x)) = true
        }

        boolRow
      }.collect()
    tmp.unpersist()
    println("-----L1          : " + l1.length + "-----")

    var k: Int = 2
    // 频繁k项集
    var lkWithCnt = l1WithCnt.map(x => (Set(x._1), x._2))
      .persist(StorageLevel.MEMORY_AND_DISK)
    // 频繁项集集合
    var freqItemsWithCnt = sc.emptyRDD[(Set[Int], Int)]
    freqItemsWithCnt = freqItemsWithCnt.union(lkWithCnt)

    val loop = new Breaks
    loop.breakable {
      while (!lkWithCnt.isEmpty) {
        println()
        // k阶候选集
        val candidates = genCandidates(sc, lkWithCnt.map(_._1), k, minCount)
          .persist(StorageLevel.MEMORY_AND_DISK)
        println("-----C" + k + "          : " + candidates.count() + "-----")
        if (candidates.isEmpty) {
          loop.break()
        }

        // 事务压缩，计算支持度计数
        val (candidatesWithCnt, newBoolMatrix, newCntMap) = calCount(sc, candidates, boolMatrix, cntMap)
        boolMatrix = newBoolMatrix
        cntMap = newCntMap
        println("-----transaction : " + boolMatrix.length + "-----")
        // 支持度过滤
        lkWithCnt = candidatesWithCnt.filter { case (_, count) => count >= minCount }
          .persist(StorageLevel.MEMORY_AND_DISK)
        freqItemsWithCnt = freqItemsWithCnt.union(lkWithCnt)
        println("-----L" + k + "          : " + lkWithCnt.count() + "-----")
        candidates.unpersist()
        k += 1
      }
    }
    // 把索引转化为元素值，支持度数计算支持度
    freqItemsWithCnt.map { case (indexes, count) =>
      val cases = indexes.toArray.sorted.reverse.map(l1BC.value)
      (cases, count.toDouble / totalCount)
    }
  }
}
