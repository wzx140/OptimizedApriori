package per.wzx.experiment4

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.SortedSet
import scala.collection.{immutable, mutable}
import scala.util.control.Breaks

class Apriori(private val minSupport: Double) extends Serializable {

  /**
   * 初始化l1频繁项集，合并重复事务项，事务项元素映射为index
   *
   * @param sc           SparkContext
   * @param transactions 原始事务集
   * @param minCount     最小count
   * @return l1WithCnt:频繁1项集(indexed)和count; l1:频繁1项集，用作索引映射元素; item2Rank:事务集中的元素映射索引;
   *         transactionCnt:事务项计数表; indexedTransactions:事务数据库(indexed)
   */
  private def init(sc: SparkContext,
                   transactions: RDD[Array[String]],
                   minCount: Int
                  ): (Array[(Int, Int)], Array[String], Map[String, Int], Map[Int, Int],
    RDD[(Int, immutable.SortedSet[Int])]) = {
    // 频繁1项集和count
    val temp = transactions.flatMap(_.map((_, 1)))
      .reduceByKey(_ + _)
      .filter { case (_, count) => count >= minCount }
      // 根据count由大到小排列
      .sortBy(-_._2)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 频繁1项集，用作索引映射元素
    val l1 = temp.map(_._1).collect()
    // 事务集中的元素映射索引
    val item2Rank = l1.zipWithIndex.toMap
    // 频繁1项集(indexed)和count
    val l1WithCnt = temp.map { case (item, x) => (item2Rank(item), x) }.collect()
    temp.unpersist()

    val l1BC = sc.broadcast(l1)
    val tmp = transactions
      // 事务压缩，过滤掉不包含频繁1项集的事务项
      .filter(_.exists(l1BC.value.contains))
      // 过滤掉不包含频繁1项集的元素
      .map(_.filter(l1BC.value.contains))
      // 统计重复事务项
      .map(x => (x.toSet, 1)).reduceByKey(_ + _)
    // 事务项计数表
    val cntMap = tmp.map(_._2).zipWithIndex().map(x => (x._2.toInt, x._1)).collectAsMap().toMap
    val item2RankBC = sc.broadcast(item2Rank)
    // 事务数据库(索引)
    val indexedTransactions = tmp.map(_._1)
      .map(_.toArray.map(item2RankBC.value))
      .map(_.to[immutable.SortedSet])
      .zipWithIndex().map(x => (x._2.toInt, x._1))

    (l1WithCnt, l1, item2Rank, cntMap, indexedTransactions)
  }

  /**
   * 由频繁k-1项集生成k阶候选集，未并行化
   *
   * @param sc        SparkContext
   * @param freqItems 频繁k项集
   * @param k         产生候选集的阶数
   * @param minCount  最小支持度
   * @return 候选项集
   */
  private def genCandidates(sc: SparkContext,
                            freqItems: Array[immutable.SortedSet[Int]],
                            k: Int,
                            minCount: Int
                           ): Array[immutable.SortedSet[Int]] = {
    val candidates = collection.mutable.ListBuffer.empty[immutable.SortedSet[Int]]
    freqItems.indices.foreach { i =>
      Range(i + 1, freqItems.length).foreach { j =>
        if ((freqItems(i) & freqItems(j)).size == k - 2) {
          candidates.append(freqItems(i) | freqItems(j))
        }
      }
    }

    candidates.distinct.filter(x => freqItems.exists(_.subsetOf(x))).toArray
  }

  /**
   * 计算候选键每个元素的count
   *
   * @param sc           SparkContext
   * @param candidates   候选集
   * @param transactions 原始事务数据库(indexed)
   * @param k            候选集的阶数
   * @return candidatesWithCnt:候选集和事务数; newTransactions:压缩后的事务数据库; newCntMap:压缩后的事务计数表
   */
  private def calCount(sc: SparkContext,
                       candidates: Array[immutable.SortedSet[Int]],
                       transactions: RDD[(Int, immutable.SortedSet[Int])],
                       cntMap: Map[Int, Int],
                       k: Int
                      ): (Array[(immutable.SortedSet[Int], Int)], RDD[(Int, immutable.SortedSet[Int])], Map[Int, Int])
  = {

    val candidatesBC = sc.broadcast(candidates)
    val cntMapBC = sc.broadcast(cntMap)
    // 统计候选项的事务数，事务压缩
    val tmp = transactions.map { case (tranIndex, tranItem) =>
      val counts = mutable.ListBuffer.empty[Int]
      val candidates = candidatesBC.value
      val cntMap = cntMapBC.value
      // 统计候选集的事务数
      candidates.foreach { canItem =>
        if (canItem.subsetOf(tranItem)) counts.append(cntMap(tranIndex)) else counts.append(0)
      }
      // 该事务是否使用过
      val isUsed = counts.sum > 0

      (counts.toArray, (tranIndex, isUsed))
    }.persist(StorageLevel.MEMORY_AND_DISK)
    val countsAll = tmp.map(_._1).collect()
      .reduce { (l1, l2) => (l1, l2).zipped.map(_ + _) }
    val candidatesWithCnt = candidates.zip(countsAll)
    // 过滤未使用的事务项
    val usedMap = tmp.map(_._2).collectAsMap()
    val newCntMap = cntMap.filter(x => usedMap(x._1))
    val newTransactions = transactions.filter(x => usedMap(x._1))
    tmp.unpersist()


    (candidatesWithCnt, newTransactions, newCntMap)
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
         ): Array[(Array[String], Double)] = {

    transactions.persist(StorageLevel.MEMORY_AND_DISK)
    val totalCount = transactions.count()
    println("-----transaction : " + totalCount + "-----")
    val minCount = math.ceil(minSupport * totalCount).toInt

    // 频繁1项集
    var (l1WithCnt, l1, item2Rank, cntMap, indexedTransactions) = init(sc, transactions, minCount)
    println("-----L1          : " + l1.length + "-----")

    var k: Int = 2
    // 频繁k项集
    var lkWithCnt = l1WithCnt.map(x => (immutable.SortedSet(x._1), x._2))
    // 频繁项集集合
    var freqItemsWithCnt = mutable.ListBuffer.empty[(SortedSet[Int], Int)]
    freqItemsWithCnt ++= l1WithCnt.map(x => (immutable.SortedSet(x._1), x._2))

    val loop = new Breaks
    loop.breakable {
      while (!lkWithCnt.isEmpty) {
        println()
        // k阶候选集
        val candidates = genCandidates(sc, lkWithCnt.map(_._1), k, minCount)
        println("-----C" + k + "          : " + candidates.length + "-----")
        if (candidates.isEmpty) {
          loop.break()
        }

        // 事务压缩，计算count
        val (candidatesWithCnt, newTransactions, newCntMap) = calCount(sc, candidates, indexedTransactions, cntMap, k)
        indexedTransactions = newTransactions
        cntMap = newCntMap
        println("-----transaction : " + indexedTransactions.count() + "-----")
        // 支持度过滤
        lkWithCnt = candidatesWithCnt.filter { case (_, count) => count >= minCount }
        freqItemsWithCnt ++= lkWithCnt
        println("-----L" + k + "          : " + lkWithCnt.length + "-----")
        k += 1
      }
    }

    // 把索引转化为实际值，事务数计算支持度
    freqItemsWithCnt.map { case (indexes, count) =>
      val cases = indexes.toArray.sorted.reverse.map(l1)
      (cases, count.toDouble / totalCount)
    }.toArray
  }
}
