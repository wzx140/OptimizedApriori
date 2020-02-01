package per.wzx.ar

import java.io.File
import java.nio.file.Paths

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable
import scala.io.Source

@Test
class MainTest {

  /**
   * 基地址
   */
  private val basePath = this.getClass.getResource("/").getPath
  private val sc: SparkContext = new SparkContext(new SparkConf().setAppName("test_Apriori").setMaster("local[2]"))

  @Before
  def setUp(): Unit = {
    // 删除output目录
    val output = new File(basePath + "output")
    if (output.exists) {
      val childFilePath = output.list
      assert(childFilePath != null)
      for (path <- childFilePath) {
        new File(Paths.get(output.getPath, path).toString).delete
      }
    }
    output.delete
  }

  @Test
  def testMain(): Unit = {
    Main.test(sc, basePath + "input", basePath + "output", 0.4d)

    // 读取执行结果
    val result = new mutable.HashSet[String]
    val output = new File(basePath + "output")
    Assert.assertTrue("未产生output文件夹", output.exists)
    val childFilePath = output.list
    Assert.assertNotEquals("未找到执行结果", 0, childFilePath.length)
    for (path <- childFilePath) { // 读取part文件内容
      if (path.equals("part-00000")) {
        val source = Source.fromFile(Paths.get(output.getPath, path).toString)
        source.getLines().foreach(result.add)
        source.close()
      }
    }

    // 读取执行结果
    val expected = new mutable.HashSet[String]
    val source = Source.fromFile(basePath + "res.txt")
    source.getLines().foreach(expected.add)
    source.close()
    assertEquals(expected, result)
  }
}


