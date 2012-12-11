package spark

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import com.google.common.io.Files

import scala.collection.mutable.ArrayBuffer

import SparkContext._
import storage.StorageLevel

object OOMTest extends org.scalatest.Tag("oom")

class DistributedSuite extends FunSuite with ShouldMatchers with BeforeAndAfter {

  val clusterUrl = "local-cluster[2,1,512]"
  val singleNodeClusterUrl = "local-cluster[1,1,512]"

  @transient var sc: SparkContext = _

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    System.clearProperty("spark.reducer.maxMbInFlight")
    System.clearProperty("spark.storage.memoryFraction")
    System.clearProperty("spark.minFreeSlaveMemory")
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  test("local-cluster format") {
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
    sc.stop()
    System.clearProperty("spark.master.port")
    sc = new SparkContext("local-cluster[2 , 1 , 512]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
    sc.stop()
    System.clearProperty("spark.master.port")
    sc = new SparkContext("local-cluster[2, 1, 512]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
    sc.stop()
    System.clearProperty("spark.master.port")
    sc = new SparkContext("local-cluster[ 2, 1, 512 ]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
    sc.stop()
    System.clearProperty("spark.master.port")
    sc = null
  }

  test("simple groupByKey") {
    sc = new SparkContext(clusterUrl, "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 5)
    val groups = pairs.groupByKey(5).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey where map output sizes exceed maxMbInFlight") {
    System.setProperty("spark.reducer.maxMbInFlight", "1")
    sc = new SparkContext(clusterUrl, "test")
    // This data should be around 20 MB, so even with 4 mappers and 2 reducers, each map output
    // file should be about 2.5 MB
    val pairs = sc.parallelize(1 to 2000, 4).map(x => (x % 16, new Array[Byte](10000)))
    val groups = pairs.groupByKey(2).map(x => (x._1, x._2.size)).collect()
    assert(groups.length === 16)
    assert(groups.map(_._2).sum === 2000)
    // Note that spark.reducer.maxMbInFlight will be cleared in the test suite's after{} block
  }

  test("accumulators") {
    sc = new SparkContext(clusterUrl, "test")
    val accum = sc.accumulator(0)
    sc.parallelize(1 to 10, 10).foreach(x => accum += x)
    assert(accum.value === 55)
  }

  test("broadcast variables") {
    sc = new SparkContext(clusterUrl, "test")
    val array = new Array[Int](100)
    val bv = sc.broadcast(array)
    array(2) = 3     // Change the array -- this should not be seen on workers
    val rdd = sc.parallelize(1 to 10, 10)
    val sum = rdd.map(x => bv.value.sum).reduce(_ + _)
    assert(sum === 0)
  }

  test("repeatedly failing task") {
    sc = new SparkContext(clusterUrl, "test")
    val accum = sc.accumulator(0)
    val thrown = intercept[SparkException] {
      sc.parallelize(1 to 10, 10).foreach(x => println(x / 0))
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("more than 4 times"))
  }

  test("caching") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).cache()
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching on disk") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.DISK_ONLY)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_ONLY_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory, serialized, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_ONLY_SER_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching on disk, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.DISK_ONLY_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory and disk, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_AND_DISK_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory and disk, serialized, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("compute without caching when no partitions fit in memory") {
    System.setProperty("spark.storage.memoryFraction", "0.0001")
    sc = new SparkContext(clusterUrl, "test")
    // data will be 4 million * 4 bytes = 16 MB in size, but our memoryFraction set the cache
    // to only 50 KB (0.0001 of 512 MB), so no partitions should fit in memory
    val data = sc.parallelize(1 to 4000000, 2).persist(StorageLevel.MEMORY_ONLY_SER)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
    System.clearProperty("spark.storage.memoryFraction")
  }

  test("compute when only some partitions fit in memory") {
    System.setProperty("spark.storage.memoryFraction", "0.01")
    sc = new SparkContext(clusterUrl, "test")
    // data will be 4 million * 4 bytes = 16 MB in size, but our memoryFraction set the cache
    // to only 5 MB (0.01 of 512 MB), so not all of it will fit in memory; we use 20 partitions
    // to make sure that *some* of them do fit though
    val data = sc.parallelize(1 to 4000000, 20).persist(StorageLevel.MEMORY_ONLY_SER)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
    System.clearProperty("spark.storage.memoryFraction")
  }

  test("passing environment variables to cluster") {
    sc = new SparkContext(clusterUrl, "test", null, Nil, Map("TEST_VAR" -> "TEST_VALUE"))
    val values = sc.parallelize(1 to 2, 2).map(x => System.getenv("TEST_VAR")).collect()
    assert(values.toSeq === Seq("TEST_VALUE", "TEST_VALUE"))
  }

  test("recover from fake OOM", OOMTest) {
    import spark.storage.{GetMemoryStatus, BlockManagerId, StorageLevel}
    import DistributedSuite.{markNodeIdentity, failOnMarkedIdentity}
    System.setProperty("spark.distributedSuite.generation", "pre-fail")
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 2, 2)
    val singleton = sc.parallelize(1 to 1, 1)
    assert(data.count === 2) // force executors to start
    val masterId = SparkEnv.get.blockManager.blockManagerId
    val hosts = SparkEnv.get.blockManager.master.getMemoryStatus.map(_._1).toSeq
    assert(hosts.size === 2)
    assert(data.flatMap(markNodeIdentity).count === 2)
    assert(data.flatMap(failOnMarkedIdentity).count === 2)
    val hostsAfterFailure = SparkEnv.get.blockManager.master.getMemoryStatus.map(_._1).toSeq
    assert(hostsAfterFailure.size === 2)
    // Test whether the block managers still work by storing a block with replication
    singleton.map(x => SparkEnv.get.blockManager.put("test_block", ArrayBuffer[Any](1, 2), StorageLevel.MEMORY_ONLY_2)).count
    val result = SparkEnv.get.blockManager.get("test_block")
    assert(result.isDefined)
    assert(result.get.toSeq === Seq(1, 2))

    // Make sure we can still run an RDD
    val newData = sc.parallelize(1 to 2, 2)
    assert(newData.count === 2)
  }

  test("recover from OOM by fixing memoryFraction", OOMTest) {
    System.setProperty("spark.storage.memoryFraction", "0.9")
    val OVERFLOW_SIZE = 1024 * 1024 * 300
    sc = new SparkContext(singleNodeClusterUrl, "test")
    val data = sc.parallelize(1 to 1, 1)
    val hugeData = data.map(ignored => Array.fill[Byte](OVERFLOW_SIZE)(42))
    assert(hugeData.cache.count === 1)
    // If we don't adjust memoryFraction, this will fail.
    val secondData = data.map(ignored => Array.fill[Byte](OVERFLOW_SIZE)(42))
    assert(secondData.cache.count === 1)
  }
}

object DistributedSuite {
  import spark.storage.BlockManagerId
  var mark = false
  def markNodeIdentity(item: Int): Option[Int] = {
    mark = true
    Some(item)
  }
  def failOnMarkedIdentity(item: Int): Option[Int] = {
    if (mark) {
      val t = new Thread { override def run() { throw new OutOfMemoryError("simulated OOM") } }
      t.start
      t.join
      return None
    } else {
      return Some(item)
    }
  }
}

