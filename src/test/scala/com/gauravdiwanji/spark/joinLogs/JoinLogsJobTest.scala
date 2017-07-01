package test.scala.com.gauravdiwanji.spark.joinLogs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import main.scala.com.gauravdiwanji.spark.joinLogs.JoinLogsJob
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class JoinLogsJobTest extends FunSuite with BeforeAndAfter{
  var sc: SparkContext = _
  
  before {
    sc = new SparkContext(new SparkConf().setAppName("joinLogsTest").setMaster("local"))
  }

  after {
    sc.stop()
  }
  
test("Check if the output is correct") {
    val job = new JoinLogsJob(sc)
    val assetLoc = getClass.getResource("/assetsRaw").getPath
    val adLoc = getClass.getResource("/adRaw").getPath
    val result = job.run("file://"+assetLoc, "file://"+adLoc)
    assert(result.collect()(0)(0) == "adcef402-9b67-4de7-be94-17a857898e3b")
    assert(result.collect()(0)(1) == 3)
    assert(result.collect()(0)(2) == 1)
    assert(result.collect()(0)(3) == 1)
    assert(result.collect()(1)(0) == "7963ee21-0d09-4924-b315-ced4adad425f")
    assert(result.collect()(1)(1) == 3)
    assert(result.collect()(1)(2) == 3)
    assert(result.collect()(1)(3) == 0)
  }
    
}
