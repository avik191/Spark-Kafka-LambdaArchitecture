package util

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

  def getSparkContext(appName : String) = {

    var checkPointDirectory = "file:///G:/sparkResources/parser/temp"
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts","true")

    // check if running from IDE or not
    if(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")){
      System.setProperty("hadoop.home.dir","G:\\sparkResources\\winutls")
      conf.setMaster("local[*]")
    }

    //initializing spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkPointDirectory)
    sc
  }

  def getSparkSession() = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark
  }

  def getStreamingContext(streamingApp :  (SparkContext,Duration) => StreamingContext, sc : SparkContext, batchDuration : Duration) = {

    val createStreamingContext = () => streamingApp(sc,batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir,createStreamingContext,sc.hadoopConfiguration,true)
      case None => StreamingContext.getActiveOrCreate(createStreamingContext)
    }

    sc.getCheckpointDir.foreach(dir => ssc.checkpoint(dir))
    ssc
  }

}
