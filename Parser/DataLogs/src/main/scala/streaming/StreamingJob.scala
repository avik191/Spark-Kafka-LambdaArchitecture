package streaming

import domain.{Activity, ActivityLog, Logs, VisitorsByProduct}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.streaming._
import _root_.util._
import com.twitter.algebird.HyperLogLogMonoid
import functions._
import config.Settings
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamingJob {

  def main(args: Array[String]): Unit = {

    //setting logger
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = SparkUtils.getSparkContext("StreamingClass")
    val batchDuration = Seconds(4)

    val spark = SparkUtils.getSparkSession()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    def streamingApp(sc : SparkContext,batchDuration : Duration) = {
      val ssc = new StreamingContext(sc,batchDuration)
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopic

      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )
      //val textDStream = ssc.textFileStream("file:///G:/sparkResources/parser/input")

      val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
        ssc,kafkaDirectParams,Set(topic)
      )

      val activityStream = kafkaStream.transform(input => {
        functions.rddToRDDActivity(input)
      }).cache()


      // save data to local
      activityStream.foreachRDD{ rdd =>

        val activityDF = rdd
          .toDF()
          .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor",
            "product","inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition",
            "inputProps.fromOffset as fromOffset","inputProps.untilOffset as untilOffset")
          // activityDF.show()
            val time = System.currentTimeMillis()
            val path = "G:\\sparkResources\\parser\\kafkaDetails\\"+time
            activityDF.rdd.map(_.toString().replace("[","").replace("]","")).coalesce(1)
                      .saveAsTextFile(path)
//          .write
//          .partitionBy("topic", "kafkaPartition", "timestamp_hour")
//          .mode(SaveMode.Append)
//          .parquet("G:\\sparkResources\\parser\\kafkaDetails")
      }

      // activity by product
      val activityStateSpec = StateSpec.function(activityStateSpecFunction).timeout(Minutes(120))

      val statefulActivityByProduct =  activityStream.transform(rdd => {

        val ds : Dataset[Logs] = rdd.toDF().as[Logs].cache()

        val activityByProductDS = ds.select($"product",$"timestamp_hour",$"action")
          .withColumn("purchase",when($"action"==="purchase",1).otherwise(0))
          .withColumn("add_to_cart",when($"action"==="add_to_cart",1).otherwise(0))
          .withColumn("page_view",when($"action"==="page_view",1).otherwise(0))
          .groupBy($"product",$"timestamp_hour").agg(sum($"purchase").as("purchase"),
          sum($"add_to_cart").as("add_to_cart"),
          sum($"page_view").as("page_view"))


        activityByProductDS.map(r => ((r.getString(0),r.getLong(1)),
          ActivityLog(r.getString(0),r.getLong(1),r.getLong(2),r.getLong(3),r.getLong(4)))).rdd
      }).mapWithState(activityStateSpec)

      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4),
          filterFunc = (record) => false
        )
        .foreachRDD(rdd => rdd.map(sr => ActivityLog(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
          .toDF().registerTempTable("ActivityByProduct"))


      // unique visitors by product
      val visitorStateSpec =
        StateSpec
          .function(mapVisitorsStateFunc)
          .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulVisitorsByProduct = activityStream.map( a => {
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      } ).mapWithState(visitorStateSpec)

      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        ) // only save or expose the snapshot every x seconds
        .foreachRDD(rdd => rdd.map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
        .toDF().registerTempTable("VisitorsByProduct"))


      ssc
    }

    val ssc = SparkUtils.getStreamingContext(streamingApp,sc,batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }

}
