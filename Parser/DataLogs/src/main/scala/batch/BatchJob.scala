package batch

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import domain._
import util._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object BatchJob {

  def main(args: Array[String]): Unit = {

    //setting logger
    Logger.getLogger("org").setLevel(Level.ERROR)

    //initializing spark context
    val sc = SparkUtils.getSparkContext("Parser")
    val spark = SparkUtils.getSparkSession()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //getting data
    val input = sc.textFile("G:\\sparkResources\\parser\\data.tsv")
    val inputRDD = input.flatMap(data => {
                                val record = data.split("\\t")
                                val ms_in_hour = 60*60*1000
                                if(record.length == 7)
                                  Some(Activity(record(0).toLong/ms_in_hour*ms_in_hour,record(1),record(2),record(3),record(4),record(5),record(6)))
                                else
                                  None
                            })

    val inputDF = inputRDD.toDF()
    val df = inputDF.select(add_months(from_unixtime($"timestamp_hour"/1000),1).as("timestamp_hour"),$"referrer",$"action",
                                          $"prevPage",$"page",$"visitor",$"product").cache()

    val keyedByProductHour = inputRDD.keyBy(data => (data.product,data.timestamp_hour)).cache()

    val visitorByProduct = keyedByProductHour
                          .mapValues(data => data.visitor)
                          .distinct()
                          .countByKey()

    val activityByProduct = keyedByProductHour
                            .mapValues(data => {
                                        val activity = data.action
                                        activity match {
                                          case "purchase" => (1,0,0)
                                          case "add_to_cart" => (0,1,0)
                                          case "page_view" => (0,0,1)
                                        }
                            })
                            .reduceByKey((a,b) => (a._1+b._1 , a._2+b._2 , a._3+b._3 ))

    val ds : Dataset[Logs] = df.as[Logs].cache()
    val visitorByProductDS = ds.select($"product",$"timestamp_hour",$"visitor").distinct()
                                .groupBy($"product",$"timestamp_hour").agg(expr("count(visitor) as unique_visitors").as[Int])

    val activityByProductDS = ds.select($"product",$"timestamp_hour",$"action")
                                .withColumn("purchase",when($"action"==="purchase",1).otherwise(0))
                                .withColumn("add_to_cart",when($"action"==="add_to_cart",1).otherwise(0))
                                .withColumn("page_view",when($"action"==="page_view",1).otherwise(0))
                                .groupBy($"product",$"timestamp_hour").agg(sum($"purchase").as("purchase"),
                                sum($"add_to_cart").as("add_to_cart"),
                                sum($"page_view").as("page_view"))

    //visitorByProduct.foreach(println)
   // activityByProduct.foreach(println)

    //visitorByProductDS.show(false)
    //activityByProductDS.show(false)

    activityByProductDS.rdd.map(_.toString().replace("[","").replace("]","")).coalesce(1).saveAsTextFile("G:\\sparkResources\\parser\\activityProducts.tsv")
  }
}
