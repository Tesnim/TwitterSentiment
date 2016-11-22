package org.pa.stream
import org.apache.spark._
//import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming._
import org.apache.spark.sql.SQLContext
// Import Spark SQL
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.fs._

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object SparkTwitter {
  def main(args: Array[String]) {
System.setProperty("hadoop.home.dir", "c:\\winutil\\")

 val sparkConf = new SparkConf().setMaster("local[4]").setAppName("TopHashtags")
 
 val sc=new SparkContext(sparkConf) // An existing SparkContext.
 val ssc = new StreamingContext(sc, Seconds(3))


    // Use the streaming context and the TwitterUtils to create the
    // Twitter stream.
    
    // terminate spark context
 val filters = args.takeRight(args.length - 4)
 val stream = TwitterUtils.createStream(ssc, None,filters)

    // Each tweet comes as a twitter4j.Status object, which we can use to
    // extract hash tags. We use flatMap() since each status could have
    // ZERO OR MORE hashtags.
 
 val statuses = stream.map(status => status.getText())
    statuses.print()
 statuses.foreachRDD(rdd => {
  // statuses.print()
   rdd.saveAsTextFile("C:/Users/tesnim/Desktop/sparkStreaming/tesnim.txt")
        
  })
      
  ssc.start()
  ssc.awaitTermination()
  }
}