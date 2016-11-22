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

object readText {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
  
   val sparkConf = new SparkConf().setMaster("local[4]").setAppName("TopHashtags")
   val sc=new SparkContext(sparkConf) // An existing SparkContext.
   val ssc = new StreamingContext(sc, Seconds(6))
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)

   // Create an RDD
    val people = sc.textFile("C:/Users/tesnim/Desktop/sparkStreaming/tesnim.txt");
  
   // The schema is encoded in a string
    val schemaString = "tweet"

    // Import Row.
    import org.apache.spark.sql.Row;

// Import Spark SQL data types
  import org.apache.spark.sql.types.{StructType,StructField,StringType};

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    
    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0)))

    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT tweet FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    val data = sc.textFile("C:/Users/tesnim/Desktop/train.csv")
    val labels = Map( "0" -> 0.0, "4" -> 1.0)

    val hashingTF = new HashingTF()
    val labelledTF =  data.map {line =>
      val parts = line.split(',')
      val label = labels(parts(0))
      val words : Seq[String] = parts(1).split("\\s+").map(_.toLowerCase)
      LabeledPoint(label, hashingTF.transform(words))
    }
  
   val splits = labelledTF.randomSplit(Array(0.7, 0.3), seed = 11L)
   val (train, test) = (splits(0), splits(1))
   test.cache()
  
    val model = NaiveBayes.train(train, lambda = 1.0) 
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
   
    var sumOfPositive = 0;
    var sumOfNegative = 0;
    var bl=results.map(t => "Tweet: " + t(0))
     for(item <- bl.collect().toArray) {
            println(item);
              val example = hashingTF.transform(item.split(" "))
                val words : Seq[String] = item.split("\\s+").map(_.toLowerCase)
      val textTweet = hashingTF.transform(words)
        println(s"Prediction of tweet item: ${model.predict(textTweet)}");
              
    if(model.predict(textTweet) == 1.0)
    {
      sumOfPositive += 1;
    }
    if(model.predict(textTweet)==0.0)
    {
      sumOfNegative += 1;
    }
    
    println(s"Number of Positive Tweets are: $sumOfPositive");
    println(s"Number of Negative Tweets are: $sumOfNegative");

  }
}