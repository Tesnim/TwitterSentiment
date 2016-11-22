package org.pa.stream
import org.apache.spark._
//import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification._

object machine {
  
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
    val conf = new SparkConf().setAppName(s"Spark example: Scala").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:/Users/tweetDataset.csv")
    
    // Load training data in LIBSVM format.
val data = sc.textFile("C:/Users/tesnim/Desktop/train.csv")
val labels = Map( "0" -> 0.0, "4" -> 1.0)

  val hashingTF = new HashingTF()
  val labelledTF =  data.map {line =>
    val parts = line.split(',')
    val label = labels(parts(0))
    val words : Seq[String] = parts(1).split("\\s+").map(_.toLowerCase)
    LabeledPoint(label, hashingTF.transform(words))
  }

  // Split data into training (60%) and test (40%).
  val splits = labelledTF.randomSplit(Array(0.7, 0.3), seed = 11L)
  val (train, test) = (splits(0), splits(1))
  test.cache()
  
  val model = NaiveBayes.train(train, lambda = 1.0) 
  val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
  
  val accuracy = 
    1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
      
  val text1="Good one #NikonCamera";
  val posTestExample = hashingTF.transform(text1.split(" "))
  val words : Seq[String] = text1.split("\\s+").map(_.toLowerCase)
  val textTweet = hashingTF.transform(words)
 
  println(f"accuracy: ${accuracy * 100}%2.3f %%")
  println(text1 + textTweet)
  println(s"Prediction of tweet $text1: ${model.predict(textTweet)}");
  
  }
}