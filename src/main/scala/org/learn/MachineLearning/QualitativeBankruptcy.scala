package org.learn.MachineLearning

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS

/**
 * Created by asus on 2015/7/25.
 */


object QualitativeBankruptcy {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Qualitative Bankruptcy")
    //sparkConf.setMaster("local[*]")
    conf.setMaster("spark://Master:7077").set("spark.driver.host","192.168.94.1").set("spark.cores.max","2")
    conf.set("spark.eventLog.enabled","true").set("spark.eventLog.dir","hdfs://Master:9000/tmp/spark-events")
    conf.setJars(List("D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\sparklearning.jar"))

    val sparkContext = new SparkContext(conf)

    val data = sparkContext.textFile("hdfs://master:9000/data/Bankruptcy/Qualitative_Bankruptcy.data.txt")
    val parsedData = data.map{line =>
      val parts = line.split(",")
      LabeledPoint(getDoubleValue(parts(6)),Vectors.dense(parts.slice(0,6).map(x=>getDoubleValue(x))))
    }
    //将parsedData的60%分为训练数据，40%分为测试数据
    val splits = parsedData.randomSplit(Array(0.8,0.2),seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)

    val labelAndPreds = testData.map{point =>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }

     
    println(labelAndPreds.toDebugString)
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble/testData.count
    println(s"TrainError:$trainErr")
  }


  def getDoubleValue(input:String) : Double =
    input match {
      case "P" => (3.0)
      case "A" => (2.0)
      case "N" => (1.0)
      case "NB" => (0.0)
      case "B" => (1.0)
      case _ => 999999
    }

}
