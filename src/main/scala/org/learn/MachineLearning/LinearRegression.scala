package org.learn.MachineLearning

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}
import org.learn.common.LogSupport

object LinearRegression extends  App with LogSupport{
  val conf = new SparkConf().setAppName("LinearRegression")
  //conf.setMaster("local[*]")
  conf.setMaster("spark://Master:7077").set("spark.driver.host","192.168.94.1").set("spark.cores.max","2")
  conf.set("spark.eventLog.enabled","true").set("spark.eventLog.dir","hdfs://Master:9000/tmp/spark-events")
  conf.setJars(List("D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\sparkleaning.jar"))
  conf.set("spark.executor.extraJavaOptions", "-server -XX:PermSize=256M -XX:MaxPermSize=512m")
  val context = new SparkContext(conf)
  val rawData = context.textFile("hdfs://Master:9000/data/linearregression.txt")

  val parsedData = rawData.map({line =>
    val split = line.split(' ')           // 字符串按空格切分
    val y = split(0).toDouble             // 第一个数转成Double型，是因变量
    val x = split.tail.map(_.toDouble)    // 其余的转成自变量向量
    LabeledPoint(y,Vectors.dense(x))      // 把因变量和自变量打包
  }).cache()

  println(s"${parsedData.count()}")

  val model = new LinearRegressionWithSGD()

  model.optimizer.setNumIterations(100)
  model.setIntercept(true)

  val tstart = System.currentTimeMillis()
  val res = model.run(parsedData)
  val tend = System.currentTimeMillis()

  println("Model training time: " + (tend - tstart) / 1000.0 + " secs")

  println(s"${res.intercept}")
  println(s"${res.weights}")

  val pred = res.predict(parsedData.map(_.features))

  val r = Statistics.corr(pred, parsedData.map(_.label))
  println("R-square = " + r * r)
}

