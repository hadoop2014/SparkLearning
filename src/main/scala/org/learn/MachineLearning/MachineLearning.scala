package org.learn.MachineLearning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

//import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.util.MLUtils


/**
 * Created by asus on 2015/5/19.
 */
object MachineLearning {
  val conf = new SparkConf().setAppName("MachineLearing")
  conf.setMaster("local[*]")
  val sparkContext = new SparkContext(conf)

  val examples = MLUtils.loadLabeledData(sparkContext,"hdfs://Master:9000/data/sogouq.mini").cache()

  val splits = examples.randomSplit(Array(0.8,0.2))
  val training = splits(0).cache()
  val test = splits(1).cache()

  val numTraining = training.count()
  val numTest = training.count()
  println(s"Training:$numTraining,Test:$numTest")

  val updater = new SquaredL2Updater

  val model = {
    val algorithm = new LogisticRegressionWithSGD()
    algorithm.optimizer.setNumIterations(200).setStepSize(1.0).setUpdater(updater).setRegParam(0.1)
    algorithm.run(training).clearThreshold()
  }

  val rprediction = model.predict(test.map(_.features))
  val rpredictionAndLabel = rprediction.zip(test.map(_.label))
  val metrics = new BinaryClassificationMetrics(rpredictionAndLabel)

  println("\n")
  println("Cetification Weight:" + model.weights(0))
  println("Wage Plan Weight" + model.weights(1))
  println("Fatigue by Hours Weight" + model.weights(2))
  println("Fatigue by Miles Weight" + model.weights(3))
  println("Foggy weather Weight" + model.weights(4))
  println("Rainy weather Weight" + model.weights(5))
  println("Windy weather Weight" + model.weights(6))
  println("\n")
  println("Intercept" + model.intercept)
  println(s"$metrics.grep")
  println(s"test.areaUnderPR = ${metrics.areaUnderPR()}")
}
