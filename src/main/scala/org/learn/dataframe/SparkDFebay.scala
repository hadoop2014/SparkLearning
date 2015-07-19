package org.learn.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by asus on 2015/6/26.
 */
case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate: Integer, openbid: Float, price: Float, item: String, daystolive: Integer)

object SparkDFebay {

  def  main (args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    val ebayText = sc.textFile("src/main/resources/ebay.csv")
    val ebay = ebayText.map(_.split(",")).map(p=>Auction(p(0),p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat,p(6).toFloat,p(7),p(8).toInt))
    val auction = ebay.toDF()
    auction.show()
    auction.printSchema()

    val count = auction.select("auctionid").distinct.count()
    System.out.println(count)

  }

}
