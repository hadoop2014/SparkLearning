package org.learn

import java.sql.ResultSet

/**
 * Created by asus on 2015/5/10.
 */
package object database {
  object ConnectionFactoryPrefs{
    implicit val Spark = new SparkConnectionFactory {}
  }

  implicit class ResultSetUtil(rs:ResultSet){
    private val columnCount = rs.getMetaData.getColumnCount

    def rows:List[List[String]] = {
      var valueList:List[List[String]] = List()
      while(rs.next()){
        val oneLine = (1 to columnCount).map(rs.getString).toList
        valueList = oneLine :: valueList
      }
      valueList.reverse
    }

    def columns:List[String] = {
      (1 to columnCount).map(rs.getMetaData.getColumnName).toList
    }
  }
}
