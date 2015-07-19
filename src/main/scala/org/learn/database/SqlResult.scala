package org.learn.database

/**
 * Created by asus on 2015/5/10.
 */
class SqlResult(val name:List[String],val value:List[List[String]]) {
  override def toString: String = {
    (name :: value).map(_.mkString(",")).mkString("\n")
  }
  //def columns = if value isEmpty 0 else value.length
  //def length = value
  def toList: List[List[String]] = name :: value
  def cell(row: Int, column: Int): String = {
    if (row < value.length && column < name.length)value(row)(column)
    else ""
  }
  def cellOption(row: Int, column: Int): Option[String] = {
    if (row < value.length && column < name.length) Some(value(row)(column))
    else None
  }
  def cell(row: Int,colunmName:String):String = {
    val column = name.indexOf(colunmName)
    if(row < value.length && column  < name.length)value(row)(column)
    else ""
  }
  def cellOption(row: Int, columnName: String): Option[String] = {
    val column = name.indexOf(columnName)
    if (row < value.length && column < name.length) Some(value(row)(column))
    else None
  }
  def resetColumns(columnNames: List[String]): SqlResult = {
    require(this.name.length == columnNames.length)
    SqlResult(columnNames, this.value)
  }
}

object SqlResult {
  def apply(name:List[String],value: List[List[String]]):SqlResult = new SqlResult(name,value)
}
