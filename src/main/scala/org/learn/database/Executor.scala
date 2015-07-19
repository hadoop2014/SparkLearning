package org.learn.database

import java.sql.{SQLException, ResultSet, Connection, Statement}
import org.learn.database.Sql
import org.learn.database.ConnectionFactory
import org.learn.database.LogSupport
import org.learn.database.SqlResult

/**
 * Created by asus on 2015/5/10.
 */
object Executor {
  trait QueryExecutor{
    this:Sql =>
    def query(conn:Connection):SqlResult = DbAccessor.query(conn,sqlStatement)

    def query(implicit conFactory:ConnectionFactory):SqlResult = {
      val conn = conFactory.createConnection
      try{
        query(conn)
      }
      finally {
          if(conn != null)conn.close()
      }
    }

  }

  trait CommandExecutor{
    this: Sql =>
    //for transaction
    def execute(conn: Connection): Boolean = DbAccessor.execute(conn, sqlStatement)
    //for standalone
    def execute(implicit connFactory: ConnectionFactory): Boolean = {
      val conn = connFactory.createConnection
      try{
        execute(conn)
      }
      finally {
        if(conn != null) conn.close()

      }
    }
    //for transaction
    def update(conn: Connection): Boolean = DbAccessor.update(conn, sqlStatement)
    //for standalone
    def update(implicit connFactory: ConnectionFactory):Boolean = {
      val conn = connFactory.createConnection
      try{
        update(conn)
      }finally {
        if(conn != null)conn.close()
      }
    }
  }

  object DbAccessor extends LogSupport{
    def query(conn: Connection, sqlStatement: String) = {
      var stmt:Statement = null
      var rs:ResultSet = null

      try {
        stmt = conn.createStatement()
        log.debug(s"execute:"+s"$sqlStatement")
        rs = stmt.executeQuery(sqlStatement)
        SqlResult(rs.columns,rs.rows)
      }
      catch {
        case e:SQLException => log.debug("catch SQLException when executeQuery Sql:" + sqlStatement)
          e.printStackTrace()
          SqlResult(List(),List())
      }
      finally {
        try{
          if(rs != null) rs.close()
          if(stmt != null) stmt.close()
        }
        catch{
          case e:SQLException => log.debug("catch SQLException in rs.close or stmt.close or conn.close()!")
        }
      }
    }
    def update(conn: Connection,sql: String): Boolean = {
      var stmt: Statement = null
      try{
        stmt = conn.createStatement()
        log.debug(s"db update :$sql")
        stmt.executeUpdate(sql)
        true
      }
      catch {
        case e: SQLException => println("catch SQLException when executeUpdate sql: " + sql)
          e.printStackTrace()
          false
      }
      finally {
        try{
          if(stmt != null) stmt.close()
        }
        catch {
          case e:SQLException =>  println("catch SQLException in stmt.close or conn.close()!")
        }
      }
    }

    def execute(conn: Connection, sql: String): Boolean = {
      var stmt: Statement = null
      try {
        stmt = conn.createStatement()
        log.debug(s"db execute: $sql")
        stmt.execute(sql)
        true
      }
      catch {
        case e: SQLException => println("catch SQLException when execute sql: " + sql)
          e.printStackTrace()
          false
      }
      finally {
        try {
          if (stmt != null) stmt.close()
        }
        catch {
          case e: SQLException => println("catch SQLException in stmt.close or conn.close()!")
        }
      }
    }
  }


}
