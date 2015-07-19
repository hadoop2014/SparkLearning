package org.learn

import java.util.concurrent.{Callable, Executors}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by asus on 2015/6/10.
 */
class Matrix(private val repr:Array[Array[Double]]) {
  def row(idx:Int):Seq[Double] = {
    repr(idx)
  }
  def col(idx:Int):Seq[Double] = {
    repr.foldLeft(ArrayBuffer[Double]()){
      (buffer,currentRow) =>
        buffer.append(currentRow(idx))
        buffer
    }.toArray
  }
  lazy val rowRank = repr.size
  lazy val colRank = if(rowRank > 0) repr(0).size else 0
  override def toString = "Matrix" + repr.foldLeft(""){
    (msg,row) => msg + row.mkString("\n|","|","|")
  }
}

trait ThreadStrategy{
  def execute[A](func: Function0[A]):Function0[A]
}

object MatrixUtils{
  def multiply(a:Matrix,b:Matrix)(implicit Threading:ThreadStrategy):Matrix = {
    assert(a.colRank == b.rowRank)
    val buffer =new Array[Array[Double]](a.rowRank)
    for (i <- 0 until a.rowRank){
      buffer(i) = new Array[Double](b.colRank)
    }
    def computeValue(row:Int,col:Int):Unit = {
      val pairwiseElements = a.row(row).zip(b.col(col))
      val products =
        for((x,y) <- pairwiseElements) yield x * y
      val results = products.sum
      buffer(row)(col) = results
    }
    val computations = for {
      i <- 0 until a.rowRank
      j <- 0 until b.colRank
    } yield Threading.execute{() => computeValue(i,j)}
    computations.foreach(_())
    new Matrix(buffer)
  }

}

object SameThreadingStrategy extends ThreadStrategy{
  override def execute[A](func:Function0[A]):Function0[A] = func
}

object TreadPoolStrategy extends ThreadStrategy{
  val pool = Executors.newFixedThreadPool(java.lang.Runtime.getRuntime.availableProcessors())
  override def execute[A](func:Function0[A]) = {
    val future = pool.submit(new Callable[A] {
      def call():A = {
        Console.println("Executing function on thread:" + Thread.currentThread().getName)
        func()
      }
    })
    () => future.get()
  }
}

object Matrix extends App{
  implicit val ts = SameThreadingStrategy
  val x = new Matrix(Array(Array(1,2,3),Array(4,5,6)))
  val y = new Matrix(Array(Array(1),Array(1),Array(1)))
  val result = MatrixUtils.multiply(x,y)
  println(result.toString)
}