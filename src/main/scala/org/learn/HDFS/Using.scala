package org.learn.HDFS

/**
 * Created by asus on 2015/8/1.
 */
trait Using {

  protected def using[A <: {def close() : Unit},B](param : A)(f : A => B) : B = {
    try{
      f(param)
    }finally {
      param.close()
    }
  }
}
