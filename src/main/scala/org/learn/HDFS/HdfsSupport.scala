package org.learn.HDFS

import java.io.{ByteArrayInputStream, FileOutputStream, BufferedInputStream, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.learn.common._
import org.learn.database._

/**
 * Created by asus on 2015/8/1.
 */
trait HdfsSupport extends LogSupport with Using{

  def hdfsConf : Configuration

  private var errorHdfsCnt = 0
  private var usingHdfsCnt = 0

  //Hdfs句柄的借贷模式
  def usingHdfs(errMsg: String)(f: FileSystem => Unit): Unit = {
    var hdfs: FileSystem = null
    this.synchronized(usingHdfsCnt += 1)
    if (usingHdfsCnt >= Constant.constMaxHdfsUseCnt.id){
      log.info(s"Hdfs handler num go to up:$usingHdfsCnt")
      this.synchronized(usingHdfsCnt = 0)
    }
    try{
      hdfs = FileSystem.get(hdfsConf)
      f(hdfs)
    }
    catch{
      case e:IOException =>
        this.synchronized(errorHdfsCnt += 1)
        log.error(errMsg + s"error Count:$errorHdfsCnt", e)
        throw e
      case ex:Throwable =>
        this.synchronized(errorHdfsCnt += 1)
        log.error(errMsg + s"error Count: $errorHdfsCnt",ex)
        throw ex
    }

    //上传HDFS文件
    def uploadFile(source: String ,target: String):Unit = {
      usingHdfs("upload file failed:"){
        hdfs =>
          using(new BufferedInputStream(this.getClass.getResourceAsStream(source))){
            in =>
              using(hdfs.create(new Path(target))){
                out =>
                  IOUtils.copyBytes(in,out,Constant.constBufferSize.id,true)
              }
          }
      }
    }

    //删除hdfs文件
    def deleteFile(target: String): Unit = {
      usingHdfs("delete failed:") {
        hdfs =>
          val path = new Path(target)
          if (hdfs.exists(path))
            hdfs.delete(path,true)
      }
    }

    //清空hdfs目录
    def emptyDir(dir: String): Unit = {
      usingHdfs("emptyDir failed:"){
        hdfs =>
          val path = new Path(dir)
        if (hdfs.isDirectory(path)){
          val files = hdfs.listFiles(path,false)
          while(files.hasNext){
            hdfs.delete(files.next().getPath,true)
          }
        }
      }
    }

    //下载hdfs文件,filter为过滤字符串,filter = "" 表示删除下载全部文件
    def downloadFile(source : String, target: String, filter: String = ""): Unit = {
      usingHdfs("download file failed:") {
        hdfs =>
          using(new FileOutputStream(target)){
            out =>
              val files = hdfs.listFiles(new Path(source),false)
              while(files.hasNext){
                val file = files.next().getPath().toString
                if (file.contains(filter) || filter.isEmpty)
                  using(hdfs.open(new Path(file))){
                    in =>
                      IOUtils.copyBytes(in,out,Constant.constBufferSize.id,false)
                  }
              }
          }
      }
    }
  }

  //追加HDFS文件,charset 为字符集
  def append(source: String, target: String,charset: String = "UTF-8"): Unit = {
    usingHdfs("append file failed:"){
      hdfs =>
        using(new ByteArrayInputStream(source.getBytes(charset))){
          in =>
            using(
              if (hdfs.exists(new Path(target))){
                hdfs.append(new Path(target))
              }
              else{
                hdfs.create(new Path(target))
              }
            ){
              out =>
                IOUtils.copyBytes(in,out,Constant.constBufferSize.id,false)
            }
        }
    }
  }

  //列出目录下所有文件
  def listFiles(path: String): List[String] = {
    var result:List[String] = Nil
    usingHdfs("listFiles failed:"){
      hdfs =>
        if(hdfs.exists(new Path(path))){
          result = hdfs.listStatus(new Path(path)).map(_.getPath.getName).toList
        }
    }
    result
  }

  //上传压缩文件
  def uploadCompressionFile(source: String,target: String,charset:String = "UTF-8",codecClassNmae: String = "org.apache.hadoop.io.compress.GzipCodec"): Unit = {
    usingHdfs("write compressed file failed:"){
      hdfs =>
        val factory  = new CompressionCodecFactory(hdfsConf)
        val codec = factory.getCodecByClassName(codecClassNmae)
        val extension = codec.getDefaultExtension

        using(new ByteArrayInputStream(source.getBytes(charset))){
          in =>
            using(hdfs.create(new Path(target + extension))){
              out =>
                using(codec.createOutputStream(out)){
                  compressionOutputStream =>
                    IOUtils.copyBytes(in,compressionOutputStream,hdfsConf,true)
                }
            }
        }

    }

  }
}
