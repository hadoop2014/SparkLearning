package org.learn.Hdfs

import java.io.{ByteArrayInputStream, FileOutputStream, BufferedInputStream, IOException, File}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.learn.common._

/**
 * Created by asus on 2015/8/1.
 */
trait HdfsFunction extends LogSupport with Using {

  def hdfsConf : Configuration

  private var errorCnt = 0
  private var usingCnt = 0

  //Hdfs句柄的借贷模式
  def usingHdfs(errMsg: String)(f: FileSystem => Unit): Unit = {
    var hdfs: FileSystem = null
    this.synchronized(usingCnt += 1)
    if (usingCnt >= Constant.constMaxHdfsUseCnt.id) {
      log.info(s"Hdfs handler num go to up:$usingCnt")
      this.synchronized(usingCnt = 0)
    }
    try {
      hdfs = FileSystem.get(hdfsConf)
      f(hdfs)
    }
    catch {
      case e: IOException =>
        this.synchronized(errorCnt += 1)
        log.error(errMsg + s"error Count:$errorCnt", e)
        throw e
      case ex: Throwable =>
        this.synchronized(errorCnt += 1)
        log.error(errMsg + s"error Count: $errorCnt", ex)
        throw ex
    }
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

  //删除hdfs文件及目录
  def deletePath(target: String): Unit = {
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

  //列出HDFS目录下所有文件
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

  //获取文件或目录下所有文件大小
  def fileSize(filename: String): Long = {
    var size = 0l
    usingHdfs("get file status failure：") {
      hdfs =>
        size = if (hdfs.getFileStatus(new Path(filename)).isDirectory)
          listFiles(filename).map(file => fileSize(s"$filename/$file")).sum
        else hdfs.getFileStatus(new Path(filename)).getLen
    }
    size
  }

  //判断是否为目录
  def isDirectory(dir: String): Boolean = {
    var result = false
    usingHdfs("Check dir exists failed：") {
      hdfs =>
        result = hdfs.isDirectory(new Path(dir))
    }
    result
  }

  //判断是否为文件
  def isFile(fileName: String): Boolean = {
    var result: Boolean = false
    usingHdfs("Check fileName exists failed.") {
      hdfs =>
        result = hdfs.isFile(new Path(fileName))
    }
    result
  }

  //HDFS文件改名
  def rename(source: String, target: String): Unit = {
    usingHdfs("rename failed:") {
      hdfs =>
        val sourcePath = new Path(source)
        val targetPath = new Path(target)

        if (!hdfs.exists(targetPath.getParent)) hdfs.mkdirs(targetPath.getParent)
        else if (hdfs.exists(targetPath)) hdfs.delete(targetPath ,true)

        hdfs.rename(sourcePath,targetPath)
    }
  }

  //移动文件到新的目录下
  def mv(sourceFile: List[String], targetPath: String): Unit = {
    usingHdfs("mv files failed:") {
      hdfs =>
        sourceFile.map(
          fileName =>
            hdfs.rename(new Path(fileName), new Path(s"$targetPath/${fileName.split("/").last}"))
            //fileName.reverse.takeWhile(_ != '/').reverse
        )
    }
  }

  //获取HDFSY目录下所有文件到本地
  def getFilesFromHdfs(hdfsPath: String, localPath: String, fileFilter: String => Boolean = {str => true}): Unit = {
    usingHdfs("get failed:") {
      hdfs =>
        val files = hdfs.listFiles(new Path(hdfsPath), false)
        val local = new File(localPath)
        if (!local.exists()) local.mkdirs()
        while (files.hasNext) {
          val file = files.next()
          if (fileFilter(file.getPath.getName)) hdfs.copyToLocalFile(false, file.getPath, new Path(localPath + s"/${file.getPath.getName}"), true)
        }
    }
  }

  //本地目录所有文件拷贝到HDFS目录下
  def putFilesToHdfs(localPath: String, hdfsPath: String, fileFilter: String => Boolean = {str => true}): Unit = {
    new File(localPath).listFiles.foreach(
      file =>
        if(file.isFile){
          copyFromLocalFile(file.getPath,s"$hdfsPath/${file.getName}")
        }
    )
  }

  // 拷贝本地文件到HDFS
  def copyFromLocalFile(localFile: String, hdfsFile: String, delSource: Boolean = false,overwrite: Boolean = true): Unit = {
    usingHdfs("Copy file to HDFS failed:") {
      hdfs =>
        hdfs.copyFromLocalFile(delSource, overwrite, new Path(localFile), new Path(hdfsFile))
    }
  }

  // 拷贝HDFS文件到本地
  def copyToLocalFile(hdfsFile: String, localFile: String, delSource: Boolean = false) = {
    usingHdfs("download one file to local") {
      hdfs =>
        hdfs.copyToLocalFile(delSource, new Path(hdfsFile), new Path(localFile))
    }
  }

  //在hdfs上建立目录
  def mkDir(target: String): Unit = {
    usingHdfs("mkdir failed:"){
      hdfs =>
        val path = new Path(target)
        if(!hdfs.exists(path))
          hdfs.mkdirs(path)
    }
  }

  //删除本地文件夹,文件夹下存在子目录
  def rmLocalDir(localPath: String): Unit = {
    val files = new File(localPath).listFiles()
    files.foreach(
      file => {
        file.delete()
      }
    )
    new File(localPath).delete()
  }

}
