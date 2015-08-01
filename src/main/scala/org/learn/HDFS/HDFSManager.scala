package org.learn.HDFS

import org.scalatest.prop.Configuration

/**
 * Created by asus on 2015/8/1.
 */
object HdfsManager extends HdfsSupport{

  lazy val currentPath = System.getProperty("user.dir")

  val hdfsConf = config

  def configHdfs(): Configuration = {
    val hdfsConfig = new Configuration()

  }


}
/*
 lazy val currentPath = System.getProperty("user.dir")

  val hdfsConf = configHdfs()

  def configHdfs(): Configuration = {
    val hdfsConfig = new Configuration()

    CdmaEtlConfig.siteConfFiles.foreach {
      file =>
        if (new java.io.File(file).exists()) {
          hdfsConfig.addResource(new Path(file))
          log.debug(s"HdfsFileManager addResource from config directory: $file.current directory ${currentPath}")
        }
        else {
          val configPath = file.split("/").init.mkString("/")
          val configFilename = file.split("/").last

          if (new java.io.File(configFilename).exists()) {
            hdfsConfig.addResource(new Path(configFilename))
            log.debug(s"HdfsFileManager addResource from current directory ${currentPath}: adding $configFilename: Cannot find file in config directory ${configPath}")
          }
          else {
            log.debug(s"HdfsFileManager addResource adding $configFilename failure: Cannot find file in config directory ${configPath} and current directory ${currentPath}")
          }
        }
    }

    hdfsConfig.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec")

    hdfsConfig
  }


  def fileSize(filename: String): Long = {
    var size = 0l
    usingHdfs("get file status failure") {
      hdfs =>
        val status = hdfs.getFileStatus(new Path(filename))
        size = if (status.isDirectory) listFiles(filename).map(file => fileSize(s"$filename/$file")).sum
        else hdfs.getFileStatus(new Path(filename)).getLen
    }
    size
  }

  def isDirectory(dir: String): Boolean = {
    var res: Boolean = false
    usingHdfs("Check dir exists failed.") {
      hdfs =>
        val p = new Path(dir)
        res = hdfs.isDirectory(p)
    }
    res
  }

  def isFile(fileName: String): Boolean = {
    var res: Boolean = false
    usingHdfs("Check fileName exists failed.") {
      hdfs =>
        val p = new Path(fileName)
        res = hdfs.isFile(p)
    }
    res
  }

  // HdfsSupport 中的 upload 在 EmsPmDataImportSpec 用例中总会上传失败，原因未知，暂时先重写一个
  override def upload(src: String, target: String): Unit = {
    usingHdfs("upload failed!") {
      hdfs =>
        hdfs.copyFromLocalFile(false, true, new Path(src), new Path(target))
    }
  }

  def rename(src: String, target: String): Unit = {
    usingHdfs("rename failed!") {
      hdfs =>
        val srcpath = new Path(src)
        val targetpath = new Path(target)

        if (!hdfs.exists(targetpath.getParent)) hdfs.mkdirs(targetpath.getParent)
        else if (hdfs.exists(targetpath)) delete(target)

        hdfs.rename(srcpath, targetpath)
    }
  }

  def mv(srcFiles: List[String], targetPath: String): Unit = {
    usingHdfs("mv files failed!") {
      hdfs =>
        srcFiles.map(fileName => hdfs.rename(new Path(fileName), new Path(s"$targetPath/${fileName.reverse.takeWhile(_ != '/').reverse}")))
    }
  }

  def get(srcPath: String, localPath: String, fileFilter: String => Boolean = {
    str => true
  }): Unit = {
    usingHdfs("get failed!") {
      hdfs =>
        val files = hdfs.listFiles(new Path(srcPath), false)
        val local = new File(localPath)
        if (!local.exists()) local.mkdirs()
        while (files.hasNext) {
          val file = files.next()
          if (fileFilter(file.getPath.getName)) hdfs.copyToLocalFile(false, file.getPath, new Path(localPath + s"/${file.getPath.getName}"), true)
        }
    }
  }


  def HDFSDownLoad(source: String, target: String): Unit = {
    usingHdfs("download failed.") {
      hdfs =>
        val srcPath = source
        val out = new FileOutputStream(s"$target")
        if (hdfs.exists(new Path(srcPath))) {
          val files = hdfs.listFiles(new Path(srcPath), false)
          log.info("files path" + files)
          try {
            while (files.hasNext) {
              log.info("while start")
              val file = files.next()
              if (file.getPath.toString.contains("part-")) {
                val in = hdfs.open(new Path(file.getPath.toString))
                try {
                  IOUtils.copyBytes(in, out, 4096, false)
                } finally {
                  if (in != null) in.close()
                }
              }
            }
          } catch {
            case e: Throwable => e.printStackTrace()
          } finally {
            if (out != null) out.close()
          }
        }
        else {
          log.debug("download Error:" + srcPath + " not exist ")
        }
    }
  }

  def appendDir(srcPath: String, dstPath: String): Unit = {
    log.debug(s"try to append $srcPath to $dstPath ...")
    listFiles(srcPath).foreach(fileName => if (fileName.startsWith("part-")) {
      appendFile(s"$srcPath/$fileName", s"$dstPath/$fileName")
    })
  }


  def appendFile(src: String, target: String): Unit = {
    log.debug(s"try to append $src to $target ...")
    usingHdfs("append failed") {
      hdfs =>
        val in = hdfs.open(new Path(src))
        val out = if (hdfs.exists(new Path(target))) {
          hdfs.create(new Path(target + System.currentTimeMillis()))
          //          hdfs.append(new Path(target))
        }
        else {
          hdfs.create(new Path(target))
        }
        try {
          IOUtils.copyBytes(in, out, 4096, true)
        } finally {
          if (in != null) in.close()
          if (out != null) out.close()
        }
    }
    log.debug(s"append $src to $target complete")
  }

  def getDfsNameServices(): String = {
    val dfsname = hdfsConf.get("dfs.nameservices")
    log.info(s"getDfsNameServices return $dfsname")
    dfsname
  }

  def downloadFile(hdfsFileName: String, localFileName: String, delSourceFile: Boolean = false) = {
    usingHdfs("download one file to local") {
      hdfs =>
        hdfs.copyToLocalFile(delSourceFile, new Path(hdfsFileName), new Path(localFileName))
    }
  }


 */