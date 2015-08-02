package org.learn.Hdfs

import java.io.File

import org.apache.hadoop.fs.Path
import org.learn.common._
import org.apache.hadoop.conf.Configuration

/**
 * Created by asus on 2015/8/1.
 */
object HdfsManager extends HdfsFunction{

  lazy val currentPath = System.getProperty("user.dir")

  val hdfsConf = configHdfs()

  def configHdfs(): Configuration = {
    val hdfsConfig = new Configuration()

    ConfigManager.siteConfFiles.foreach{
      file =>
        if(new File(file).exists()){
          hdfsConfig.addResource(new Path(file))
          log.info(s"Hdfs manager loader file: $file in path:$currentPath")
        }
        else {
          val configPath = file.split("/").init.mkString("/")    //获取路径名
          val configFile = file.split("/").last

          if (new File(file).exists()){
            hdfsConfig.addResource(new Path(file))
            log.info(s"Hdfs manager loader file: $file in path:$currentPath,but not in path $configPath")
          }
          else {
            log.info(s"Hdfs manager loader file: $file failure, it is not in path:$currentPath or path $configPath")
          }
        }
    }
    hdfsConfig
  }

  def getDfsNameServices(): String = {
    val dfsname = hdfsConf.get("dfs.nameservices")
    log.info(s"getDfsNameServices return $dfsname")
    dfsname
  }

}


