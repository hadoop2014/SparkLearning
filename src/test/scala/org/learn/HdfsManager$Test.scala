package org.learn

import org.learn.Hdfs._
import org.learn.common.LogSupport
import org.scalatest.{BeforeAndAfter, FlatSpec, ShouldMatchers}
import java.io.File

/**
 * Created by asus on 2015/8/2.
 */
class HdfsManager$Test extends FlatSpec with ShouldMatchers with LogSupport with BeforeAndAfter{

  val target = "hdfs://Master:9000/hdfstest"
  val localPath = "./src/main/resources/"

  it should "create Hdfs test dictionary " in {
    HdfsManager.mkDir(target)
    HdfsManager.isDirectory(target) should be (true)
  }

  it should "copy local file to hdfs" in{
    HdfsManager.putFilesToHdfs(localPath,target)
    HdfsManager.listFiles(target).size should be (6)
  }

  it should "copy hdfs file to local" in {
    val targetPath = "./hdfstest"
    HdfsManager.getFilesFromHdfs(target,targetPath)
    new File(targetPath).listFiles().size should be (6)
    HdfsManager.rmLocalDir(targetPath)
  }

  it should "delete hdfs test dictionary" in {
    HdfsManager.deletePath(target)
    HdfsManager.isDirectory(target) should be (false)
  }
}
