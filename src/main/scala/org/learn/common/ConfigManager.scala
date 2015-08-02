package org.learn.common

/**
 * Created by asus on 2015/8/2.
 */
object ConfigManager {

  protected val configHome = "./src/main/resources/"

  val siteConfFiles = List(s"${configHome}core-site.xml", s"${configHome}hdfs-site.xml", s"${configHome}hive-site.xml")

}
