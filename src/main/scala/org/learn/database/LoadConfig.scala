package org.learn.database

/**
 * Created by asus on 2015/5/10.
 */
import com.typesafe.config
import com.typesafe.config.ConfigFactory

object LoadConfig {
  def getString(key: String):String = ConfigFactory.load("database.conf").getString(key)
}
