package org.learn.common

/**
 * Created by asus on 2015/5/10.
 */
import org.slf4j.LoggerFactory

trait LogSupport {
  protected val log = LoggerFactory.getLogger(this.getClass)
}
