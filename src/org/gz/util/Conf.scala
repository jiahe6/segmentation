package org.gz.util

import com.typesafe.config.ConfigFactory

trait Conf {
  val config =  ConfigFactory.load("application.conf")
}