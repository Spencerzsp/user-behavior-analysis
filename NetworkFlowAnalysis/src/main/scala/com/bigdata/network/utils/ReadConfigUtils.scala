package com.bigdata.network.utils

import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.fluent.Parameters

/**
  * @ author spencer
  * @ date 2020/4/27 17:38
  */
object ReadConfigUtils {

  private val params = new Parameters()
  private val builder = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties().setFileName("my.properties"))

  // 通过 getConfiguration 获取配置对象
  val config = builder.getConfiguration()

}
