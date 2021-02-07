package io.github.interestinglab.waterdrop.spark

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSource
import io.github.interestinglab.waterdrop.common.config.CheckResult

trait BaseSparkSource[Data] extends BaseSource[SparkEnvironment] {

  protected var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  def getData(env: SparkEnvironment): Data;

  /**
   * required option, must be sub class Impl
   * @return
   */
  def getReQuiredOptions(): List[String] ={
    List("")
  }

  override def checkConfig(): CheckResult = {
    checkConfig(getReQuiredOptions)
  }

  def checkConfig(requiredOptions: List[String]): CheckResult = {

    val nonExistsOptions = requiredOptions
      .map(optionName => (optionName, config.hasPath(optionName)))
      .filter { p =>
        val (optionName, exists) = p
        !exists
      }
    if (nonExistsOptions.isEmpty) {
      new CheckResult(true, "")
    } else {
      new CheckResult(
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string"
      )
    }

  }

}
