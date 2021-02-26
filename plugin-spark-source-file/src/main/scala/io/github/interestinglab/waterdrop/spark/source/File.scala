package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.common.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import scala.collection.JavaConversions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.{Failure, Success, Try}

class File extends SparkBatchSource {

  override def prepare(env: SparkEnvironment): Unit = {}

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(config.getString("path"), "file://")
    fileReader(env.getSparkSession, path)
  }

  override def getReQuiredOptions(): List[String] = {
    List("path")
  }

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "format" -> "json",
        "kerberos.username" -> "",
        "kerberos.keytab" -> ""
      )
    )

    this.config = config.withFallback(defaultConfig)
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }

    path
  }

  protected def fileReader(spark: SparkSession, path: String): Dataset[Row] = {
    val format = config.getString("format")
    var reader = spark.read.format(format)

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "options.", false)) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader = reader.options(optionMap)
      }
      case Failure(exception) => // do nothing
    }

    format match {
      case "text" => reader.load(path).withColumnRenamed("value", "raw_message")
      case "parquet" => reader.parquet(path)
      //      case "xml" => reader.xml(path)
      case "json" => reader.json(path)
      case "orc" => reader.orc(path)
      case "csv" => reader.csv(path)
      case _ => reader.format(format).load(path)
    }
  }

}
