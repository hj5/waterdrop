package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

class Hive extends SparkBatchSource {

  override def prepare(env: SparkEnvironment): Unit = {}

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    env.getSparkSession.sql(config.getString("pre_sql"))
  }

  override def getReQuiredOptions(): List[String] = {
    List("pre_sql")
  }

}
