package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.config.ConfigFactory
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.sink.multi.path.KeyPathMultipleTextOutputFormat
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Hdfs extends FileSinkBase {

  var multiPathMode: Boolean = false

  override def checkConfig(): CheckResult = {
    checkConfigImpl(List("hdfs://"))
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "key.separator" -> "/", //in hdfs outputMultiPath mode Used to slice dataframe`s first column value to paths
        "multi.path.mode" -> false //whether is hdfs outputMultiPath mode
      )
    )
    config = config.withFallback(defaultConfig)
    multiPathMode = config.getBoolean("multi.path.mode")
    super.prepare(env)
  }

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    //用户登录
    val keyTab: String = config.getString("kerberos.keytab")
    if(!keyTab.isEmpty){
      UserGroupInformation.setConfiguration(env.getSparkSession.sparkContext.hadoopConfiguration)
      //    val a = UserGroupInformation.loginUserFromKeytabAndReturnUGI("api/presto-server", config.getString("kerberos.keytab"))
      //    a.doAs(
      //      new PrivilegedAction[Void]() {
      //        override def run: Void = {
      //          outputImpl(data, "hdfs://")
      //          null
      //        }
      //      })
      UserGroupInformation.loginUserFromKeytab(config.getString("kerberos.username"), keyTab)
    }
    if(multiPathMode){
      outputMultiPath(data, "hdfs://")
    } else {
      outputImpl(data, "hdfs://")
    }
  }

  /**
   * 根据field1输出数据到hdfs
   * @param df data must be like this：sc.parallelize(List(("d1_t1", "www"), ("d1_t2", "blog"), ("d2_t1", "com"), ("d2_t2", "bt")))
   * @param defaultUriSchema
   */
  def outputMultiPath(df: Dataset[Row], defaultUriSchema: String): Unit = {

    val path = buildPathWithDefaultSchema(config.getString("path"), defaultUriSchema)
    val rdd = df.rdd.map(
      row => (convertKey2path(row.getString(0)), row.getString(1)))
    rdd.saveAsHadoopFile(
        path,
        classOf[String],
        classOf[String],
        classOf[KeyPathMultipleTextOutputFormat])

  }

  /**
   * slice key to the relative path of the output to HDFS
   * @param key
   * @return string
   */
  def convertKey2path(key: String) = {
    val key_separator = config.getString("key.separator")
    if (key_separator.equals("/")){
      key
    } else {
      key.replaceAll(key_separator,"/")
    }

  }
}
