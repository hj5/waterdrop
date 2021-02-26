package io.github.interestinglab.waterdrop.spark.sink

import java.security.PrivilegedAction

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Dataset, Row}


class Hdfs extends FileSinkBase {

  override def checkConfig(): CheckResult = {
    checkConfigImpl(List("hdfs://"))
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
    outputImpl(data, "hdfs://")
  }
}
