package io.github.interestinglab.waterdrop.spark.source

import java.security.PrivilegedAction

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Dataset, Row}

class Hdfs extends File {

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(config.getString("path"), "hdfs://")
    //用户登录
    val keyTab: String = config.getString("kerberos.keytab")
    if(!keyTab.isEmpty){
      UserGroupInformation.setConfiguration(env.getSparkSession.sparkContext.hadoopConfiguration)
      //    val a = UserGroupInformation.loginUserFromKeytabAndReturnUGI("api/presto-server", config.getString("kerberos.keytab"))
      //    a.doAs(
      //      new PrivilegedAction[Dataset[Row]]() {
      //        override def run: Dataset[Row] = {
      //          fileReader(env.getSparkSession, path)
      //        }
      //      })
      UserGroupInformation.loginUserFromKeytab(config.getString("kerberos.username"), keyTab)
    }

    fileReader(env.getSparkSession, path)
  }

}
