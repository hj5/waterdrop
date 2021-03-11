package io.github.interestinglab.waterdrop.spark.source

import java.util.Properties

import io.github.interestinglab.waterdrop.common.config.{CheckResult, TypesafeConfigUtils}
import io.github.interestinglab.waterdrop.config.ConfigFactory
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingSource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._

class KafkaStream extends SparkStreamingSource[(String, String)] {

  private var schema: StructType = _

  private val kafkaParams = new Properties()

  private var offsetRanges: Array[OffsetRange] = _

  private var inputDStream: InputDStream[ConsumerRecord[String, String]] = _

  private val consumerPrefix = "consumer."

  private var topics: Set[String] = _

  private var key_type: String = _

  var test: Boolean = false

  override def prepare(env: SparkEnvironment): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "key.type" -> "topic", //allowed values: topic、key
        "test" -> "false", //是否是测试模式：不提交kafka offset
        consumerPrefix + "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        consumerPrefix + "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        consumerPrefix + "enable.auto.commit" -> false
      )
    )

    config = config.withFallback(defaultConfig)
    key_type = config.getString("key.type")
    test = config.getBoolean("test")
    schema = StructType(
      Array(StructField(key_type, DataTypes.StringType),
            StructField("raw_message", DataTypes.StringType)))

    topics = config.getString("topics").split(",").toSet
    val consumerConfig =
      TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
    consumerConfig.entrySet.foreach(entry => {
      val key = entry.getKey
      val value = entry.getValue.unwrapped
      kafkaParams.put(key, String.valueOf(value))
    })

    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
  }

  override def rdd2dataset(sparkSession: SparkSession,
                           rdd: RDD[(String, String)]): Dataset[Row] = {
    val value = rdd.map(record => Row(record._1, record._2))
    sparkSession.createDataFrame(value, schema)
  }

  override def getData(env: SparkEnvironment): DStream[(String, String)] = {

    inputDStream = KafkaUtils.createDirectStream(
      env.getStreamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams))

    key_type match {
      case "topic" =>
        inputDStream.transform { rdd =>
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.map(record => {
            (record.topic(), record.value())
          })
        }
      case "key" =>
        val dStream = inputDStream.transform { rdd =>
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.map(record => {
            (record.key(), record.value())
          })
        }
        dStream
    }

  }

  override def checkConfig(): CheckResult = {

    config.hasPath("topics") match {
      case true => {
        val consumerConfig = TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
        consumerConfig.hasPath("group.id") &&
          !consumerConfig.getString("group.id").trim.isEmpty match {
          case true => new CheckResult(true, "")
          case false =>
            new CheckResult(false, "please specify [consumer.group.id] as non-empty string")
        }
      }
      case false => new CheckResult(false, "please specify [topics] as non-empty string, multiple topics separated by \",\"")
    }

    config.hasPath("key.type") match {
      case true => {
        config.getString("key.type") match {
          case "topic"|"key" => new CheckResult(true, "")
          case key_type => new CheckResult(false, "please specify [key.type] as non-empty string,allowed values: topic、key,now value:" + key_type)
        }
      }
      case false => new CheckResult(true, "")
    }

  }

  override def afterOutput(): Unit = {
    if (test) {
      return
    }
    inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    for (offsets <- offsetRanges) {
      val fromOffset = offsets.fromOffset
      val untilOffset = offsets.untilOffset
      if (untilOffset != fromOffset) {
        println(
          s"complete consume topic: ${offsets.topic} partition: ${offsets.partition} from ${fromOffset} until ${untilOffset}")
      }
    }
  }
}
