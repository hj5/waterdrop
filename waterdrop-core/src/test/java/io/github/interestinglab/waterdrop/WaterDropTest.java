package io.github.interestinglab.waterdrop;

import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author jian.huang
 * @version 5.3
 * 2021/2/22
 */
public class WaterDropTest {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf()
//                .setAppName("spark-phoenix")
//                .setMaster("local");
//        SparkContext sc = new SparkContext(conf);
//         spark = new HiveContext(sc);
        SparkSession spark = SparkSession.builder().appName("SparkSQLJoinDemo").master("local[*]").getOrCreate();

//        String config = "env{}\n transform{}\n source {\n" + "  hive {\n" + "    pre_sql = \"select * from alex1\"\n"
//                + "    result_table_name = \"alex1\"\n" + "  }\n" + "}\n" + "sink {\n" + "  elasticsearch {\n"
//                + "      hosts = [\"172.24.5.212:9200\"]\n" + "      index = \"waterdrop\"\n"
//                + "      es.mapping.id = \"id\"\n" + "      es.batch.size.entries = 100000\n"
//                + "      index_time_format = \"yyyy.MM.dd\"\n" + "  }\n" + "}";
        String config = "######\n" +
                "###### This config file is a demonstration of batch processing in waterdrop config\n" +
                "######\n" +
                "\n" +
                "env {\n" +
                "  # You can set spark configuration here\n" +
                "  # see available properties defined by spark: https://spark.apache.org/docs/latest/configuration.html#available-properties\n" +
                "  spark.app.name = \"Waterdrop-hive2\"\n" +
                "  spark.executor.instances = 1\n" +
                "  spark.executor.cores = 1\n" +
                "  spark.executor.memory = \"1g\"\n" +
                "  #spark.sql.catalogImplementation = \"hive\"\n" +
                "  #spark.sql.catalogImplementation = \"in-memory\"\n" +
                "}\n" +
                "\n" +
                "source {\n" +
                "  # This is a example input plugin **only for test and demonstrate the feature input plugin**\n" +
                "  # Fake {\n" +
                "  #   result_table_name = \"my_dataset\"\n" +
                "  # }\n" +
                "\n" +
                "  #hive\n" +
                "  #hive {\n" +
                "  #  pre_sql = \"select * from alex1 where name='\"${city}\"'\"\n" +
                "  #  result_table_name = \"alex1\"\n" +
                "  #}\n" +
                "\n" +
                "  # You can also use other input plugins, such as hdfs\n" +
                "  # hdfs {\n" +
                "  #   result_table_name = \"accesslog\"\n" +
                "  #   path = \"hdfs://hadoop-cluster-01/nginx/accesslog\"\n" +
                "  #   format = \"json\"\n" +
                "  # }\n" +
                "  #hdfs {\n" +
                "  #     result_table_name = \"accesslog\"\n" +
                "  #     path = \"hdfs://bfd-haifeng--1.novalocal:8020/tmp/flink/hello.txt\"\n" +
                "  #     format = \"text\"\n" +
                "  #}\n" +
                "  hdfs {\n" +
                "         result_table_name = \"accesslog\"\n" +
                "         path = \"hdfs://172.24.3.173:25006/tmp/abc/aa.txt\"\n" +
                "         format = \"text\"\n" +
                "         kerberos.keytab = \"/Users/hj/Documents/workspace/bdos/waterdrop/plugin-spark-sink-file/src/main/resources/presto.keytab\"\n" +
                "         options.fs.defaultFS = \"hdfs://172.24.3.173:25006\"\n" +
                "         options.hadoop.security.authentication = \"kerberos\"\n" +
                "         options.hadoop.security.authorization = \"true\"\n" +
                "         options.hadoop.rpc.protection = \"privacy\"\n" +
                "         options.dfs.namenode.kerberos.principal = \"hdfs/hadoop.hadoop.com@HADOOP.COM\"\n" +
                "         options.dfs.datanode.kerberos.https.principal=\"hdfs/hadoop.hadoop.com@HADOOP.COM\"\n" +
                "         options.dfs.datanode.kerberos.principal = \"hdfs/hadoop.hadoop.com@HADOOP.COM\"\n" +
                "  }\n" +
                "\n" +
                "  # If you would like to get more information about how to configure waterdrop and see full list of input plugins,\n" +
                "  # please go to https://interestinglab.github.io/waterdrop/#/zh-cn/configuration/base\n" +
                "}\n" +
                "\n" +
                "transform {\n" +
                "  # split data by specific delimiter\n" +
                "\n" +
                "  # you can also you other filter plugins, such as sql\n" +
                "  # sql {\n" +
                "  #   sql = \"select * from accesslog where request_time > 1000\"\n" +
                "  # }\n" +
                "\n" +
                "  # If you would like to get more information about how to configure waterdrop and see full list of filter plugins,\n" +
                "  # please go to https://interestinglab.github.io/waterdrop/#/zh-cn/configuration/base\n" +
                "}\n" +
                "\n" +
                "sink {\n" +
                "  # choose stdout output plugin to output data to console\n" +
                "  Console {\n" +
                "  #  soure_table_name = \"alex1\"\n" +
                "  }\n" +
                "\n" +
                "  #elasticsearch {\n" +
                "  #    hosts = [\"172.24.5.212:9200\"]\n" +
                "  #    #index = \"waterdrop-${now}\"\n" +
                "  #    index = \"waterdrop\"\n" +
                "  #    es.mapping.id = \"id\"\n" +
                "  #    es.batch.size.entries = 100000\n" +
                "  #    index_time_format = \"yyyy.MM.dd\"\n" +
                "  #}\n" +
                "\n" +
                "\n" +
                "\n" +
                "  # you can also you other output plugins, such as sql\n" +
                "  # hdfs {\n" +
                "  #   path = \"hdfs://hadoop-cluster-01/nginx/accesslog_processed\"\n" +
                "  #   save_mode = \"append\"\n" +
                "  # }\n" +
                "  #hdfs {\n" +
                "  #     path = \"hdfs://172.24.3.173:25006/tmp/demo\"\n" +
                "  #     save_mode = \"append\",\n" +
                "  #     serializer = \"text\"\n" +
                "  #     partition_by = []\n" +
                "  #     path_time_format = \"yyyyMMddHHmmss\"\n" +
                "  #     kerberos.keytab = \"/Users/hj/Documents/workspace/bdos/waterdrop/plugin-spark-sink-file/src/main/resources/presto.keytab\"\n" +
                "  #}\n" +
                "\n" +
                "  # If you would like to get more information about how to configure waterdrop and see full list of output plugins,\n" +
                "  # please go to https://interestinglab.github.io/waterdrop/#/zh-cn/configuration/base\n" +
                "}\n";
        String variable = "city=beijing";
        Waterdrop.exec(spark,config,variable);
    }
}
