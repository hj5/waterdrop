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
        String config = "env{}\n transform{}\n source {\n" + "  hive {\n" + "    pre_sql = \"select * from alex1\"\n"
                + "    result_table_name = \"alex1\"\n" + "  }\n" + "}\n" + "sink {\n" + "  elasticsearch {\n"
                + "      hosts = [\"172.24.5.212:9200\"]\n" + "      index = \"waterdrop\"\n"
                + "      es.mapping.id = \"id\"\n" + "      es.batch.size.entries = 100000\n"
                + "      index_time_format = \"yyyy.MM.dd\"\n" + "  }\n" + "}";
        String variable = "city=beijing";
        Waterdrop.exec(spark,config,variable);
    }
}
