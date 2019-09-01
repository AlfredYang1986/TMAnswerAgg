package com.pharbers.BPMgoSpkProxy

import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class BPMgoSpkProxy {

    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"

    private val conf = new SparkConf()
        .set("spark.yarn.jars", yarnJars)
        .set("spark.yarn.archive", yarnJars)
        .setAppName("mongo-extractor")
        .setMaster("yarn")
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.executor.memory", "1g")
        .set("spark.executor.cores", "1")
        .set("spark.executor.instances", "1")

    def loadDataFromMgo2Spark(): DataFrame = {
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val readConfig = ReadConfig(Map(
            "uri" -> "mongodb://192.168.100.115/",
            "database" -> "pharbers-ntm-client",
            "collection" -> "presets")) // 1)

        spark.read.mongo(readConfig) // 2)
    }
}
