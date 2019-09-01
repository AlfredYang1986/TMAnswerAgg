package com.pharbers.BPMgoSpkProxy

import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.config.ReadConfig

class BPMgoSpkProxy {

    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"

    val mongoHost = System.getProperty("MONGO_HOST")
    val mongoPort = System.getProperty("MONGO_PORT")
    val destDatabase = System.getProperty("MONGO_DEST")

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
            "uri" -> ("mongodb://" + mongoHost + ":" + mongoPort + "/"),
            "database" -> destDatabase,
            "collection" -> "presets"))

        spark.read.mongo(readConfig) // 2)
    }
}
