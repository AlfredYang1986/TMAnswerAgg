package com.pharbers.BPMgoSpkProxy

import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.config.ReadConfig
import com.pharbers.TmAggregation.TmAggPreset2Cal
import org.apache.spark.sql.functions._

object BPMgoSpkProxyImpl {

    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"

//    lazy val mongoHost = System.getProperty("MONGO_HOST")
//    lazy val mongoPort = System.getProperty("MONGO_PORT")
//    lazy val destDatabase = System.getProperty("MONGO_DEST")
    lazy val mongoHost = "pharbers.com"
    lazy val mongoPort = "5555"
    lazy val destDatabase = "pharbers-ntm-client"

    private val conf = new SparkConf()
        .set("spark.yarn.jars", yarnJars)
        .set("spark.yarn.archive", yarnJars)
        .setAppName("mongo-extractor")
        .setMaster("yarn")
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.executor.memory", "1g")
        .set("spark.executor.cores", "1")
        .set("spark.executor.instances", "1")

    def loadDataFromMgo2Spark(
                                 proposalId: String,
                                 projectId: String,
                                 periodId: String,
                                 phase: Int = 0): String = {

        val job_id = TmAggPreset2Cal.apply(proposalId, projectId, periodId, phase)

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val cal_data_rc = ReadConfig(Map(
            "uri" -> ("mongodb://" + mongoHost + ":" + mongoPort + "/"),
            "database" -> destDatabase,
            "collection" -> "cal"))

        val cal_data = spark.read.mongo(cal_data_rc).filter(col("job_id") === job_id).drop(col("_id"))
        cal_data.show(100)
        cal_data.write.parquet("hdfs://192.168.100.137:9000/tmtest0831/jobs/" + job_id + "/input/" + "cal_data")
//        cal_data.write.json("hdfs://192.168.100.137:9000/tmtest0831/jobs/" + job_id + "/json/" + "cal_data")

        val cal_comp_rc = ReadConfig(Map(
            "uri" -> ("mongodb://" + mongoHost + ":" + mongoPort + "/"),
            "database" -> destDatabase,
            "collection" -> "cal_comp"))

        val cal_comp = spark.read.mongo(cal_comp_rc).filter(col("job_id") === job_id).drop(col("_id"))
        cal_comp.show(100)
        cal_comp.write.parquet("hdfs://192.168.100.137:9000/tmtest0831/jobs/" + job_id + "/input/" + "cal_comp")
//        cal_comp.write.json("hdfs://192.168.100.137:9000/tmtest0831/jobs/" + job_id + "/json/" + "cal_comp")

        job_id
    }
}
