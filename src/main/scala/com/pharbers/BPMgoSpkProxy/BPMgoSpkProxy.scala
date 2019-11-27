package com.pharbers.BPMgoSpkProxy

import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.pharbers.TmAggregation._
import org.apache.spark.sql.functions._

object BPMgoSpkProxyImpl {

    val yarnJars: String = "hdfs://spark.master:8020/jars/sparkJars"

    lazy val mongoHost = System.getProperty("MONGO_HOST")
    lazy val mongoPort = System.getProperty("MONGO_PORT")
    lazy val destDatabase = System.getProperty("MONGO_DEST")
	lazy val mongoUri = "mongodb://" + mongoHost + ":" + mongoPort + "/"

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
            "uri" -> mongoUri,
            "database" -> destDatabase,
            "collection" -> "cal"))

        val cal_data = spark.read.mongo(cal_data_rc).filter(col("job_id") === job_id).drop(col("_id"))
        cal_data.write.parquet("hdfs://192.168.100.137:8020/tmtest0831/jobs/" + job_id + "/input/" + "cal_data")
//        cal_data.write.json("hdfs://192.168.100.137:8020/tmtest0831/jobs/" + job_id + "/json/" + "cal_data")

        val cal_comp_rc = ReadConfig(Map(
            "uri" -> ("mongodb://" + mongoHost + ":" + mongoPort + "/"),
            "database" -> destDatabase,
            "collection" -> "cal_comp"))

        val cal_comp = spark.read.mongo(cal_comp_rc).filter(col("job_id") === job_id).drop(col("_id"))
        cal_comp.write.parquet("hdfs://192.168.100.137:8020/tmtest0831/jobs/" + job_id + "/input/" + "cal_comp")
//        cal_comp.write.json("hdfs://192.168.100.137:8020/tmtest0831/jobs/" + job_id + "/json/" + "cal_comp")

        job_id
    }

    def loadDataFromSpark2Mgo(
                                 jobId: String,
                                 proposalId: String,
                                 projectId: String,
                                 periodId: String,
                                 phase: Int = 0): Unit = {

        val spark = SparkSession.builder().config(conf).getOrCreate()
        val df_cal = spark.read.parquet("hdfs://192.168.100.137:8020/tmtest0831/jobs/" + jobId + "/output/" + "cal_report")
        df_cal.write
            .format("com.mongodb.spark.sql.DefaultSource")
            .option("spark.mongodb.output.uri", s"${mongoUri + destDatabase}")
            .option("collection", "cal_report")
            .mode("append")
            .save()

        val df_comp = spark.read.parquet("hdfs://192.168.100.137:8020/tmtest0831/jobs/" + jobId + "/output/" + "competitor")
        df_comp.write
            .format("com.mongodb.spark.sql.DefaultSource")
            .option("spark.mongodb.output.uri", s"${mongoUri + destDatabase}")
            .option("collection", "cal_competitor")
            .mode("append")
            .save()

        val df_summary = spark.read.parquet("hdfs://192.168.100.137:8020/tmtest0831/jobs/" + jobId + "/output/" + "summary")
        df_summary.write
            .format("com.mongodb.spark.sql.DefaultSource")
            .option("spark.mongodb.output.uri", s"${mongoUri + destDatabase}")
            .option("collection", "cal_FinalSummary")
            .mode("append")
            .save()

        TmAggCal2Report.apply(jobId, proposalId, projectId, periodId, phase)
    }
}
