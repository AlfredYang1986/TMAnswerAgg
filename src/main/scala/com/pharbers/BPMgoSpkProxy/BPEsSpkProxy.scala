package com.pharbers.BPMgoSpkProxy

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

object BPEsSpkProxyImpl {
    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"

    lazy val esHost: String = System.getProperty("ES_HOST")
    lazy val esPort: String = System.getProperty("ES_PORT")

    private val conf = new SparkConf()
            .set("spark.yarn.jars", yarnJars)
            .set("spark.yarn.archive", yarnJars)
            .setAppName("es-extractor")
            .setMaster("yarn")
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.executor.memory", "1g")
            .set("spark.executor.cores", "1")
            .set("spark.executor.instances", "1")

    def loadDataFromEs2Spark(): Unit = {

    }

    def loadDataFromSpark2Es(index: String, data: List[Map[String, Any]]): Unit = {
        conf.set("es.nodes.wan.only", "true")
        conf.set("es.pushdown", "true")
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", esHost)
        conf.set("es.port", esPort)
        val ss = SparkSession.builder().config(conf).getOrCreate()
        // TODO 发布时删除
        ss.sparkContext.addJar("hdfs://spark.master:9000/jars/context/elasticsearch-hadoop-7.2.0.jar")
        ss.sparkContext.makeRDD(data).saveToEs(index)
    }
}
