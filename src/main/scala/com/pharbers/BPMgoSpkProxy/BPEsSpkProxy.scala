package com.pharbers.BPMgoSpkProxy

import com.pharbers.TmAggregation.TmAggReport2Show
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.{DefaultHttpClient, HttpClients}

object BPEsSpkProxyImpl {
    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"

    lazy val esHost: String = System.getProperty("ES_HOST")
    lazy val esPort: String = System.getProperty("ES_PORT")
    lazy val esIndex: String = "tmrs_new"

    private val conf = new SparkConf()
            .set("spark.yarn.jars", yarnJars)
            .set("spark.yarn.archive", yarnJars)
            .setAppName("es-extractor")
            .setMaster("yarn")
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.executor.memory", "1g")
            .set("spark.executor.cores", "1")
            .set("spark.executor.instances", "1")

    def save2Es(index: String, data: List[Map[String, Any]]): Unit = {
        conf.set("es.nodes.wan.only", "true")
        conf.set("es.pushdown", "true")
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", esHost)
        conf.set("es.port", esPort)

        val ss = SparkSession.builder().config(conf).getOrCreate()
        ss.sparkContext.addJar("hdfs://spark.master:9000/jars/context/elasticsearch-hadoop-7.2.0.jar")
        ss.sparkContext.makeRDD(data).saveToEs(index)
    }

    def loadDataFromEs2Spark(): Unit = {

    }

    def deleteEsByProjectId(projectId: String): Unit = {
        val url = s"http://$esHost:$esPort/$esIndex/_delete_by_query?q=project_id.keyword:$projectId";
        HttpClients.createDefault().execute(new HttpPost(url))
    }

    def loadDataFromSpark2Es(proposalId: String,
                             projectId: String,
                             periodId: String,
                             phase: Int = 0): Unit = {
        conf.set("es.nodes.wan.only", "true")
        conf.set("es.pushdown", "true")
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", esHost)
        conf.set("es.port", esPort)

        deleteEsByProjectId(projectId)
        val data = TmAggReport2Show.apply(proposalId, projectId, periodId, phase)
        val ss = SparkSession.builder().config(conf).getOrCreate()
        ss.sparkContext.makeRDD(data).saveToEs(esIndex)
    }
}
