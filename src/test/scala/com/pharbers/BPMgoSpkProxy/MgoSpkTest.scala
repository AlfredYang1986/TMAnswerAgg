package com.pharbers.BPMgoSpkProxy

import org.scalatest.FunSuite

class MgoSpkTest extends FunSuite {
    test("extract data from mongo 2 spark") {
        println("start")

        val mongoHost = System.setProperty("MONGO_HOST", "pharbers.com")
        val mongoPort = System.setProperty("MONGO_PORT", "5555")
        val destDatabase = System.setProperty("MONGO_DEST", "pharbers-ntm-client")

        val proxy = new BPMgoSpkProxy
        val result = proxy.loadDataFromMgo2Spark()
        result.printSchema()
        result.show()
    }
}