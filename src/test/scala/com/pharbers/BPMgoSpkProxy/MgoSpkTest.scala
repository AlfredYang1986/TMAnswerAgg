package com.pharbers.BPMgoSpkProxy

import org.scalatest.FunSuite

class MgoSpkTest extends FunSuite {
    test("extract data from mongo 2 spark") {
        println("start")

        System.setProperty("MONGO_HOST", "192.168.100.14")
        System.setProperty("MONGO_PORT", "30010")
        System.setProperty("MONGO_DEST", "pharbers-ntm-client")

        val result = BPMgoSpkProxyImpl.loadDataFromMgo2Spark(
            "5d57ed3cab0bf2192d416afb",
            "5d6baed5b83d06c919a7d7b1",
            "5d6baed5b83d06c919a7d7b2",
            0
        )
        println(result)
    }
}