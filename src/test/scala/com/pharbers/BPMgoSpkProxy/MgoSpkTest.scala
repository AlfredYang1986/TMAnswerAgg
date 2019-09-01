package com.pharbers.BPMgoSpkProxy

import org.scalatest.FunSuite

class MgoSpkTest extends FunSuite {
    test("extract data from mongo 2 spark") {
        println("start")

        System.setProperty("MONGO_HOST", "pharbers.com")
        System.setProperty("MONGO_PORT", "5555")
        System.setProperty("MONGO_DEST", "pharbers-ntm-client")

        val proxy = new BPMgoSpkProxy
        val result = proxy.loadDataFromMgo2Spark(
            "5d57ed3cab0bf2192d416afb",
            "5d6b7cb3744610c15e0cf474",
            "5d6b7cb3744610c15e0cf475",
            0
        )
        println(result)
    }
}