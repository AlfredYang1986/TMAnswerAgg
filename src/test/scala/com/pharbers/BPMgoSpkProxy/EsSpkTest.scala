package com.pharbers.BPMgoSpkProxy

import org.scalatest.FunSuite

class EsSpkTest extends FunSuite {
    test("extract data from spark 2 es") {
        println("start")

        System.setProperty("ES_HOST", "pharbers.com")
        System.setProperty("ES_PORT", "9200")

//        BPEsSpkProxyImpl.deleteEsByCond("5d7a2b49b18904003f20b538", 2)

        val result = BPEsSpkProxyImpl.loadDataFromSpark2Es(
            "5d57ed3cab0bf2192d416afb",
            "5d6cf7c39197e2002b8c6b96",
            "5d6f742f153b3667418a2752",
            0
        )
        println(result)
    }
}
