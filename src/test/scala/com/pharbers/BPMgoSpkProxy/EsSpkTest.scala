package com.pharbers.BPMgoSpkProxy

import org.scalatest.FunSuite

class EsSpkTest extends FunSuite {
    test("extract data from es 2 spark") {
        println("start")

        System.setProperty("ES_HOST", "pharbers.com")
        System.setProperty("ES_PORT", "9200")

        BPEsSpkProxyImpl.loadDataFromSpark2Es("test-qi",
            List(Map("a" -> "1", "b" -> 2))
        )
    }
}
