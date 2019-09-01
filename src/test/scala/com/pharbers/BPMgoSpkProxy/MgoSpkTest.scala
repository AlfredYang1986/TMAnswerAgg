package com.pharbers.BPMgoSpkProxy

import org.scalatest.FunSuite

class MgoSpkTest extends FunSuite {
    test("extract data from mongo 2 spark") {
        println("start")
        val proxy = new BPMgoSpkProxy
        val result = proxy.loadDataFromMgo2Spark()
        result.printSchema()
        result.show()
    }
}