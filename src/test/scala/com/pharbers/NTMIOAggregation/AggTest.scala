package com.pharbers.NTMIOAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

    test("test for tm agg") {
        println("start")
        println(TmInputAgg("5d563e6245c8d71248eaa8a4",
            "5d563e6fa659e6103b435534",
            "5d563e6fa659e6103b435535"))
    }

    test("test for tm report") {
        println("start")
        println(TmReportAgg("5d5606e7c388c003ed91967a",
            "",
            ""))
    }

//    test("test for tm result agg") {
//        println("start")
//        println(TmResultAgg("3517b56b-8a39-4b2c-b4f8-0df5aea55e86"))
//    }
}