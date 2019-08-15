package com.pharbers.NTMIOAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

    test("test for tm agg") {
        println("start")
        println(TmInputAgg("5d5515ae6573f7239ad3cb0f",
            "5d5515eb12563c156b04f8d5",
            "5d5515eb12563c156b04f8d6"))
    }

    test("test for tm report") {
        println("start")
        println(TmReportAgg("5d5555af2b6f2830b93bfd92",
            "",
            ""))
    }

//    test("test for tm result agg") {
//        println("start")
//        println(TmResultAgg("3517b56b-8a39-4b2c-b4f8-0df5aea55e86"))
//    }
}