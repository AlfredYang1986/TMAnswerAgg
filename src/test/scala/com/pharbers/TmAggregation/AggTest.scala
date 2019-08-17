package com.pharbers.TmAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

//    test("test for tm new agg") {
//        println("start")
//        println(TmAggPreset2Cal.apply("5d57ed3cab0bf2192d416afb",
//            "5d57f2946db007183e2628e9",
//            "5d57f2946db007183e2628ea", 0))
//    }
//
//    test("test for tm new agg report to show") {
//        println("start")
//        println(TmAggReport2Show.apply("5d57ed3cab0bf2192d416afb",
//            "5d57f2946db007183e2628e9",
//            "5d57f2946db007183e2628ea", 0))
//    }

    test("test for tm new agg cal_report to report") {
        println("start")
        println(TmAggCal2Report.apply(
            "211d0d5f-46ca-4ea0-b40d-524d652d5e2c",
            "5d565f644c4b7a1b1d7fa7ff",
            "5d5661078a35ce1a2609b228",
            "5d5661078a35ce1a2609b229", 0))
    }
}
