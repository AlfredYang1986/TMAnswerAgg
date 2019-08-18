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
            "28aaf8d1-6f7b-4e40-a80c-e82f14d453da",
            "5d57ed3cab0bf2192d416afb",
            "5d57f2946db007183e2628e9",
            "5d57f2946db007183e2628ea", 0))
    }
}
