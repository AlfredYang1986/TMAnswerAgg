package com.pharbers.TmAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

    test("test for tm new agg") {
        println("start")
        println(TmAggPreset2Cal.apply("5d57ed3cab0bf2192d416afb",
            "5d6b86e9700931c3330dac04",
            "5d6b8a01700931c3330dac67", 1))
//        println(TmAggPreset2Cal.apply("5d57ed3cab0bf2192d416afb",
//            "5d57f2946db007183e2628e9",
//            "5d57f2946db007183e2628ea", 0))
    }

    test("test for tm new agg report to show") {
        println("start")
        println(TmAggReport2Show.apply("5d57ed3cab0bf2192d416afb",
            "5d57f2946db007183e2628e9",
            "5d57f2946db007183e2628ea", 0))
    }

    test("test for tm new agg cal_report to report") {
        println("start")
        println(TmAggCal2Report.apply(
            "0ff568e7-3e1a-48df-9ab0-d90f88130614",
            "5d57ed3cab0bf2192d416afb",
            "5d5b60f7c9f8a1002b9546a6",
            "5d5be64552942e22fdd09cbe", 0))
    }

    test("test for tm new agg cal_report to report1") {
        println("start")
        println(TmAggCal2Report.apply(
            "7b5cdf82-70e1-4026-8aa8-5296cb48bbca",
            "5d57ed3cab0bf2192d416afb",
            "5d59110df013b2055287813e",
            "5d59110df013b2055287813f", 0))
    }
}
