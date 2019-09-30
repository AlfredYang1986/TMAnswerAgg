package com.pharbers.TmAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

    test("test for tm new agg") {
        println("start")
        println(TmAggPreset2Cal.apply("5d57ed3cab0bf2192d416afb",
            "5d6e3772309d6c352ee6b4ec",
            "5d6e385e309d6c352ee6b54f", 1))
//        println(TmAggPreset2Cal.apply("5d57ed3cab0bf2192d416afb",
//            "5d57f2946db007183e2628e9",
//            "5d57f2946db007183e2628ea", 0))
    }

    // 导入预设数据
    test("test for tm new agg preset to show") {

        System.setProperty("ES_HOST", "pharbers.com")
        System.setProperty("ES_PORT", "9200")

        println("start")
        println(TmAggPreset2Show.apply(
            "5d79c5761c3bac3244ab93c6", //5d79c5761c3bac3244ab93c6 -> UCB // 5d79c5751c3bac3244ab9325 -> TM
            "5d6f742f153b3667418a2751",
            "5d6f742f153b3667418a2752",
            0
        ))
    }

    test("test for tm new agg report to show") {

        System.setProperty("ES_HOST", "pharbers.com")
        System.setProperty("ES_PORT", "9200")

        println("start")
        println(TmAggReport2Show.apply(
            "5d57ed3cab0bf2192d416afb",
            "5d6cf7c39197e2002b8c6b96",
            "5d6f742f153b3667418a2752",
            0
        ))
    }

    test("test for tm new agg cal_report to report") {
        println("start")
        println(TmAggCal2Report.apply(
            "2e5174a7-3d21-4f49-8210-80faf54cb20d", // "decd57f5-5595-4f0b-a98e-0ba5be49e462",
            "5d8c3a4c91baa60cf7acf132",
            "5d91d3f2827730002b441845",
            "5d91cc9f827730002b440fdb", 0))
    }

    test("test for tm new agg cal_report to report1") {
        println("start")
        println(TmAggCal2Report.apply(
            "cce5bcb6-68f0-45fe-be7c-3e7d42fc4976",
            "5d57ed3cab0bf2192d416afb",
            "5d59110df013b2055287813e",
            "5d59110df013b2055287813f", 0))
    }
}
