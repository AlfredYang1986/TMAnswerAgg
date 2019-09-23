package com.pharbers.TmAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

    test("test for tm new agg") {
        println("start")
        println(TmAggPreset2Cal.apply("5d7718b861ca7408f172c8c8",
            "5d771b10e893ec02fe39b3f7",
            "5d771c3ee893ec02fe39b45a", 1))
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
            "156e8a81-3d97-48ac-8891-2d86c011680d", // "decd57f5-5595-4f0b-a98e-0ba5be49e462",
            "5d81fe2d8f807ab15afea035",
            "5d882818cd6d65002bf8ada4",
            "5d882b3bcd6d65002bf8ae07", 1))
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
