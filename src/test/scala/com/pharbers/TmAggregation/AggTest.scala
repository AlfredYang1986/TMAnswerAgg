package com.pharbers.TmAggregation

import com.mongodb.casbah.Imports.{DBObject, ObjectId}
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum.periodsColl
import org.scalatest.FunSuite
import com.mongodb.casbah.Imports._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggMongoOpt.aggCollEnum2Coll

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
            "f95dd4ba-f597-4a82-a4ca-56930c652f1f",
            "5d57ed3cab0bf2192d416afb",
            "5d68ffd9d0b16f03ef581e43",
            "5d68ffdad0b16f03ef581e44", 2))
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
