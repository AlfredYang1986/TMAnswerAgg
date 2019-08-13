package com.pharbers.TmIOAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

    test("test for tm agg") {
        println("start")
        println(TmInputAgg("5cc54aa3eeefcc04515c46cb",
            "5d316f8879609b0f2a6f568e",
            "5d346f2950ae3610a5fc70b2"))
    }

    test("test for tm result agg") {
        println("start")
        println(TmResultAgg("3517b56b-8a39-4b2c-b4f8-0df5aea55e86"))
    }
}