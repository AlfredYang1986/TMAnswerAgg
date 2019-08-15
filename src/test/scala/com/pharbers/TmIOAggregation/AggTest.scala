package com.pharbers.TmIOAggregation

import org.scalatest.FunSuite

class AggTest extends FunSuite {

    test("test for tm agg") {
        println("start")
        println(TmInputAgg("5d54b5e714c1120690c2b379",
            "5d54b5ef4dbe5b03c07adaab",
            "5d54b5ef4dbe5b03c07adaac"))
    }

    test("test for tm result agg") {
        println("start")
        println(TmResultAgg("3517b56b-8a39-4b2c-b4f8-0df5aea55e86"))
    }
}