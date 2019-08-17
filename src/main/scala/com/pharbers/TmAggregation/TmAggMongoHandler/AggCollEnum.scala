package com.pharbers.TmAggregation.TmAggMongoHandler

object AggCollEnum extends Enumeration {
    type AggCollEnum = String

    val proposalsColl = "proposals"
    val projectsColl = "projects"
    val periodsColl = "periods"
    val hospitalsColl = "hospitals"
    val productsColl = "products"
    val resourcesColl = "resources"
    val presetsColl = "presets"
    val reportsColl = "reports"
    val answersColl = "answers"

    val calColl = "cal"
    val calCompColl = "cal_comp"
    val calReportColl = "cal_report"
}
