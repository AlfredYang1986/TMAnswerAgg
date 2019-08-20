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
    val calCompetitorColl = "cal_competitor"

    val showReportColl = "show_report"
    val calFinalResult = "cal_FinalSummary"
    val finals = "finals"
    
    val calData1 = "cal_data1"
    val calData2 = "cal_data2"
}
