package com.pharbers.TmAggregation

import com.mongodb.casbah.Imports._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggMongoOpt.aggCollEnum2Coll

package object TmAggCal2Report {
    def apply(
                 jobId: String,
                 proposalId: String,
                 projectId: String,
                 periodId: String,
                 phase: Int = 0): Unit = {

        val jobResult = calReportColl.find(DBObject("job_id" -> jobId)).toList

        val curPeriod = periodsColl.findOne(DBObject("_id" -> new ObjectId(periodId))).getOrElse(null)
        val curProposal = proposalsColl.findOne(DBObject("_id" -> new ObjectId(proposalId))).getOrElse(null)
        val curProject = projectsColl.findOne(DBObject("_id" -> new ObjectId(projectId))).getOrElse(null)

        val (hosps, products, resources) = (
            hospitalsColl.find(
                $or(curProposal.getAs[List[ObjectId]]("targets").get
                    .map(x => DBObject("_id" -> x)))).toList,
            productsColl.find(
                $or(curProposal.getAs[List[ObjectId]]("products").get
                    .map(x => DBObject("_id" -> x)))).toList,
            resourcesColl.find(
                $or(curProposal.getAs[List[ObjectId]]("resources").get
                    .map(x => DBObject("_id" -> x)))).toList
        )

        aggHospital(jobResult, hosps, products, resources, curProject, curPeriod, phase)
        aggRegion(jobResult, hosps, products, resources, curProject, curPeriod, phase)
    }

    def queryNumSafe(x: AnyRef): Double = {
        if (x == null) 0.0
        else x.toString.toDouble
    }

    def queryStringSafe(x: AnyRef): String = {
        if (x == null) ""
        else x.toString
    }

    def queryName(x: DBObject): String =
        x.get("hospital").toString + "##" +
            x.get("product").toString + "##" +
            x.get("representative").toString

    def aggReport(
                       results: List[DBObject],
                       hospitals: List[DBObject],
                       products: List[DBObject],
                       resources: List[DBObject],
                       project: DBObject,
                       period: DBObject,
                       phase: Int,
                       cat: String,
                       func: DBObject => String) = {

        val bulk = reportsColl.initializeOrderedBulkOperation

        results.groupBy(func(_)).foreach { it =>

            val items = it._2
            val (hn :: pn :: rn :: Nil) = queryName(it._2.head).split("##").toList
            val builder  = MongoDBObject.newBuilder
            builder += "phase" -> phase
            builder += "category" -> cat //"Hospital"
            builder += "hospital" -> hospitals.find(_.get("name") == hn).get._id
            builder += "product" -> products.find(_.get("name") == pn).get._id
            builder += "resource" -> resources.find(_.get("name") == rn).get._id
            builder += "region" -> queryStringSafe(items.head.get("city"))

            /**
              * 1. report 内容
              *  - sales
              *  - salesContri
              *  - salesQuota
              *  - quotaGrowthMOM
              *  - quotaContri
              *  - share
              *  - salesGrowthYOY
              *  - salesGrowthMOM
              *  - achievements
              */
            builder += "sales" -> items.map(x => queryNumSafe(x.get("sales"))).sum
            builder += "salesContri" -> items.map(x => queryNumSafe(x.get("sales"))).sum
            builder += "salesGrowthYOY" -> 0.0
            builder += "salesGrowthMOM" -> 0.0
            builder += "salesQuota" -> items.map(x => queryNumSafe(x.get("quota"))).sum
            builder += "quotaGrowthMOM" -> 0.0
            builder += "share" -> items.map(x => queryNumSafe(x.get("share"))).sum
            builder += "achievements" -> items.map(x => queryNumSafe(x.get("achievements"))).sum


            builder += "projectId" -> project._id.get.toString
            builder += "periodId" -> period._id.get.toString

            /**
              * 2. preset 内容
              *  - ytd
              *  - sales
              *  - share
              *  - patientNum
              *  - achievements
              *  - budget
              *  - initBudget
              */

            bulk.insert(builder.result)
        }

        bulk.execute()
    }

    def aggHospital(
                       results: List[DBObject],
                       hospitals: List[DBObject],
                       products: List[DBObject],
                       resources: List[DBObject],
                       project: DBObject,
                       period: DBObject,
                       phase: Int) = {

        aggReport(
            results, hospitals, products, resources,
            project, period, phase, "Hospital",
            (res) => {
                res.get("hospital").toString + "##" +
                    res.get("product").toString + "##" +
                    res.get("representative").toString
        })
    }

    def aggRegion(
                       results: List[DBObject],
                       hospitals: List[DBObject],
                       products: List[DBObject],
                       resources: List[DBObject],
                       project: DBObject,
                       period: DBObject,
                       phase: Int) = {

        aggReport(
            results, hospitals, products, resources,
            project, period, phase, "Region",
            (res) => {
                queryStringSafe(res.get("city")) + "##" +
                    res.get("product").toString
            })
    }
}
