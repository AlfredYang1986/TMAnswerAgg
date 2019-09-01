package com.pharbers.TmAggregation

import java.util.UUID

import com.mongodb.casbah.Imports._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggMongoOpt.aggCollEnum2Coll

package object TmAggReport2Show {
    def apply(
                 proposalId: String,
                 projectId: String,
                 periodId: String,
                 phase: Int = 0): String = {

        val jobId = UUID.randomUUID().toString

//        val curPeriod = periodsColl.findOne(DBObject("_id" -> new ObjectId(periodId))).getOrElse(null)
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

        val bulk = showReportColl.initializeOrderedBulkOperation
        loadCurrentReport(curProject, curProposal, phase).foreach { x =>
            val builder = MongoDBObject.newBuilder
            builder += "job_id" -> jobId
            bulk.insert(periodPresetReport(hosps, products, resources, jobId, x))
        }
        loadCurrentPreset(curProject, curProposal, phase).foreach { x =>
            val builder = MongoDBObject.newBuilder
            builder += "job_id" -> jobId
            bulk.insert(periodAbilityReport(hosps, products, resources, jobId, x))
        }
        bulk.execute()
        jobId

    }

    def loadCurrentReport(project: DBObject, proposal: DBObject, phase: Int): List[DBObject] = {
        val condi = ("phase" $lt phase) ++ ("proposalId" -> proposal._id.get.toString) ++ ("projectId" -> "")
        val condi01 = ("phase" $lt phase) ++ ("projectId" -> project._id.get.toString)
        reportsColl.find($or(condi :: condi01 :: Nil)).toList
    }


    def loadCurrentPreset(project: DBObject, proposal: DBObject, phase: Int): List[DBObject] = {
        val condi = ("phase" $lte phase) ++ ("proposalId" -> proposal._id.get.toString) ++ ("category" -> 2) ++ ("projectId" -> "")
        val condi01 = ("phase" $lte phase) ++ ("projectId" -> project._id.get.toString) ++ ("category" -> 2)
        presetsColl.find($or(condi :: condi01 :: Nil)).toList
    }


    def queryNumSafe(x: AnyRef): Double = {
        if (x == null) 0.0
        else x.toString.toDouble
    }

    def periodAbilityReport(
                              hosps: List[DBObject],
                              products: List[DBObject],
                              resources: List[DBObject],
                              jobId: String,
                              preset: DBObject): DBObject = {

        val builder = MongoDBObject.newBuilder

        builder += "job_id" -> jobId
        builder += "category" -> "Ability"
        builder += "share" -> 0.0

        builder += "sales" -> 0.0
        builder += "quota" -> 0.0
        builder += "budget" -> 0.0
        builder += "potential" -> 0.0
        builder += "phase" -> preset.get("phase")

        resources.find( x => x.get("_id") == preset.get("resource")) match {
            case Some(r) => {
                builder += "representative" -> r.get("name")
                builder += "representative_time" -> 0.0

                builder += "work_motivation" -> queryNumSafe(preset.get("workMotivation"))
                builder += "territory_management_ability" -> queryNumSafe(preset.get("territoryManagementAbility"))
                builder += "sales_skills" -> queryNumSafe(preset.get("salesSkills"))
                builder += "product_knowledge" -> queryNumSafe(preset.get("productKnowledge"))
                builder += "behavior_efficiency" -> queryNumSafe(preset.get("behaviorEfficiency"))
            }
            case None => {
                builder += "representative" -> ""
                builder += "representative_time" -> 0.0

                builder += "work_motivation" -> 0.0
                builder += "territory_management_ability" -> 0.0
                builder += "sales_skills" -> 0.0
                builder += "product_knowledge" -> 0.0
                builder += "behavior_efficiency" -> 0.0
            }
        }

        builder.result()
    }

    def periodPresetReport(
                              hosps: List[DBObject],
                              products: List[DBObject],
                              resources: List[DBObject],
                              jobId: String,
                              report: DBObject): DBObject = {
        val builder = MongoDBObject.newBuilder

        builder += "job_id" -> jobId
        builder += "category" -> report.get("category")
        builder += "share" -> queryNumSafe(report.get("share"))

        builder += "sales" -> queryNumSafe(report.get("sales"))
        builder += "quota" -> queryNumSafe(report.get("salesQuota"))
        builder += "budget" -> 0.0
        builder += "potential" -> queryNumSafe(report.get("potential"))
        builder += "phase" -> report.get("phase")

        hosps.find(_.get("_id") == report.get("hospital")) match {
            case Some(h) => {
                builder += "hospital" -> h.get("name")
                builder += "hospital_level" -> h.get("level")
                builder += "region" -> h.get("position")
            }
            case None => {
                builder += "hospital" -> ""
                builder += "hospital_level" -> ""
                builder += "region" -> report.get("region")
            }
        }

        products.find(_.get("_id") == report.get("product")) match {
            case Some(p) => {
                builder += "product" -> p.get("name")
                builder += "life_cycle" -> p.get("lifeCycle")
            }
            case None => {
                builder += "product" -> ""
                builder += "life_cycle" -> ""
            }
        }

        resources.find( x => x.get("_id") == report.get("resource")) match {
            case Some(r) => {
                builder += "representative" -> r.get("name")
                builder += "representative_time" -> 0.0
            }
            case None => {
                builder += "representative" -> ""
                builder += "representative_time" -> 0.0
            }
        }

        builder += "work_motivation" -> 0.0
        builder += "territory_management_ability" -> 0.0
        builder += "sales_skills" -> 0.0
        builder += "product_knowledge" -> 0.0
        builder += "behavior_efficiency" -> 0.0

        products.find( x => x.get("_id") == report.get("product")) match {
            case Some(r) => {
                builder += "product_area" -> r.get("treatmentArea")
                builder += "status" -> (if (r.get("name") == "开拓来") "已开发" else "未开发")
            }
            case None => {
                builder += "product_area" -> ""
                builder += "status" -> ""
            }
        }

        builder.result()
    }
}
