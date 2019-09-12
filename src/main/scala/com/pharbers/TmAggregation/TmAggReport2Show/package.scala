package com.pharbers.TmAggregation

import java.util.UUID

import com.mongodb.casbah.Imports._
import com.pharbers.BPMgoSpkProxy.BPEsSpkProxyImpl
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggMongoOpt.aggCollEnum2Coll

import scala.collection.mutable

package object TmAggReport2Show {
    def apply(
                 proposalId: String,
                 projectId: String,
                 periodId: String,
                 phase: Int = 0): List[Map[String, Any]] = {

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

//        val bulk = showReportColl.initializeOrderedBulkOperation
//        loadCurrentReport(curProject, curProposal, phase).map { x =>
//            val builder = MongoDBObject.newBuilder
//            builder += "job_id" -> showId
//            bulk.insert(periodPresetReport(hosps, products, resources, showId, x))
//        }
//        loadCurrentPreset(curProject, curProposal, phase).map { x =>
//            val builder = MongoDBObject.newBuilder
//            builder += "job_id" -> showId
//            bulk.insert(periodAbilityReport(hosps, products, resources, showId, x))
//        }
//        bulk.execute()

        val presetReports = loadCurrentReport(curProject, curProposal, phase).map { x =>
            periodPresetReport(projectId, hosps, products, resources, x)
        }
        val abilityReports = loadCurrentPreset(curProject, curProposal, phase).map { x =>
            periodAbilityReport(projectId, hosps, products, resources, x)
        }

        val result = presetReports ::: abilityReports ::: Nil

        // 院外
        val in = result.filter(p => List("会南市五零一医院", "省人民医院", "会东市医科大学附属第二医院").contains(p.get("hospital"))).map { x =>
            val builder = MongoDBObject.newBuilder
            builder ++= x
            builder += "hospital_level" -> "院外"
            builder += "sales" -> x.getAs[Double]("sales").get / 2
            builder.result()
        }

        // 院内
        val ino = result.filter(p => List("会南市五零一医院", "省人民医院", "会东市医科大学附属第二医院").contains(p.get("hospital"))).map { x =>
            val builder = MongoDBObject.newBuilder
            builder ++= x
            builder += "sales" -> x.getAs[Double]("sales").get / 2
            builder.result()
        }

        // 其他
        val out = result.filter(p => !List("会南市五零一医院", "省人民医院", "会东市医科大学附属第二医院").contains(p.get("hospital")))

        (in ::: ino ::: out).map { x =>
            val tmp = collection.immutable.Map.newBuilder[String, Any]
            x.toSeq.map (y => tmp += (y._1 -> y._2))
            tmp.result()
        }
    }

    def loadCurrentReport(project: DBObject, proposal: DBObject, phase: Int): List[DBObject] = {
        val condi01 = ("phase" $eq phase) ++ ("projectId" -> project._id.get.toString)
        reportsColl.find(condi01).toList
    }


    def loadCurrentPreset(project: DBObject, proposal: DBObject, phase: Int): List[DBObject] = {
       val condi01 = ("phase" $eq phase) ++ ("projectId" -> project._id.get.toString) ++ ("category" -> 2)
        presetsColl.find(condi01).toList
    }


    def queryNumSafe(x: AnyRef): Double = {
        if (x == null) 0.0
        else x.toString.toDouble
    }

    def periodAbilityReport(projectId: String,
                              hosps: List[DBObject],
                              products: List[DBObject],
                              resources: List[DBObject],
                              preset: DBObject): DBObject = {

        val builder = MongoDBObject.newBuilder

        builder += "project_id" -> projectId
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

    def periodPresetReport(projectId: String,
                           hosps: List[DBObject],
                           products: List[DBObject],
                           resources: List[DBObject],
                           report: DBObject): DBObject = {
        val builder = MongoDBObject.newBuilder

        builder += "project_id" -> projectId
        builder += "category" -> report.get("category")
        builder += "share" -> queryNumSafe(report.get("share"))

        builder += "sales" -> queryNumSafe(report.get("sales"))
        builder += "quota" -> queryNumSafe(report.get("salesQuota"))
        builder += "budget" -> 0.0
        builder += "potential" -> queryNumSafe(report.get("potential"))
        builder += "phase" -> report.get("phase")
        builder += "status" -> report.get("drugEntrance")
        builder += "currentPatientNum" -> report.get("patientNum")

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
                builder += "product_type" -> p.get("productType")
                builder += "life_cycle" -> p.get("lifeCycle")
            }
            case None => {
                builder += "product" -> ""
                builder += "product_type" -> 2
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
            }
            case None => {
                builder += "product_area" -> ""
            }
        }

        builder.result()
    }
}
