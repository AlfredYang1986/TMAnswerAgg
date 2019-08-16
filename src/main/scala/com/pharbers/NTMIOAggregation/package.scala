package com.pharbers

import java.util.UUID

import com.mongodb.casbah.Imports._

import scala.collection.immutable

package object NTMIOAggregation {
    val mongodbHost = "192.168.100.176"
    val mongodbPort = 27017
    val mongodbUsername = ""
    val mongodbPassword = ""
    val ntmDBName = "pharbers-ntm-client"
    val answerCollName = "answers"
    val presetCollName = "presets"
    val periodCollName = "periods"
    val projectCollName = "projects"
    val proposalCollName = "proposals"
    val hospCollName = "hospitals"
    val prodCollName = "products"
    val resCollName = "resources"
    val calCollName = "cal"
    val calCompCollName = "cal_comp"
    val calReportCollName = "cal_report"
    val reportCollName = "reports"
    val showReportCollName = "show_reports"

    lazy val client : MongoClient = MongoClient(mongodbHost, mongodbPort)
    lazy val db = client(ntmDBName)
    lazy val collAnswer = db(answerCollName)
    lazy val collPreset = db(presetCollName)
    lazy val collPeriod = db(periodCollName)
    lazy val collProject = db(projectCollName)
    lazy val collProposal = db(proposalCollName)
    lazy val collHosp = db(hospCollName)
    lazy val collProd = db(prodCollName)
    lazy val collRes = db(resCollName)
    lazy val collCal = db(calCollName)
    lazy val collComp = db(calCompCollName)
    lazy val collCalReport = db(calReportCollName)
    lazy val collReports = db(reportCollName)
    lazy val collShowReports = db(showReportCollName)

    /**
      * 将用户的输入抽象统一成计算抽象
      * @param proposalId
      * @param periodId
      * @return
      */
    def TmInputAgg(proposalId: String, projectId: String, periodId: String) : String = {
        val builder = MongoDBObject.newBuilder
        builder += "_id" -> new ObjectId(periodId)
        collPeriod.findOne(builder.result) match {
            case Some(p) => {
                val answers = periodAnswers(p)
                val presets = lastPeriodPreset(p, proposalId)
                aggAnswerWithPresetsToTmJobs(answers, presets, p, proposalId, projectId)
            }

            case None => null
        }
    }

    def periodAnswers(period: MongoDBObject): List[DBObject] = {
        period.getAs[List[ObjectId]]("answers") match {
            case Some(Nil) => Nil
            case Some(lst) => collAnswer.find($or(lst.map(x => DBObject("_id" -> x)))).toList
            case None => Nil
        }
    }

    def infoWithProposal(proposalId: String) : (List[DBObject], List[DBObject], List[DBObject]) = {
        val builder = MongoDBObject.newBuilder
        builder += "_id" -> new ObjectId(proposalId)
        collProposal.findOne(builder.result) match {
            case Some(p) => {
                (
                    hospWithProposal(p.getAs[List[ObjectId]]("targets").get),
                    productsWithProposal(p.getAs[List[ObjectId]]("products").get),
                    resourcesWithProposal(p.getAs[List[ObjectId]]("resources").get)
                )
            }
            case None => (Nil, Nil, Nil)
        }
    }

    def lastPeriodPreset(period: MongoDBObject, proposalId: String, phase: Int = 0): List[DBObject] = {
        val lp =
        period.getAs[String]("last") match {
            case Some(lp) => {
                val builder = MongoDBObject.newBuilder
                builder += "_id" -> lp
                collPeriod.findOne(builder.result) match {
                    case Some(p) => presetWithIds(p.getAs[List[ObjectId]]("presets").get)
                    case None => Nil
                }
            }
            case None => Nil
        }

        val pp =
        {
            val builder = MongoDBObject.newBuilder
            builder += "proposalId" -> proposalId
            builder += "phase" -> (phase - 1)
            collPreset.find(builder.result).toList
        }
        lp ::: pp
    }

    def presetWithIds(ids: List[ObjectId]) : List[DBObject] =
        collPreset.find($or(ids.map(x => DBObject("_id" -> x)))).toList

    def hospWithProposal(ids: List[ObjectId]) : List[DBObject] =
        collHosp.find($or(ids.map(x => DBObject("_id" -> x)))).toList

    def productsWithProposal(ids: List[ObjectId]) : List[DBObject] =
        collProd.find($or(ids.map(x => DBObject("_id" -> x)))).toList

    def resourcesWithProposal(ids: List[ObjectId]) : List[DBObject] =
        collRes.find($or(ids.map(x => DBObject("_id" -> x)))).toList

    def aggAnswerWithPresetsToTmJobs(
                                        answers: List[DBObject],
                                        presets: List[DBObject],
                                        period: MongoDBObject,
                                        proposalId: String,
                                        projectId: String
                                    ) : String = {
        val ra = answers.filter(_.get("category") == "Resource")
        val ma = answers.find(_.get("category") == "Management").getOrElse(MongoDBObject.newBuilder.result())
        val ba = answers.filter(_.get("category") == "Business")
        val jobId = UUID.randomUUID().toString

        val (hosps, products, resources) = infoWithProposal(proposalId)

        ba.foreach { x =>
            val builder = MongoDBObject.newBuilder

            builder += "dest_id" -> x.get("target").toString

            val h = hosps.find(_.get("_id") == x.get("target")).get
            builder += "hospital" -> h.get("name")
            builder += "hospital_level" -> h.get("level")

            builder += "goods_id" -> x.get("product").toString
            val p = products.find(_.get("_id") == x.get("product")).get
            builder += "product" -> p.get("name")
            builder += "life_cycle" -> p.get("lifeCycle")

            builder += "quota" -> x.get("salesTarget")
            builder += "budget" -> x.get("budget")
            builder += "meeting_attendance" -> x.get("meetingPlaces")
            builder += "call_time" -> x.get("visitTime")

            builder += "business_strategy_planning" -> ma.get("strategAnalysisTime")
            builder += "admin_work" -> ma.get("adminWorkTime")
            builder += "kol_management" -> ma.get("clientManagementTime")
            builder += "employee_kpi_and_compliance_check" -> ma.get("kpiAnalysisTime")
            builder += "team_meeting" -> ma.get("teamMeetingTime")

            builder += "period_id" -> period.get("_id").get.toString
            builder += "project_id" -> projectId
            builder += "job_id" -> jobId

            x.getAs[ObjectId]("resource") match {
                case Some(id) => {
                    val rat = ra.find(_.get("resource") == id).get

                    builder += "representative_id" -> id.toString
                    val r = resources.find(_.get("_id") == id).get
                    builder += "representative" -> r.get("name")
                    builder += "representative_time" -> r.get("totalTime")

                    builder += "product_knowledge_training" -> rat.get("productKnowledgeTraining")
                    builder += "career_development_guide" -> rat.get("vocationalDevelopment")
                    builder += "territory_management_training" -> rat.get("regionTraining")
                    builder += "performance_review" -> rat.get("performanceTraining")
                    builder += "sales_skills_training" -> rat.get("salesAbilityTraining")
                    builder += "field_work" -> rat.get("assistAccessTime")
                    builder += "one_on_one_coaching" -> rat.get("abilityCoach")

                    presets.find(x => x.get("resource") == id) match {
                        case Some(pr) => {
                            builder += "p_territory_management_ability" -> pr.get("territoryManagementAbility")
                            builder += "p_sales_skills" -> pr.get("salesSkills")
                            builder += "p_product_knowledge" -> pr.get("productKnowledge")
                            builder += "p_behavior_efficiency" -> pr.get("behaviorEfficiency")
                            builder += "p_work_motivation" -> pr.get("workMotivation")

                            builder += "p_target" -> pr.get("targetDoctorNum")
                            builder += "p_target_coverage" -> pr.get("targetDoctorCoverage")
                            builder += "p_high_target" -> pr.get("highTarget")
                            builder += "p_middle_target" -> pr.get("middleTarget")
                            builder += "p_low_target" -> pr.get("lowTarget")
                        }
                        case None => {
                            builder += "p_territory_management_ability" -> "0"
                            builder += "p_sales_skills" -> "0"
                            builder += "p_product_knowledge" -> "0"
                            builder += "p_behavior_efficiency" -> "0"
                            builder += "p_work_motivation" -> "0"

                            builder += "p_target" -> "0"
                            builder += "p_target_coverage" -> "0"
                            builder += "p_high_target" -> "0"
                            builder += "p_middle_target" -> "0"
                            builder += "p_low_target" -> "0"
                        }
                    }
                }
                case None => {
                    builder += "representative_id" -> ""
                    builder += "representative" -> ""
                    builder += "representative_time" -> 0

                    builder += "product_knowledge_training" -> 0
                    builder += "career_development_guide" -> 0
                    builder += "territory_management_training" -> 0
                    builder += "performance_review" -> 0
                    builder += "sales_skills_training" -> 0
                    builder += "field_work" -> 0
                    builder += "one_on_one_coaching" -> 0

                    builder += "p_territory_management_ability" -> "0"
                    builder += "p_sales_skills" -> "0"
                    builder += "p_product_knowledge" -> "0"
                    builder += "p_behavior_efficiency" -> "0"
                    builder += "p_work_motivation" -> "0"

                    builder += "p_target" -> "0"
                    builder += "p_target_coverage" -> "0"
                    builder += "p_high_target" -> "0"
                    builder += "p_middle_target" -> "0"
                    builder += "p_low_target" -> "0"
                }
            }

            presets.find(x => x.get("hospital") == h.get("_id") && x.get("product") == p.get("_id")) match {
                case Some(ps) => {
                    builder += "p_sales" -> ps.get("sales")
                    builder += "p_quota" -> ps.get("salesQuota")
                    builder += "p_share" -> ps.get("share")
                    builder += "potential" -> ps.get("potential")
                    builder += "patient" -> ps.get("patientNum")
                }
                case None => {
                    builder += "p_sales" -> "0"
                    builder += "p_quota" -> "0"
                    builder += "p_share" -> "0.0"
                    builder += "potential" -> "0.0"
                    builder += "patient" -> "0"
                }
            }

            collCal.insert(builder.result())
        }

        /**
          * 竞品表
          */
        {
            val bulk = collComp.initializeOrderedBulkOperation
            products.filter(_.get("productType") == 1).foreach { x =>
                val builder = MongoDBObject.newBuilder
                val tmp = presets.find ( y => y.get("product") == x.get("_id") && (y.get("category") == 16)).get
                builder += "job_id" -> jobId
                builder += "project_id" -> projectId
                builder += "period_id" -> period.get("_id").get.toString

                builder += "life_cycle" -> x.get("lifeCycle")
                builder += "product" -> x.get("name")
                builder += "p_share" -> tmp.get("share")

                bulk.insert(builder.result)
            }
            bulk.execute()
        }

        jobId
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
        builder += "share" -> report.getAs[Double]("share").getOrElse(0.0)
        builder += "sales" -> report.getAs[Double]("sales").getOrElse(0.0)
        builder += "quota" -> report.getAs[Double]("quota").getOrElse(0.0)
        builder += "budget" -> 0.0
        builder += "potential" -> report.get("potential")

        hosps.find(_.get("_id") == report.get("hospital")) match {
            case Some(h) => {
                builder += "hospital" -> h.get("name")
                builder += "hospital_level" -> h.get("level")
            }
            case None => {
                builder += "hospital" -> ""
                builder += "hospital_level" -> ""
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

        resources.find(_.get("_id") == report.get("resource")) match {
            case Some(r) => {
                builder += "representative" -> r.get("name")
                builder += "representative_time" -> 0

                builder += "work_motivation" -> report.getAs[Double]("workMotivation").getOrElse(0.0)
                builder += "territory_management_ability" -> report.getAs[Double]("territoryManagementAbility").getOrElse(0.0)
                builder += "sales_skills" -> report.getAs[Double]("salesSkills").getOrElse(0.0)
                builder += "product_knowledge" -> report.getAs[Double]("productKnowledge").getOrElse(0.0)
                builder += "behavior_efficiency" -> report.getAs[Double]("behaviorEfficiency").getOrElse(0.0)
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

    /**
      * TmReportAgg
      */
    def TmReportAgg(proposalId: String, projectId: String, periodId: String): String = {
        val jobId = UUID.randomUUID().toString
        val (hosps, products, resources) = infoWithProposal(proposalId)

        val bulk = collShowReports.initializeOrderedBulkOperation
        val builder = MongoDBObject.newBuilder
        builder += "proposalId" -> proposalId
        collReports.find(builder.result).toList.foreach { x =>
            builder += "job_id" -> jobId
            bulk.insert(periodPresetReport(hosps, products, resources, jobId, x))
        }
        bulk.execute()
        jobId
    }
}
