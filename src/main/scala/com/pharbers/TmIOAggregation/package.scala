package com.pharbers

import java.util.UUID

import com.mongodb.casbah.Imports._

package object TmIOAggregation {

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

    def lastPeriodPreset(period: MongoDBObject, proposalId: String): List[DBObject] = {
        period.getAs[String]("last") match {
            /**
              * 有前期，向前期中取值
              */
            case Some(lp) => {
                val builder = MongoDBObject.newBuilder
                builder += "_id" -> lp
                collPeriod.findOne(builder.result) match {
                    case Some(p) => presetWithProposal(p.getAs[List[ObjectId]]("presets").get)
                    case None => Nil
                }
            }

            /**
              * 没有前序周期，向proposal中取值
              */
            case None => {
                val builder = MongoDBObject.newBuilder
                builder += "_id" -> new ObjectId(proposalId)
                collProposal.findOne(builder.result) match {
                    case Some(p) => presetWithProposal(p.getAs[List[ObjectId]]("presets").get)
                    case None => Nil
                }
            }
        }
    }

    def presetWithProposal(ids: List[ObjectId]) : List[DBObject] =
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
                                        projectId: String,
                                        needComp: Boolean = false
                                    ) : String = {
        val ra = answers.filter(_.get("category") == "Resource")
        val ma = answers.find(_.get("category") == "Management").get
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

            builder += "period" -> period.get("_id").get.toString
            builder += "project" -> projectId
            builder += "job" -> jobId

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
                    builder += "p_sales" -> ps.get("achievements")
                    builder += "p_quota" -> ps.get("salesQuota")
                    builder += "p_share" -> ps.get("share")
                    builder += "potential" -> ps.get("potential")
                }
                case None => {
                    builder += "p_sales" -> "0"
                    builder += "p_quota" -> "0"
                    builder += "p_share" -> "0.0"
                    builder += "potential" -> "0.0"
                }
            }

            collCal.insert(builder.result())
        }

        /**
          * 竞品表
          */
        if (needComp) {
            val bulk = collComp.initializeOrderedBulkOperation
            products.filter(_.get("productType") == 1).foreach { x =>
                val builder = MongoDBObject.newBuilder
                val tmp = presets.find(_.get("product") == x.get("name")).get
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

    /**
      * 根据TM R 返回的数据，重新写入数据库操作
      * @param uid
      * @return
      */
    def TmResultAgg(uid: String): String = {
        var period: Option[DBObject] = None
        val bulk = collPreset.initializeOrderedBulkOperation
        val ids = collCalReport.filter(_.get("job_id") == uid).map { x =>
            /**
              * 1. query当前计算的period
              */
            if (period isEmpty) {
                val builder = MongoDBObject.newBuilder
                builder += "_id" -> new ObjectId(x.get("period_id").toString)
                period = collPeriod.findOne(builder.result)
            }

            /**
              * 2. 形成基于产品以及医院的presets
              */
            val id = new ObjectId()
            val builder = MongoDBObject.newBuilder
            builder += "_id" -> id
            builder += "hospital" -> x.get("dest_id")
            builder += "product" -> x.get("good_id")
            builder += "resource" -> x.get("representative_id")

            builder += "salesQuota" -> x.get("quota")
            builder += "achievements" -> x.get("sales")
            builder += "share" -> x.get("share")

            builder += "potential" -> x.get("potential")
            builder += "territoryManagementAbility" -> x.get("territory_management_ability")
            builder += "salesSkills" -> x.get("sales_skills")
            builder += "productKnowledge" -> x.get("product_knowledge")
            builder += "behaviorEfficiency" -> x.get("behavior_efficiency")
            builder += "workMotivation" -> x.get("work_motivation")

            bulk.insert(builder.result)
            id
        }
        /**
          * 3. 将presets写入
          */
        val bulkResult = bulk.execute()
        if (bulkResult.isAcknowledged && bulkResult.getInsertedCount > 0) {
            period.get += "presets" -> ids.toList
            collPeriod.update(DBObject("_id" -> period.get.get("_id")), period.get)
        }

        uid
    }
}
