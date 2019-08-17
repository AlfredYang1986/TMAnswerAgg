package com.pharbers.TmAggregation

import java.util.UUID

import com.mongodb.casbah.Imports._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggMongoOpt.aggCollEnum2Coll

package object TmAggPreset2Cal {
    def apply(
                 proposalId: String,
                 projectId: String,
                 periodId: String,
                 phase: Int = 0): String = {

        val curPeriod = periodsColl.findOne(DBObject("_id" -> new ObjectId(periodId))).getOrElse(null)
        val curProposal = proposalsColl.findOne(DBObject("_id" -> new ObjectId(proposalId))).getOrElse(null)
        val curProject = projectsColl.findOne(DBObject("_id" -> new ObjectId(projectId))).getOrElse(null)

        aggAnswerWithPresetsToTmJobs(
            periodAnswers(curPeriod),
            loadCurrentPreset(curProject, curProposal, phase),
            hospitalsColl.find(
                $or(curProposal.getAs[List[ObjectId]]("targets").get
                    .map(x => DBObject("_id" -> x)))).toList,
            productsColl.find(
                $or(curProposal.getAs[List[ObjectId]]("products").get
                    .map(x => DBObject("_id" -> x)))).toList,
            resourcesColl.find(
                $or(curProposal.getAs[List[ObjectId]]("resources").get
                    .map(x => DBObject("_id" -> x)))).toList,
            curPeriod,
            curProposal,
            projectId,
            phase
        )
    }

    def periodAnswers(period: MongoDBObject): List[DBObject] = {
        period.getAs[List[ObjectId]]("answers") match {
            case Some(lst) => answersColl.find($or(lst.map(x => DBObject("_id" -> x)))).toList
            case None => Nil
        }
    }

    def loadCurrentPreset(project: DBObject, proposal: DBObject, phase: Int): List[DBObject] =
        presetsColl.find(
            $or(
                $and("phase" -> phase, "proposalId" -> proposal._id.get.toString) ::
                $and("phase" -> phase, "project" -> project._id.get.toString) :: Nil
            )
        ).toList

    def aggAnswerWithPresetsToTmJobs(
                                        answers: List[DBObject],
                                        presets: List[DBObject],
                                        hospitals: List[DBObject],
                                        products: List[DBObject],
                                        resources: List[DBObject],
                                        period: DBObject,
                                        proposal: DBObject,
                                        projectId: String,
                                        phase: Int): String = {

        val resourceAnswers = answers.filter(_.get("category") == "Resource")
        val managementAnswers = answers.find(_.get("category") == "Management").getOrElse(MongoDBObject.newBuilder.result())
        val businessAnswers = answers.filter(_.get("category") == "Business")
        val jobId = UUID.randomUUID().toString

        val bulk = calColl.initializeOrderedBulkOperation
        businessAnswers.foreach { cal_data =>
            val builder = MongoDBObject.newBuilder

            /**
              * 1. hospital
              */
            builder += "dest_id" -> cal_data.get("target").toString
            val curHosp = hospitals.find(_.get("_id") == cal_data.get("target")).get
            builder += "hospital" -> curHosp.get("name")
            builder += "hospital_level" -> curHosp.get("level")
            builder += "city" -> curHosp.get("position")

            /**
              * 2. product
              */
            builder += "goods_id" -> cal_data.get("product").toString
            val curProduct = products.find(_.get("_id") == cal_data.get("product")).get
            builder += "product" -> curProduct.get("name")
            builder += "life_cycle" -> curProduct.get("lifeCycle")
            builder += "product_area" -> curProduct.get("treatmentArea")

            /**
              * 2.1 从presets中拿上周期预设，完善
              *  - status
              *  - potential
              *  - 上期销售 p_quota
              *  - 上期份额 p_share
              *  - 上期budget p_budget
              *  - 上期的ytd
              *  - 上期计算结果病人人数
              *  - 初始总budget
              */
            presets.find( x =>
                x.get("hospital") == curHosp.get("_id") &&
                x.get("product") == curProduct.get("_id") &&
                x.get("category") == 8
            ) match {
                case Some(curHPPreset) => {
                    builder += "status" -> (
                        if (curHPPreset.get("currentDurgEntrance").toString == "1") "已开发"
                        else "未开发")

                    builder += "p_sales" -> curHPPreset.get("lastSales")
                    builder += "p_quota" -> curHPPreset.get("lastQuota")
                    builder += "p_share" -> curHPPreset.get("lastShare")
                    builder += "p_budget" -> curHPPreset.get("lastBudget")

                    builder += "patient" -> curHPPreset.get("currentPatientNum")
                    builder += "p_ytd_sales" -> curHPPreset.get("ytd")
                    builder += "potential" -> curHPPreset.get("potential")
                    builder += "initial_budget" -> curHPPreset.get("initBudget")
                }
                case None => {
                    builder += "p_sales" -> 0
                    builder += "p_quota" -> 0
                    builder += "p_share" -> 0
                    builder += "p_budget" -> 0

                    builder += "patient" -> 0
                    builder += "p_ytd_sales" -> 0
                    builder += "potential" -> 0
                }
            }

            /**
              * 2.2 presets中拿到去年同期销量
              */
            reportsColl.findOne(
                $and(
                    DBObject("hospital" -> curHosp.get("_id")) ::
                    DBObject("product" -> curProduct.get("_id")) ::
                    DBObject("phase" -> (phase - 4)) ::
                    DBObject("category" -> "Hospital") :: Nil)
            ) match {
                case Some(pppp) => {
                    builder += "pppp_sales" -> pppp.get("sales")
                }
                case None => builder += "pppp_sales" -> 0
            }


            /**
              * 3. 用户输入信息
              */
            builder += "quota" -> cal_data.get("salesTarget")
            builder += "budget" -> cal_data.get("budget")
            builder += "meeting_attendance" -> cal_data.get("meetingPlaces")
            builder += "call_time" -> cal_data.get("visitTime")

            /**
              * 4. 管理输入信息
              */
            builder += "business_strategy_planning" -> managementAnswers.get("strategAnalysisTime")
            builder += "admin_work" -> managementAnswers.get("adminWorkTime")
            builder += "kol_management" -> managementAnswers.get("clientManagementTime")
            builder += "employee_kpi_and_compliance_check" -> managementAnswers.get("kpiAnalysisTime")
            builder += "team_meeting" -> managementAnswers.get("teamMeetingTime")

            /**
              * 5. 人员代表输入信息
              */
            cal_data.getAs[ObjectId]("resource") match {
                case Some(rid) => {
                    /**
                      * 5.1 代表基本信息
                      */
                    val curRes = resources.find(_.get("_id") == rid).get
                    builder += "representative_id" -> rid.toString
                    builder += "representative" -> curRes.get("name")
                    builder += "representative_time" -> curRes.get("totalTime")

                    /**
                      * 5.2 代表填写答案
                      */
                    val rat = resourceAnswers.find(_.get("resource") == rid).get
                    builder += "product_knowledge_training" -> rat.get("productKnowledgeTraining")
                    builder += "career_development_guide" -> rat.get("vocationalDevelopment")
                    builder += "territory_management_training" -> rat.get("regionTraining")
                    builder += "performance_review" -> rat.get("performanceTraining")
                    builder += "sales_skills_training" -> rat.get("salesAbilityTraining")
                    builder += "field_work" -> rat.get("assistAccessTime")
                    builder += "one_on_one_coaching" -> rat.get("abilityCoach")

                    /**
                      * 5.3 代表上一周期的能力信息
                      * 从category为2的preset中拿
                      */
                    presets.find(x =>
                            x.get("resource") == rid &&
                                x.get("category") == 2) match {

                        case Some(curResPreset) => {
                            builder += "p_territory_management_ability" -> curResPreset.get("currentTMA")
                            builder += "p_sales_skills" -> curResPreset.get("currentSalesSkills")
                            builder += "p_product_knowledge" -> curResPreset.get("currentProductKnowledge")
                            builder += "p_behavior_efficiency" -> curResPreset.get("currentBehaviorEfficiency")
                            builder += "p_work_motivation" -> curResPreset.get("currentWorkMotivation")

                            builder += "p_target" -> curResPreset.get("currentTargetDoctorNum")
                            builder += "p_target_coverage" -> curResPreset.get("currentTargetDoctorCoverage")
                            builder += "p_high_target" -> curResPreset.get("currentClsADoctorVT")
                            builder += "p_middle_target" -> curResPreset.get("currentClsBDoctorVT")
                            builder += "p_low_target" -> curResPreset.get("currentClsCDoctorVT")
                        }
                        case None => {
                            builder += "p_territory_management_ability" -> 0
                            builder += "p_sales_skills" -> 0
                            builder += "p_product_knowledge" -> 0
                            builder += "p_behavior_efficiency" -> 0
                            builder += "p_work_motivation" -> 0

                            builder += "p_target" -> 0
                            builder += "p_target_coverage" -> 0
                            builder += "p_high_target" -> 0
                            builder += "p_middle_target" -> 0
                            builder += "p_low_target" -> 0
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

            /**
              * 6. 固定信息
              */
            builder += "hosp_num" -> hospitals.length
            builder += "rep_num" -> resources.length

            /**
              * 7. 系统信息
              */
            builder += "period_id" -> period.get("_id").toString
            builder += "project_id" -> projectId
            builder += "job_id" -> jobId

            bulk.insert(builder.result)
        }
        bulk.execute()
        /**
          * 8. 竞争品
          */
        val bulk02 = calCompColl.initializeOrderedBulkOperation
        products.filter(_.get("productType") == 1).foreach { x =>
            val builder = MongoDBObject.newBuilder
            val tmp = presets.find ( y =>
                y.get("product") == x.get("_id") &&
                y.get("category") == 16 &&
                y.get("phase") == phase
            ).get

            builder += "job_id" -> jobId
            builder += "project_id" -> projectId
            builder += "period_id" -> period.get("_id").toString

            builder += "life_cycle" -> x.get("lifeCycle")
            builder += "product" -> x.get("name")
            builder += "product_area" -> x.get("treatmentArea")

            if (proposal.get("case") == "tm") builder += "p_share" -> tmp.get("share")
            else builder += "market_share_c" -> tmp.get("share")

            bulk02.insert(builder.result)
        }
        bulk02.execute()
        jobId
    }
}
