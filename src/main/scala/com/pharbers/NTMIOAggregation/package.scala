package com.pharbers

import java.util.UUID

import com.mongodb.casbah.Imports._

import scala.collection.immutable

package object NTMIOAggregation {
//    val mongodbHost = "192.168.100.176"
    val mongodbHost = "192.168.100.176"
    val mongodbPort = 27017
    val mongodbUsername = ""
    val mongodbPassword = ""
    val ntmDBName = "pharbers-ntm-client-10"
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

    def lastYearPreset(proposalId: String, phase: Int = 0): List[DBObject] = {
        val builder = MongoDBObject.newBuilder
        builder += "proposalId" -> proposalId
        builder += "phase" -> (phase - 4)
        collPreset.find(builder.result).toList
    }

    def currentPeriodPreset(proposalId: String, phase: Int = 0): List[DBObject] = {
        val builder = MongoDBObject.newBuilder
        builder += "proposalId" -> proposalId
        builder += "phase" -> phase
        collPreset.find(builder.result).toList
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
            builder += "city" -> h.get("position")

            builder += "goods_id" -> x.get("product").toString
            val p = products.find(_.get("_id") == x.get("product")).get
            builder += "product" -> p.get("name")
            builder += "life_cycle" -> p.get("lifeCycle")
            builder += "product_area" -> p.get("treatmentArea")
            if (p.get("name") == "开拓来") builder += "status" -> "已开发"
            else builder += "status" -> "未开发"

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
                }
                case None => {
                    builder += "p_sales" -> "0"
                    builder += "p_quota" -> "0"
                    builder += "p_share" -> "0.0"
                }
            }

            currentPeriodPreset(proposalId, 0).
                find(x => x.get("hospital") == h.get("_id") && x.get("product") == p.get("_id")) match {

                case Some(ps) => {
                    builder += "patient" -> ps.get("patientNum")
//                    builder += "p_ytd_sales" -> ps.get("ytd")
                    builder += "p_ytd_sales" -> "0"
//                    builder += "pppp_sales" -> ps.get("lySalse")
//                    builder += "p_budget" -> ps.get("salesQuota")
                    builder += "p_budget" -> "0"
                    builder += "potential" -> ps.get("potential")
                }
                case None => {
                    builder += "patient" -> "0"
                    builder += "p_ytd_sales" -> "0"
//                    builder += "pppp_sales" -> "0"
                    builder += "p_budget" -> "0"
                    builder += "potential" -> "0.0"
                }
            }

            currentPeriodPreset(proposalId, -4).
                find(x => x.get("hospital") == h.get("_id") && x.get("product") == p.get("_id")) match {

                case Some(ps) => {
                    builder += "pppp_sales" -> ps.get("sales")
                }
                case None => {
                    builder += "pppp_sales" -> "0"
                }
            }

            builder += "hosp_num" -> hosps.length
            builder += "rep_num" -> resources.length
            builder += "initial_budget" -> 250000.0


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
                builder += "product_area" -> x.get("treatmentArea")

                collProposal.findOne(DBObject("_id"->new ObjectId(proposalId))) match {
                    case Some(p) => {
                        if (p.get("case") == "tm") builder += "p_share" -> tmp.get("share")
                        else builder += "market_share_c" -> tmp.get("share")
                    }
                    case None => ???
                }

                bulk.insert(builder.result)
            }
            bulk.execute()
        }

        jobId
    }

    def queryNumSafe(x: AnyRef): Double = {
        if (x == null) 0.0
        else x.toString.toDouble
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
                builder += "representative_time" -> 0.0

//                builder += "work_motivation" -> report.getAs[Double]("workMotivation").getOrElse(0.0)
                builder += "work_motivation" -> queryNumSafe(report.get("workMotivation"))
//                builder += "territory_management_ability" -> report.getAs[Double]("territoryManagementAbility").getOrElse(0.0)
                builder += "territory_management_ability" -> queryNumSafe(report.get("territoryManagementAbility"))
//                builder += "sales_skills" -> report.getAs[Double]("salesSkills").getOrElse(0.0)
                builder += "sales_skills" -> queryNumSafe(report.get("salesSkills"))
//                builder += "product_knowledge" -> report.getAs[Double]("productKnowledge").getOrElse(0.0)
                builder += "product_knowledge" -> queryNumSafe(report.get("productKnowledge"))
//                builder += "behavior_efficiency" -> report.getAs[Double]("behaviorEfficiency").getOrElse(0.0)
                builder += "behavior_efficiency" -> queryNumSafe(report.get("behaviorEfficiency"))
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
    
    def calReport2Report(hosps: List[DBObject],
                         products: List[DBObject],
                         resources: List[DBObject],
                         calReport: List[DBObject],
                         phase: Int,
                         periodId: String, projectId: String): (List[DBObject], List[DBObject], List[DBObject]) = {
        
        def queryProportion(molecule: Double, denominator: Double): Double = {
            if (denominator == 0.0 || molecule == 0.0) { 0.0 }
            else molecule / denominator
        }
        
        def queryHospitalId(name: String): String =
            hosps.find(_.getAs[String]("name").get == name) match {
                case Some(o) => o.getAs[ObjectId]("_id").get.toString
                case None => ""
            }
        
        def queryProductId(name: String): String =
            products.find(_.getAs[String]("name").get == name) match {
                case Some(o) => o.getAs[ObjectId]("_id").get.toString
                case None => ""
            }
        
        def queryResourceId(name: String): String =
            resources.find(_.getAs[String]("name").get == name) match {
                case Some(o) => o.getAs[ObjectId]("_id").get.toString
                case None => ""
            }
        
        def groupByHospitalAgg() = {
            val groupProduct = calReport.groupBy(cr => (cr.get("product").toString))
            
            calReport.map { hosp =>
                val achievements = queryProportion(hosp.getAsOrElse[Double]("sales", 0.0),
                    hosp.getAsOrElse[Double]("quota", 0.0))
                val sumQuota = groupProduct.get(hosp.get("product").toString).get.map(_.getAsOrElse[Double]("quota", 0.0)).sum
                val sumSales = groupProduct.get(hosp.get("product").toString).get.map(_.getAsOrElse[Double]("sales", 0.0)).sum
                
                val quotaContri = queryProportion(hosp.getAsOrElse[Double]("quota", 0.0), sumQuota)
    
                val salesContri = queryProportion(hosp.getAsOrElse[Double]("sales", 0.0), sumSales)
    
                val salesGrowthMOM = queryProportion(hosp.getAsOrElse[Double]("sales", 0.0),
                    hosp.getAsOrElse[Double]("p_sales", 0.0)) - 1
    
                val salesGrowthYOY = queryProportion(hosp.getAsOrElse[Double]("sales", 0.0),
                    hosp.getAsOrElse[Double]("pppp_sales", 0.0)) - 1
    
                val quotaGrowthMOM = queryProportion(hosp.getAsOrElse[Double]("quota", 0.0),
                    hosp.getAsOrElse[Double]("p_sales", 0.0)) - 1
    
                val share = hosp.getAs[Double]("market_share") match {
                    case Some(s) => s
                    case None => hosp.getAsOrElse[Double]("share", 0.0)
                }
    
                val builder = MongoDBObject.newBuilder
                builder += "__v" -> 0
                builder += "achievements" -> achievements // sales / quota
                builder += "behaviorEfficiency" -> hosp.getAsOrElse[Double]("behavior_efficiency", 0.0)
                builder += "category" -> "Hospital"
                builder += "drugEntrance" -> hosp.getAsOrElse[String]("status", "")
                builder += "hospital" -> queryHospitalId(hosp.getAsOrElse[String]("hospital", ""))
                builder += "patientNum" -> hosp.getAsOrElse[Double]("patient", 0.0)
                builder += "periodId" -> periodId
                builder += "phase" -> phase
                builder += "potential" -> hosp.getAsOrElse[Double]("potential", 0.0)
                builder += "product" -> queryProductId(hosp.getAsOrElse[String]("product", ""))
                builder += "productKnowledge" -> hosp.getAsOrElse[Double]("product_knowledge", 0.0)
                builder += "projectId" -> projectId
                builder += "proposalId" -> None
                builder += "quotaContri" -> quotaContri // quota / p_sales
                builder += "quotaGrowthMOM" -> quotaGrowthMOM // 当前quota / p_sales - 1
                builder += "region" -> hosp.getAsOrElse[String]("region", "")
                builder += "resource" -> queryHospitalId(hosp.getAsOrElse[String]("representative", ""))
                builder += "sales" -> hosp.getAsOrElse[Double]("sales", 0.0)
                builder += "salesContri" -> salesContri // sales / sums
                builder += "salesGrowthMOM" -> salesGrowthMOM // sales / p_sales -1
                builder += "salesGrowthYOY" -> salesGrowthYOY // sales / pppp_sales -1
                builder += "salesQuota" -> hosp.getAsOrElse[Double]("quota", 0.0)
                builder += "salesSkills" -> hosp.getAsOrElse[Double]("sales_skills", 0.0)
                builder += "share" -> share // Ucb Marker share, TM share 要做处理
                builder += "territoryManagementAbility" -> hosp.getAsOrElse[Double]("territory_management_ability", 0.0)
                builder += "workMotivation" -> hosp.getAsOrElse[Double]("work_motivation", 0.0)
                builder.result()
            }
        }
    
        def groupByProductAgg() = {
            val sumQouta = calReport.map(_.getAsOrElse[Double]("quota", 0.0)).sum
            val sumSales = calReport.map(_.getAsOrElse[Double]("sales", 0.0)).sum
            calReport.groupBy(cr => cr.get("product")).toList.map { data =>
                val sales = data._2.map(_.getAsOrElse[Double]("sales", 0.0)).sum
                val quota = data._2.map(_.getAsOrElse[Double]("quota", 0.0)).sum
                val potential = data._2.map(_.getAsOrElse[Double]("potential", 0.0)).sum
                val achievements = queryProportion(sales, quota)
	            val quotaContri = queryProportion(quota, sumQouta)
                val salesContri = queryProportion(sales, sumSales)
	            val salesGrowthMOM = queryProportion(sales, data._2.map(_.getAsOrElse[Double]("p_sales", 0.0)).sum) - 1
	            val salesGrowthYOY = queryProportion(sales, data._2.map(_.getAsOrElse[Double]("pppp_sales", 0.0)).sum) - 1
                val quotaGrowthMOM = queryProportion(quota, data._2.map(_.getAsOrElse[Double]("p_sales", 0.0)).sum) - 1
	            val share = queryProportion(sales, potential)
                
                val builder = MongoDBObject.newBuilder
                builder += "__v" -> 0
                builder += "achievements" -> achievements // sales / quota
                builder += "behaviorEfficiency" -> None
                builder += "category" -> "Product"
                builder += "drugEntrance" -> None
                builder += "hospital" -> None
                builder += "patientNum" -> None
                builder += "periodId" -> periodId
                builder += "phase" -> phase
                builder += "potential" -> None
                builder += "product" -> queryProductId(data._1.toString)
                builder += "productKnowledge" -> None
                builder += "projectId" -> projectId
                builder += "proposalId" -> None
                builder += "quotaContri" -> quotaContri //quotaContri // quota / 预设quota
                builder += "quotaGrowthMOM" -> quotaGrowthMOM
                builder += "region" -> None
                builder += "resource" -> None
                builder += "sales" -> sales
                builder += "salesContri" -> salesContri //salesContri // sales / sums
                builder += "salesGrowthMOM" -> salesGrowthMOM //salesGrowthMOM // sales / p_sales -1
                builder += "salesGrowthYOY" -> salesGrowthYOY //salesGrowthYOY // sales / pppp_sales -1
                builder += "salesQuota" -> quota
                builder += "salesSkills" -> None
                builder += "share" -> share //hosp.getAsOrElse[Double]("share", 0.0)// Ucb Marker share, TM share 要做处理
                builder += "territoryManagementAbility" -> None //hosp.getAsOrElse[Double]("territory_management_ability", 0.0)
                builder += "workMotivation" -> None //hosp.getAsOrElse[Double]("work_motivation", 0.0)
                builder.result()
            }
        }
       
        def groupByResourceAgg() = {
            val groupProduct = calReport.groupBy(cr => (cr.get("product").toString))
            
            val resource_data = calReport.groupBy(cr => (cr.get("representative"),
                    cr.get("product"),
                    cr.get("work_motivation"),
                    cr.get("territory_management_ability"),
                    cr.get("sales_skills"),
                    cr.get("product_knowledge"),
                    cr.get("behavior_efficiency"))).toList
            
            resource_data.map { rd => {
                val sumQuota = groupProduct.get(rd._1._2.toString).get.map(_.getAsOrElse[Double]("quota", 0.0)).sum
                val sumSales = groupProduct.get(rd._1._2.toString).get.map(_.getAsOrElse[Double]("sales", 0.0)).sum
                
                val sales = rd._2.map(_.getAsOrElse[Double]("sales", 0.0)).sum
                val quota = rd._2.map(_.getAsOrElse[Double]("quota", 0.0)).sum
                val potential = rd._2.map(_.getAsOrElse[Double]("potential", 0.0)).sum
                val achievements = queryProportion(sales, quota)
                val quotaContri = queryProportion(quota, sumQuota)
                val salesContri = queryProportion(sales, sumSales)
                val salesGrowthMOM = queryProportion(sales, rd._2.map(_.getAsOrElse[Double]("p_sales", 0.0)).sum) - 1
                val salesGrowthYOY = queryProportion(sales, rd._2.map(_.getAsOrElse[Double]("pppp_sales", 0.0)).sum) - 1
                val quotaGrowthMOM = queryProportion(quota, rd._2.map(_.getAsOrElse[Double]("p_sales", 0.0)).sum) - 1
                val share = queryProportion(sales, potential)
                
                val builder = MongoDBObject.newBuilder
                builder += "__v" -> 0
                builder += "achievements" -> achievements // sales / quota
                builder += "behaviorEfficiency" -> rd._1._7
                builder += "category" -> "Resource"
                builder += "drugEntrance" -> None //rd.getAsOrElse[String]("status", "")
                builder += "hospital" -> None
                builder += "patientNum" -> None //rd.getAsOrElse[Double]("patient", 0.0)
                builder += "periodId" -> periodId //hosp.getAsOrElse[String]("period_id", "")
                builder += "phase" -> phase
                builder += "potential" -> None//rd.getAsOrElse[Double]("potential", 0.0)
                builder += "product" -> queryProductId(rd._1._2.toString)
                builder += "productKnowledge" -> rd._1._6
                builder += "projectId" -> projectId
                builder += "proposalId" -> None
                builder += "quotaContri" -> quotaContri // quota / 预设quota
                builder += "quotaGrowthMOM" -> quotaGrowthMOM
                builder += "region" -> None
                builder += "resource" -> queryResourceId(rd._1._1.toString)
                builder += "sales" -> sales
                builder += "salesContri" -> salesContri // sales / sums
                builder += "salesGrowthMOM" -> salesGrowthMOM // sales / p_sales -1
                builder += "salesGrowthYOY" -> salesGrowthYOY // sales / pppp_sales -1
                builder += "salesQuota" -> quota //rd.getAsOrElse[Double]("quota", 0.0)
                builder += "salesSkills" -> rd._1._5
                builder += "share" -> share //rd.getAsOrElse[Double]("share", 0.0)// Ucb Marker share, TM share 要做处理
                builder += "territoryManagementAbility" -> rd._1._4
                builder += "workMotivation" -> rd._1._3
                builder.result()
            }}
            
        }
    
        (groupByHospitalAgg(), groupByProductAgg() ,groupByResourceAgg())
        
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
    
    /**
      * collection => cal_report to reports
      */
    def TmResultAgg(proposalId: String, projectId: String, periodId: String): String = {
        val jobId = UUID.randomUUID().toString
        val (hosps, products, resources) = infoWithProposal(proposalId)
        
        val bulk = collReports.initializeOrderedBulkOperation
        val builder = MongoDBObject.newBuilder
        
        builder += "period_id" ->  periodId
        builder += "project_id" -> projectId
        val calReports = collCalReport.find(builder.result).toList
    
        // emmm
        val projectCondition = MongoDBObject.newBuilder
        projectCondition += "_id" -> new ObjectId(projectId)
    
        val periods = collProject.findOne(projectCondition.result) match {
            case Some(dbo) => dbo.getAs[List[ObjectId]]("periods").get // 哇  数据库为啥是ObjectId哦
            case None => Nil
        }
    
        val presetCondition = MongoDBObject.newBuilder
        presetCondition += "proposalId" -> projectId
	    
        val presets = collPreset.find(presetCondition.result).toList
        
        val result = calReport2Report(hosps, products,
                        resources, calReports,
                        periods.indexOf(new ObjectId(periodId)),
                        periodId, projectId)
        result._1 foreach(bulk.insert(_))
        result._2 foreach(bulk.insert(_))
        result._3 foreach(bulk.insert(_))
	    
        bulk.execute()
        jobId
    }
}
