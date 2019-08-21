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

		val jobResult = calReportColl.find(DBObject("job_id" -> jobId)).toList.filter { x =>
            x.get("hospital") != null &&
            x.get("product") != null &&
            x.get("representative") != null
        }

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

		val competitors = calCompetitorColl.find(
			$and("job_id" -> jobId, "period_id" -> periodId, "project_id" -> projectId)).toList

		aggHospital(jobResult, hosps, products, resources, curProject, curPeriod, curProposal, phase)
		aggRegion(jobResult, hosps, products, resources, curProject, curPeriod, curProposal, phase)
		aggResource(jobResult, hosps, products, resources, curProject, curPeriod, curProposal, phase)
		aggProduct(jobResult, hosps, products, resources, curProject, curPeriod, curProposal, phase)
		aggSales(jobResult, hosps, products, resources, curProject, curPeriod, curProposal, phase)

		aggPreset(jobResult, hosps, products, competitors, resources, curProject, curPeriod, phase) // category 8
		aggResourcePreset(jobResult, hosps, products, resources, curProject, curPeriod, phase) // category 2
		aggQuotaPreset(jobResult, hosps, products, resources, curProject, curPeriod, phase) // category 4

		aggFinalSummary(curProject, jobId)
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
		queryStringSafe(x.get("hospital")) + "##" +
			queryStringSafe(x.get("product")) + "##" +
			queryStringSafe(x.get("representative"))

	def aggReport(
		             results: List[DBObject],
		             hospitals: List[DBObject],
		             products: List[DBObject],
		             resources: List[DBObject],
		             project: DBObject,
		             period: DBObject,
		             proposal: DBObject,
		             phase: Int,
		             cat: String,
		             func: DBObject => String) = {

		val bulk = reportsColl.initializeOrderedBulkOperation

		results.groupBy(func(_)).foreach { it =>

			val items = it._2
            val (hn :: pn :: rn :: Nil) = queryName(it._2.head).split("##").toList
            val builder = MongoDBObject.newBuilder
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
            builder += "salesContri" -> 0.0
            builder += "salesGrowthYOY" -> 0.0
            builder += "salesGrowthMOM" -> 0.0
            builder += "salesQuota" -> items.map(x => queryNumSafe(x.get("quota"))).sum
            builder += "quotaGrowthMOM" -> 0.0
            builder += "share" -> 0.0
            builder += "achievements" -> items.map(x => queryNumSafe(x.get("achievements"))).sum

            builder += "projectId" -> project._id.get.toString
            builder += "periodId" -> period._id.get.toString
            builder += "proposalId" -> proposal._id.get.toString

            bulk.insert(builder.result)
        }

		bulk.execute()
	}

	def aggQuotaPreset(
		                  results: List[DBObject],
		                  hospitals: List[DBObject],
		                  products: List[DBObject],
		                  resources: List[DBObject],
		                  project: DBObject,
		                  period: DBObject,
		                  phase: Int) = {

		/**
		  * 3. category 4 for Quotas
		  */
		val bulk = reportsColl.initializeOrderedBulkOperation

		results.groupBy(res => res.get("product")).foreach { it =>

			val items = it._2
			val (hn :: pn :: rn :: Nil) = queryName(it._2.head).split("##").toList
			val builder = MongoDBObject.newBuilder
			builder += "phase" -> (phase + 1)
			builder += "category" -> 4
			builder += "hospital" -> hospitals.find(_.get("name") == hn).get._id
			builder += "product" -> products.find(_.get("name") == pn).get._id
			builder += "resource" -> resources.find(_.get("name") == rn).get._id

			builder += "lastSales" -> 0.0
			builder += "lastQuota" -> items.map(x => queryNumSafe(x.get("quota"))).sum
			builder += "lastAchievement" -> 0.0
			builder += "lastShare" -> 0.0
			builder += "lastBudget" -> 0.0
			builder += "initBudget" -> 0.0
			builder += "currentPatientNum" -> 0.0
			builder += "currentDurgEntrance" -> 0.0
			builder += "potential" -> 0.0
			builder += "ytd" -> 0.0

			builder += "currentTMA" -> 0.0
			builder += "currentSalesSkills" -> 0.0
			builder += "currentProductKnowledge" -> 0.0
			builder += "currentBehaviorEfficiency" -> 0.0
			builder += "currentWorkMotivation" -> 0.0
			builder += "currentTargetDoctorNum" -> 0.0
			builder += "currentTargetDoctorCoverage" -> 0.0
			builder += "currentClsADoctorVT" -> 0.0
			builder += "currentClsBDoctorVT" -> 0.0
			builder += "currentClsCDoctorVT" -> 0.0

			bulk.insert(builder.result)
		}

		bulk.execute()
	}

	def aggResourcePreset(
		                     results: List[DBObject],
		                     hospitals: List[DBObject],
		                     products: List[DBObject],
		                     resources: List[DBObject],
		                     project: DBObject,
		                     period: DBObject,
		                     phase: Int) = {

		/**
		  * 2. category 2 for resource ability
		  */
		val bulk = reportsColl.initializeOrderedBulkOperation

		results.groupBy(res => res.get("representative")).foreach { it =>

			val items = it._2
			val (hn :: pn :: rn :: Nil) = queryName(it._2.head).split("##").toList
			val builder = MongoDBObject.newBuilder
			builder += "phase" -> (phase + 1)
			builder += "category" -> 2
			builder += "hospital" -> hospitals.find(_.get("name") == hn).get._id
			builder += "product" -> products.find(_.get("name") == pn).get._id
			builder += "resource" -> resources.find(_.get("name") == rn).get._id

			builder += "lastSales" -> 0.0
			builder += "lastQuota" -> 0.0
			builder += "lastAchievement" -> 0.0
			builder += "lastShare" -> 0.0
			builder += "lastBudget" -> 0.0
			builder += "initBudget" -> 0.0
			builder += "currentPatientNum" -> 0.0
			builder += "currentDurgEntrance" -> 0.0
			builder += "potential" -> 0.0
			builder += "ytd" -> 0.0

			builder += "currentTMA" -> queryNumSafe(items.head.get("tma"))
			builder += "currentSalesSkills" -> queryNumSafe(items.head.get("sales_skills"))
			builder += "currentProductKnowledge" -> queryNumSafe(items.head.get("product_knowledge"))
			builder += "currentBehaviorEfficiency" -> queryNumSafe(items.head.get("behavior_efficiency"))
			builder += "currentWorkMotivation" -> queryNumSafe(items.head.get("work_motivation"))
			builder += "currentTargetDoctorNum" -> queryNumSafe(items.head.get("target"))
			builder += "currentTargetDoctorCoverage" -> queryNumSafe(items.head.get("target_coverage"))
			builder += "currentClsADoctorVT" -> queryNumSafe(items.head.get("high_target"))
			builder += "currentClsBDoctorVT" -> queryNumSafe(items.head.get("middle_target"))
			builder += "currentClsCDoctorVT" -> queryNumSafe(items.head.get("low_target"))

			bulk.insert(builder.result)
		}

		bulk.execute()
	}

	def aggPreset(
		             results: List[DBObject],
		             hospitals: List[DBObject],
		             products: List[DBObject],
		             competitors: List[DBObject],
		             resources: List[DBObject],
		             project: DBObject,
		             period: DBObject,
		             phase: Int) = {

		val bulk = presetsColl.initializeOrderedBulkOperation

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
		results.foreach { res =>
			val builder = MongoDBObject.newBuilder
			/**
			  * 1. category 8 for next period
			  */
			val (hn :: pn :: rn :: Nil) = queryName(res).split("##").toList
			builder += "phase" -> (phase + 1)
			builder += "category" -> 8
			builder += "hospital" -> hospitals.find(_.get("name") == hn).get._id
			builder += "product" -> products.find(_.get("name") == pn).get._id
			builder += "resource" -> resources.find(_.get("name") == rn).get._id

			builder += "projectId" -> project._id.get.toString

			builder += "lastSales" -> queryNumSafe(res.get("sales"))
			builder += "lastQuota" -> queryNumSafe(res.get("quota"))
			builder += "lastAchievement" -> queryNumSafe(res.get("achievements"))
			builder += "lastShare" -> queryNumSafe(res.get("market_share"))
			builder += "lastBudget" -> queryNumSafe(res.get("budget"))
			builder += "initBudget" -> queryNumSafe(res.get("next_budget"))
			builder += "currentPatientNum" -> queryNumSafe(res.get("patient"))
			builder += "currentDurgEntrance" -> (if (res.get("status") == "已开发") "1"
			else if (res.get("status") == "正在开发") "2"
			else "0")
			builder += "potential" -> queryNumSafe(res.get("potential"))
			builder += "ytd" -> queryNumSafe(res.get("ytd_sales"))

			builder += "currentTMA" -> 0.0
			builder += "currentSalesSkills" -> 0.0
			builder += "currentProductKnowledge" -> 0.0
			builder += "currentBehaviorEfficiency" -> 0.0
			builder += "currentWorkMotivation" -> 0.0
			builder += "currentTargetDoctorNum" -> 0.0
			builder += "currentTargetDoctorCoverage" -> 0.0
			builder += "currentClsADoctorVT" -> 0.0
			builder += "currentClsBDoctorVT" -> 0.0
			builder += "currentClsCDoctorVT" -> 0.0

			bulk.insert(builder.result())
		}

		competitors.foreach { comp =>
			val builder = MongoDBObject.newBuilder
			/**
			  * 1. category 16 for next period
			  */
			val hn = null
			val pn = comp.get("product")
			val rn = null
			builder += "phase" -> (phase + 1)
			builder += "category" -> 16
			builder += "hospital" -> hn
			builder += "product" -> products.find(_.get("name") == pn).get._id
			builder += "resource" -> rn

			builder += "projectId" -> project._id.get.toString

			builder += "lastSales" -> queryNumSafe(comp.get("sales"))
			builder += "lastQuota" -> 0.0
			builder += "lastAchievement" -> 0.0
			builder += "lastShare" -> queryNumSafe(comp.get("market_share"))
			builder += "lastBudget" -> 0.0
			builder += "initBudget" -> 0.0
			builder += "currentPatientNum" -> 0.0
			builder += "currentDurgEntrance" -> ""
			builder += "potential" -> 0.0
			builder += "ytd" -> 0.0

			builder += "currentTMA" -> 0.0
			builder += "currentSalesSkills" -> 0.0
			builder += "currentProductKnowledge" -> 0.0
			builder += "currentBehaviorEfficiency" -> 0.0
			builder += "currentWorkMotivation" -> 0.0
			builder += "currentTargetDoctorNum" -> 0.0
			builder += "currentTargetDoctorCoverage" -> 0.0
			builder += "currentClsADoctorVT" -> 0.0
			builder += "currentClsBDoctorVT" -> 0.0
			builder += "currentClsCDoctorVT" -> 0.0

			bulk.insert(builder.result())
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
		               proposal: DBObject,
		               phase: Int) = {

		aggReport(
			results, hospitals, products, resources,
			project, period, proposal, phase, "Hospital",
			(res) => {
				queryStringSafe(res.get("hospital")) + "##" +
					queryStringSafe(res.get("product")) + "##" +
					queryStringSafe(res.get("representative"))
			})
	}

	def aggRegion(
		             results: List[DBObject],
		             hospitals: List[DBObject],
		             products: List[DBObject],
		             resources: List[DBObject],
		             project: DBObject,
		             period: DBObject,
		             proposal: DBObject,
		             phase: Int) = {

		aggReport(
			results, hospitals, products, resources,
			project, period, proposal, phase, "Region",
			(res) => {
				queryStringSafe(res.get("city")) + "##" +
					queryStringSafe(res.get("product"))
			})
	}

	def aggResource(
		               results: List[DBObject],
		               hospitals: List[DBObject],
		               products: List[DBObject],
		               resources: List[DBObject],
		               project: DBObject,
		               period: DBObject,
		               proposal: DBObject,
		               phase: Int) = {

		aggReport(
			results, hospitals, products, resources,
			project, period, proposal, phase, "Resource",
			(res) => {
				queryStringSafe(res.get("representative")) + "##" +
					queryStringSafe(res.get("product"))
			})
	}

	def aggProduct(results: List[DBObject],
	               hospitals: List[DBObject],
	               products: List[DBObject],
	               resources: List[DBObject],
	               project: DBObject,
	               period: DBObject,
	               proposal: DBObject,
	               phase: Int) = {

		aggReport(
			results, hospitals, products, resources,
			project, period, proposal, phase, "Product",
			(res) => {
				"##" + queryStringSafe(res.get("product"))
			})
	}

	def aggSales(results: List[DBObject],
	             hospitals: List[DBObject],
	             products: List[DBObject],
	             resources: List[DBObject],
	             project: DBObject,
	             period: DBObject,
	             proposal: DBObject,
	             phase: Int) = {

		aggReport(
			results, hospitals, products, resources,
			project, period, proposal, phase, "Sales",
			(res) => "##")
	}

	def aggFinalSummary(project: DBObject, jobId: String) = {
		val f = calFinalResult.findOne(DBObject("job_id" -> jobId)).get
		f += "_id" -> new ObjectId

		f += "quotaAchv" -> f.get("quota_achv")
		f -= "quota_achv"

		f += "salesForceProductivity" -> f.get("sales_force_productivity")
		f -= "sales_force_productivity"

		f += "roi" -> f.get("return_on_investment")
		f -= "return_on_investment"

        f += "newAccount" -> f.get("new_account")

		finals.insert(f)
		val lst = project.getAs[List[ObjectId]]("finals") match {
			case Some(lst) => lst :+ f._id
			case None => f._id
		}
		project += "finals" -> lst
		projectsColl.update(DBObject("_id" -> project._id), project)
	}
}
