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
                 phase: Int = 0): String = {

        val jobResult = calReportColl.find(DBObject("job_id" -> jobId)).toList

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

    }

    def aggHospital(
                       results: List[DBObject],
                       hospitals: List[DBObject],
                       products: List[DBObject],
                       resources: List[DBObject],
                       phase: Int) = {
        val bulk = reportsColl.initializeOrderedBulkOperation

        results.groupBy(res => res.get("hospital").toString + "##" + res.get("product").toString).foreach { it =>
            val items = it._2
            val (hn :: pn :: Nil) = it._1.split("##")
            val builder  = MongoDBObject.newBuilder
            builder += "phase" -> phase
            builder += "category" -> "Hospital"
            builder += "hospital" -> hospitals.find(_.get("name") == hn).get._id
            builder += "product" -> products.find(_.get("name") == pn).get._id
//            builder += "resource" -> resources.find(_.get("name") == )
            

        }



        bulk.execute()
    }
}
