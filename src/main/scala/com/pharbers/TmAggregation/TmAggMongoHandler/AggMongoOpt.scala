package com.pharbers.TmAggregation.TmAggMongoHandler

import com.mongodb.casbah.Imports._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum.AggCollEnum

object AggMongoOpt {

    lazy val mongodbHost = "pharbers.com" //System.getProperty("MONGO_HOST")
    lazy val mongodbPort = "5555".toInt //System.getProperty("MONGO_PORT").toInt
    lazy val mongodbUsername = ""
    lazy val mongodbPassword = ""
    lazy val ntmDBName = "pharbers-ntm-client" //System.getProperty("MONGO_DEST")

    lazy val db = MongoClient(mongodbHost, mongodbPort)(ntmDBName)
    var colls: Map[AggCollEnum, MongoCollection] = Map.empty

    def apply(name: AggCollEnum): MongoCollection = colls.get(name) match {
        case Some(c) => c
        case None => {
            val tmp = db(name.toString)
            colls = colls + (name -> tmp)
            tmp
        }
    }

    import scala.language.implicitConversions
    implicit def aggCollEnum2Coll(x: AggCollEnum): MongoCollection = this.apply(x)
}
