package com.pharbers.TmAggregation.TmAggMongoHandler

import com.mongodb.casbah.Imports._
import com.pharbers.TmAggregation.TmAggMongoHandler.AggCollEnum.AggCollEnum

object AggMongoOpt {
    val mongodbHost = "192.168.100.176"
    val mongodbPort = 27017
    val mongodbUsername = ""
    val mongodbPassword = ""
    val ntmDBName = "pharbers-ntm-client"

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
