package org.hammerlab.spear

import com.github.fakemongo.Fongo
import com.mongodb.casbah.{MongoClient, MongoCollection}

trait MongoTestCollection {

  def getMongoConfig: Option[(String, Int)] = {
    val mongoOpt = Option(System.getProperty("mongo"))
    val hostOpt = Option(System.getProperty("mongo.host")).orElse(Option(System.getProperty("mongo.hostname")))
    val portOpt = Option(System.getProperty("mongo.port")).map(_.toInt)
    mongoOpt.orElse(hostOpt).orElse(portOpt).map(_ => (hostOpt.getOrElse("localhost"), portOpt.getOrElse(27017)))
  }

  val collection: MongoCollection =
    getMongoConfig match {
      case Some((hostname, port)) =>
        println(s"using real mongo at ${hostname}:${port}")
        val client = MongoClient(hostname, port)
        val db = client("test")
        val collection = db("test")
        collection.drop()
        collection
      case _ =>
        println(s"using 'fake' in-memory mongo")
        val mongo = new Fongo("fake-mongo")
        val db = mongo.getDB("test-db")
        val collection = db.getCollection("test-collection")
        new MongoCollection(collection)
    }
}
