package org.hammerlab.spear

import com.github.fakemongo.Fongo
import com.mongodb.casbah.{MongoClient, MongoCollection}

trait MongoTestCollection {
  val collection: MongoCollection =
    if (System.getProperty("mongo") != null) {
      println(s"using real mongo... ${System.getProperty("mongo")}")
      val client = MongoClient("localhost", 27017)
      val db = client("test")
      val collection = db("test")
      collection.drop()
      collection
    } else {
      println(s"using fake mongo...")
      val mongo = new Fongo("fake-mongo")
      val db = mongo.getDB("test-db")
      val collection = db.getCollection("test-collection")
      new MongoCollection(collection)
    }
}
