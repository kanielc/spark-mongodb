/*
 *
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package com.stratio.provider.mongodb.performance

import com.mongodb.casbah.Imports._
import com.stratio.provider.mongodb._
import com.stratio.provider.mongodb.writer.MongodbBatchWriter
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.{FlatSpec, Matchers}

class MongodbPerformanceSpec extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"

  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .build()

  behavior of "Library performance"

  it should "should be as fast as possible in" in {
    val words = scala.io.Source.fromFile("/usr/share/dict/words").getLines().toSeq
    val inversePairs = words.zip(words.reverse).map(ss => DBObject("first" -> ss._1, "second" -> ss._2)).toList

    withEmbedMongoFixture(List()) { mongodbProc =>
      val start = System.currentTimeMillis()
      // write our pairs
      val mongodbBatchWriter = new MongodbBatchWriter(testConfig)
      val dbOIterator = inversePairs.iterator
      mongodbBatchWriter.saveWithPk(dbOIterator)

      // now read our pairs back
      val pairsDF = TestSQLContext.fromMongoDB(testConfig)
      pairsDF.registerTempTable("pairs")
      pairsDF.show()
      val wordsRedux = TestSQLContext.sql("SELECT a.`_id`, a.`first` from pairs as a join pairs as b where a.`first` = b.`second`")
      TestSQLContext.udf.register("length", (s: String) => if (s == null) 0 else s.length)
      val wordLengths = TestSQLContext.sql("select `_id`, length(`first`) as lf, length(`second`) as ls from pairs").distinct
      println("word lengths count: " + wordLengths.count())

      // throw in a filter
      val longWordPairs = wordLengths.filter("lf > 3 and ls > 3")

      // outer join it (drop _id to cause generation of new one)
      val longOnes = wordsRedux.join(longWordPairs, wordsRedux("_id") === longWordPairs("_id"), "leftouter")

      // let's write again what we had in Mongo one more time (new _id generated though)
      wordsRedux.drop("_id").saveToMongodb(testConfig)  // as batch
      wordsRedux.drop("_id").saveToMongodb(testConfig, batch = false)  // and not as batch
      wordLengths.drop("_id").saveToMongodb(testConfig)
      wordLengths.drop("_id").saveToMongodb(testConfig, batch = false)
      longOnes.drop("_id").saveToMongodb(testConfig)

      // now read everything back for good measure
      val allItems = TestSQLContext.fromMongoDB(testConfig)
      println("non-null count of second: " + allItems.filter("ls is not null").count())

      val end = System.currentTimeMillis()
      println("Total time: " + (end - start))
    }
  }
}
