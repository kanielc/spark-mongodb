/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.datasource.mongodb.writer

import com.mongodb.casbah.Imports._
import com.stratio.datasource.Config
import com.stratio.datasource.mongodb.MongodbConfig

/**
 * A batch writer implementation for mongodb writer.
 * The used semantics for storing objects is 'replace'.
 *
 * @param config Configuration parameters (host,database,collection,...)
 * @param batchSize Group size to be inserted via bulk operation
 */
class MongodbBatchWriter(
                          config: Config,
                          batchSize: Int = 100) extends MongodbWriter(config) {

  final val IdKey = "_id"

  def save(it: Iterator[DBObject]): Unit = {
    val pkConfig: Option[Array[String]] = config.get[Array[String]](MongodbConfig.UpdateFields)
    val idFieldConfig: Option[String] = config.get[String](MongodbConfig.IdField)
    it.grouped(batchSize).foreach { group =>
      val bulkOperation = dbCollection.initializeUnorderedBulkOperation
      group.foreach { element =>
        if (idFieldConfig.isDefined || pkConfig.isDefined) {
          val query = getUpdateQuery(element, pkConfig, idFieldConfig)
          if (query.isEmpty) bulkOperation.insert(element) else bulkOperation.find(query).upsert().replaceOne(element)
        } else bulkOperation.insert(element)
      }
      bulkOperation.execute(config.getOrElse[WriteConcern](MongodbConfig.WriteConcern, MongodbConfig.DefaultWriteConcern))
    }
  }

  private def getUpdateQuery(element: DBObject,
                             pkConfig: Option[Array[String]],
                             idFieldConfig: Option[String]): Map[String, AnyRef] = {
    val idValue: Map[String, AnyRef] =
      if (idFieldConfig.isDefined && element.contains(IdKey)) Map(IdKey -> element.get(IdKey))
      else Map()
    if(idValue.isEmpty) {
      val pkValues: Map[String, AnyRef] =
        if (pkConfig.isDefined)
          pkConfig.get.flatMap(field => if (element.contains(field)) Some(field -> element.get(field)) else None).toMap
        else Map()
      pkValues
    } else idValue
  }
}