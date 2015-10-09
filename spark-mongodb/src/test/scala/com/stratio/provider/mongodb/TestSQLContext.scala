package org.apache.spark.sql.test

import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLConf, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** A SQLContext that can be used for local testing. */
class LocalSQLContext
  extends SQLContext(
    new SparkContext(
      "local[2]",
      "TestSQLContext",
      new SparkConf().set("spark.sql.testkey", "true"))) {

  override protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  protected[sql] class SQLSession extends super.SQLSession {
    protected[sql] override lazy val conf: SQLConf = new SQLConf {
      /** Fewer partitions to speed up testing. */
      override def numShufflePartitions: Int = 2
    }
  }

  /**
   * Turn a logical plan into a [[DataFrame]]. This should be removed once we have an easier way to
   * construct [[DataFrame]] directly out of local data without relying on implicits.
   */
  protected[sql] implicit def logicalPlanToSparkQuery(plan: LogicalPlan): DataFrame = {
    DataFrame(this, plan)
  }

}

object TestSQLContext extends LocalSQLContext
