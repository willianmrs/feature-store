package br.com.ifood.data.featurestore.aggregation.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.concurrent.duration.Duration

object Operation extends Enumeration {
  type Operation = Value
  val SUM, MIN, MAX, MEAN, AVG = Value

  def getSparkExpr(op: Operation.Value, expr: Column): Column = {
    op match {
      case SUM => sum(expr)
      case MIN => min(expr)
      case MAX => max(expr)
      case MEAN => mean(expr)
      case AVG => avg(expr)
    }
  }
}


case class AggAction(featureName: String,
                     period: Duration,
                     field: String,
                     operation: Operation.Value)
