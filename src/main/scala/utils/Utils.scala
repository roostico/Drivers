package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}

object Utils {

  private val feesList = Seq("extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee")

  def debugDistinctValues(df: DataFrame, features: Seq[String] = feesList): Unit = {
    features.foreach(feat => df.select(feat).distinct().orderBy(desc(feat)).show(500))
  }

  def debugHighAmounts(df: DataFrame): Unit = {
    df.select(col("total_amount"), col("fare_amount"), col("tip_amount"), col("aggregate_fee"), col("duration_minutes_bin_label"), col(
      "trip_distance_bin_label")).orderBy(desc("total_amount")).show(500)
  }
}
