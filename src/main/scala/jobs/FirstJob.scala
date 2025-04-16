package jobs

import org.apache.spark.sql.{Column, SparkSession}
import utils._
import utils.Preprocess.{applyAllPreprocess, binColByEqualQuantiles, binColByStepValue, dropNullValues, getPercentage, addCostPerDistanceComparison}
import org.apache.spark.sql.functions._

import scala.language.postfixOps

object FirstJob {

  private val datasetDir = "/dataset"
  private val yellowDatasetDir = s"$datasetDir/dataset_yellow"
  private val greenDatasetDir = s"$datasetDir/dataset_green"
  private val fhvDatasetDir = s"$datasetDir/dataset_fhv"
  private val fhvhvDatasetDir = s"$datasetDir/dataset_fhvhv"
  private val outputDir = "/output/firstJobOutput"
  private val timeZoneOver: String = "overnight"
  private val timeZones = Map(timeZoneOver -> (20, 6), "regular" -> (6, 20))
  private val sparkFilters: Map[String, Column] = Map(
    "passenger_count" -> (col("passenger_count") > 0),
    "trip_distance" -> (col("trip_distance") > 0),
    "RatecodeID" -> (col("RatecodeID").between(1, 6) || col("RatecodeID") === 99),
    "store_and_fwd_flag" -> (col("store_and_fwd_flag") === "Y" || col("store_and_fwd_flag") === "N"),
    "payment_type" -> col("payment_type").between(1, 6),
    "fare_amount" -> (col("fare_amount") > 0),
    "tax" -> (col("tax") > 0)
  )
  private val taxFilter: Column => Column = _ > 0
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("First job")
      .getOrCreate()

    if (args.length == 0) {
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)

    val yellowDataset = dropNullValues(spark.read
      .parquet(Commons.getDatasetPath(deploymentMode, yellowDatasetDir))) // First: 44644946 DropNull: 40013565 DropNegativeDuration: 39999817
    
    var dfPreprocessed = applyAllPreprocess(
      yellowDataset,
      sparkFilters,
      taxFilter,
      timeZones,
      "tpep_dropoff_datetime",
      "tpep_pickup_datetime"
    )

    dfPreprocessed = binColByEqualQuantiles(
      getPercentage(
        dfPreprocessed, f"duration_minutes_$timeZoneOver", "duration_minutes"),
      f"duration_minutes_${timeZoneOver}_pcg", 20)

    dfPreprocessed.show(500)

    val groupByCols = Seq(
      "passenger_count",
      "store_and_fwd_flag",
      "payment_type",
      "aggregate_fee_bin_label",
      "duration_minutes_bin_label",
      "year",
      s"duration_minutes_${timeZoneOver}_pcg_bin"
    )

    val datasetWithAvgCosts = dfPreprocessed
      .groupBy(groupByCols.map(col): _*)
      .agg(round(avg("cost_per_distance"), 2).alias("avg_cost_per_distance"))

    val dfWithAvgCost = dfPreprocessed
      .join(datasetWithAvgCosts, groupByCols, "left")

    val dfWithPriceDiffPcg = binColByStepValue(addCostPerDistanceComparison(dfWithAvgCost), "cost_per_distance_diff_pcg", 10)

    dfWithPriceDiffPcg
      .show(400)

  }
}
