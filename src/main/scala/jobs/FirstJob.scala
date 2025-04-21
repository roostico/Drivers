package jobs

import org.apache.spark.sql.{Column, SparkSession}
import utils._
import utils.Preprocess.{addCostComparison, applyAllPreprocess, binColByEqualQuantiles, binColByStepValue, dropNullValues, getPercentage}
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
    "tolls_amount" -> (col("tolls_amount") < 200),
  )
  private val colsForClassification: Seq[String] = Seq(
    "passenger_count",
    "store_and_fwd_flag",
    "payment_type",
    "aggregate_fee_bin_label",
    "duration_minutes_bin_label",
    "trip_distance_bin_label",
    "year",
    s"duration_minutes_${timeZoneOver}_pcg_bin"
  )
  private val colsForValuesAnalysis: Seq[String] = Seq(
    "passenger_count",
    "store_and_fwd_flag",
    "payment_type",
    "aggregate_fee_bin_label",
    "duration_minutes_bin_label",
    "trip_distance_bin_label",
    "year",
    s"duration_minutes_${timeZoneOver}_pcg_bin"
  )
  private val taxFilter: Column => Column = _ >= 0
  
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
      .parquet(Commons.getDatasetPath(deploymentMode, yellowDatasetDir)))
    
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

    val datasetWithAvgDistanceCosts = dfPreprocessed
      .groupBy(colsForClassification.map(col): _*)
      .agg(round(avg("cost_per_distance"), 2).alias("avg_cost_per_distance"))

    val datasetWithAvgTimeCosts = dfPreprocessed
      .groupBy(colsForClassification.map(col): _*)
      .agg(round(avg("cost_per_time"), 2).alias("avg_cost_per_time"))

    val dfWithAvgCost = dfPreprocessed
      .join(datasetWithAvgDistanceCosts, colsForClassification, "left")
      .join(datasetWithAvgTimeCosts, colsForClassification, "left")

    val dfWithPriceDiffPcg = binColByStepValue(
      binColByStepValue(
        addCostComparison(
          addCostComparison(
            dfWithAvgCost,
            "distance"),
          "time"
        ),
        "cost_per_distance_diff_pcg", 5
      ),
      "cost_per_time_diff_pcg", 5
    )

    val totalCount = dfWithPriceDiffPcg.count()

    val priceCols = Seq("time", "distance").map(feat => f"cost_per_${feat}_diff_pcg_bin_label")
    val relevantCols = colsForValuesAnalysis ++ priceCols

    val reducedDfForAnalysis = dfWithPriceDiffPcg
      .select(relevantCols.map(col): _*)

    val statsPerFeature = colsForValuesAnalysis.map { colName =>
      val groupCols = priceCols :+ colName
      reducedDfForAnalysis.groupBy(groupCols.map(col): _*)
        .agg(count("*").alias("count"))
        .withColumn("feature", lit(colName))
        .withColumn("value", col(colName).cast("string"))
        .withColumn("cost_distance_label", col("cost_per_distance_diff_pcg_bin_label"))
        .withColumn("cost_time_label", col("cost_per_time_diff_pcg_bin_label"))
        .withColumn("pcg", round(col("count") / totalCount.toDouble * 100, 2))
        .select("feature", "value", "count", "pcg", "cost_distance_label", "cost_time_label")
    }

    val finalStatsDF = statsPerFeature.reduce(_ union _)

    finalStatsDF
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, outputDir))
  }
}
