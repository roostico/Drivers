package jobs

import org.apache.spark.sql.{Column, SparkSession}
import utils._
import utils.Preprocess.{addCostComparison, applyAllPreprocess, binColByEqualQuantiles, binColByStepValue, dropNullValues, getPercentage}
import org.apache.spark.sql.functions._

import java.io.PrintWriter
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
  private val colsForValuesAnalysis: List[String] = List(
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
        "cost_per_distance_diff_pcg", 10
      ),
      "cost_per_time_diff_pcg", 5
    )

    // {"time" -> {"store_and_fwd_flag_Y" -> {"0-10" -> 5.5%}}}
    val resultsMap: scala.collection.mutable.Map[
      String, scala.collection.mutable.Map[
        String, Map[
          String, Float]]] =
      scala.collection.mutable.Map("time" -> scala.collection.mutable.Map(),
        "distance" -> scala.collection.mutable.Map())

    val stepForBinEdges: Int = 10
    val binEdges = (-100 to 100 by stepForBinEdges).map(_.toFloat)

    colsForValuesAnalysis.foreach { colAnalyze =>
      val uniqueValues = dfWithPriceDiffPcg.select(col(colAnalyze)).distinct()
        .collect()
        .map(_.get(0))
        .toList

      uniqueValues.foreach { uniqueVal =>

        val filteredDF = dfWithPriceDiffPcg.filter(col(colAnalyze) === uniqueVal)
        val totalCount = filteredDF.count().toFloat

        if (totalCount > 0) {

          Seq("time", "distance").foreach { feat =>
            val binCounts = binEdges.map { start =>
              val end = start + stepForBinEdges
              val label = s"[$start|$end)"
              val count = filteredDF
                .filter(col(f"cost_per_${feat}_diff_pcg") >= start && col(f"cost_per_${feat}_diff_pcg") < end)
                .count()
              label -> "%.2f".format(count.toFloat / totalCount * 100).toFloat
            }.toMap

            // Add extra bin for > 100
            val over100Count = filteredDF.filter(col(f"cost_per_${feat}_diff_pcg") > 100).count()
            val overLabel = "(>100)"

            // Add extra bin for > 100
            val under100Count = filteredDF.filter(col(f"cost_per_${feat}_diff_pcg") < -100).count()
            val underLabel = "(<-100)"

            val fullBinCounts = binCounts +
              (overLabel -> "%.2f".format(over100Count.toFloat / totalCount * 100).toFloat) +
              (underLabel -> "%.2f".format(under100Count.toFloat / totalCount * 100).toFloat)

            resultsMap(feat)(s"$colAnalyze=$uniqueVal") = fullBinCounts
          }
        }
      }
    }

    val pw = new PrintWriter(outputDir)
    pw.write(resultsMap.toString)
    pw.close()
  }
}
