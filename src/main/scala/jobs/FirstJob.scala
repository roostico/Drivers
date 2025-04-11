package jobs

import org.apache.spark.sql.SparkSession
import utils._
import utils.Preprocess.{addDurationRemovingNegatives, addTimeZones, addYear, applyAllPreprocess, binColByEqualQuantiles, binColByStepValue, dropNullValues}
import org.apache.spark.sql.functions._

object FirstJob {

  private val datasetDir = "/dataset"
  private val yellowDatasetDir = s"$datasetDir/dataset_yellow"
  private val greenDatasetDir = s"$datasetDir/dataset_green"
  private val fhvDatasetDir = s"$datasetDir/dataset_fhv"
  private val fhvhvDatasetDir = s"$datasetDir/dataset_fhvhv"
  private val outputDir = "/output/firstJobOutput"
  private val timeZones = Map("overnight" -> (20, 6), "regular" -> (6, 20))
  
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


    val datasetWithEqualBins = applyAllPreprocess(
      yellowDataset,
      timeZones
    )
    datasetWithEqualBins.filter(col("duration_minutes_overnight") > 0 && col("duration_minutes_regular") > 0).show(300)

//    val datasetWithAvgCosts = datasetWithEqualBins.groupBy("duration_minutes_bin_label", "passenger_count").count().orderBy("duration_minutes_bin_label")
//    datasetWithAvgCosts.show(300)

  }
}
