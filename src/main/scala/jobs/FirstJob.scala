package jobs

import org.apache.spark.sql.SparkSession
import utils._
import utils.Preprocess.{addDurationRemovingNegatives, binColByStepValue, binColByEqualQuantiles, dropNullValues}
import org.apache.spark.sql.functions._

object FirstJob {

  private val datasetDir = "/dataset"
  private val yellowDatasetDir = s"$datasetDir/dataset_yellow"
  private val greenDatasetDir = s"$datasetDir/dataset_green"
  private val fhvDatasetDir = s"$datasetDir/dataset_fhv"
  private val fhvhvDatasetDir = s"$datasetDir/dataset_fhvhv"
  val outputDir = "/output/firstJobOutput"
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("First job")
      .getOrCreate()

    if(args.length == 0){
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)

    val yellowDataset = dropNullValues(spark.read
      .parquet(Commons.getDatasetPath(deploymentMode, yellowDatasetDir))) // First: 44644946 DropNull: 40013565 DropNegativeDuration: 39999817

    val datasetWithBins = binColByStepValue(addDurationRemovingNegatives(yellowDataset), "duration_minutes")
    val datasetWithEqualBins = binColByEqualQuantiles(addDurationRemovingNegatives(yellowDataset), "duration_minutes", 25)

    val durationCountsEquals = datasetWithEqualBins.groupBy("duration_minutes_bin_label").count().orderBy("duration_minutes_bin_label")
    durationCountsEquals.show(300)

  }
}
