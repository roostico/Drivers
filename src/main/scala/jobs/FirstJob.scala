package jobs

import org.apache.spark.sql.SparkSession
import utils._

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

    val rddYellowDataset = spark.read
      .parquet(Commons.getDatasetPath(deploymentMode, yellowDatasetDir)).rdd
    val rddGreenDataset = spark.read
      .parquet(Commons.getDatasetPath(deploymentMode, greenDatasetDir)).rdd
    val rddFhvDataset = spark.read
      .parquet(Commons.getDatasetPath(deploymentMode, fhvDatasetDir)).rdd
    val rddFhvhvDataset = spark.read
      .parquet(Commons.getDatasetPath(deploymentMode, fhvhvDatasetDir)).rdd

    rddYellowDataset.take(5).foreach(println)
    rddGreenDataset.take(5).foreach(println)
    rddFhvDataset.take(5).foreach(println)
    rddFhvhvDataset.take(5).foreach(println)
  }
}
