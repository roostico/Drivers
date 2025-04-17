package jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, SparkSession}
import utils.Commons
import utils.Preprocess.{addDurationRemovingNegatives, applyAllPreprocess}

object SecondJob {

  private val datasetFolder = "./dataset"
  private val yellowCab = s"$datasetFolder/yellow_cab"
  private val timeZoneOver: String = "overnight"
  private val timeZones = Map(timeZoneOver -> (20, 6), "regular" -> (6, 20))
  private val sparkFilters: Map[String, Column] = Map(
    "VendorID" -> (col("VendorID") isin(1, 2, 6, 7)),
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
      .appName("Second job")
      .getOrCreate()


    if (args.length == 0) {
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)

    val schema = StructType(Seq(
      StructField("VendorID", IntegerType, nullable = false)
    ))

    val yellowDataset = spark
      .read
      .option("recursiveFileLookup", "true")
      .parquet(Commons.getDatasetPath(deploymentMode, yellowCab))

    val droppedNa = yellowDataset.na.drop()

    val filteredDF =  applyAllPreprocess(
      droppedNa,
      sparkFilters,
      taxFilter,
      timeZones,
      "tpep_dropoff_datetime",
      "tpep_pickup_datetime"
    )

    filteredDF.show()



  }








}
