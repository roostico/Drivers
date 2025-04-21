package jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import utils.Commons

object SecondJob {

  private val datasetFolder = "./dataset"
  private val yellowCab = s"$datasetFolder/yellow_cab"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Second job")
      .getOrCreate()

    if (args.length == 0) {
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)

    val yellowDataset = spark
      .read
      .option("recursiveFileLookup", "true")
      .parquet(Commons.getDatasetPath(deploymentMode, yellowCab))

    val filtered = yellowDataset
      .filter(col("VendorID") isin(1, 2, 6, 7))
      .filter(col("fare_amount") > 0)
      .filter(col("tip_amount") >= 0) // Include also all cases without tips
      .filter(col("payment_type").between(1, 6))
      .filter(col("trip_distance") > 0)
      .filter(col("tip_amount") <= col("fare_amount") * 1.5) // Remove tip over the 50% of total fare
      .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) // Ensure that drop off > pickup
      .na.drop()
      .dropDuplicates()

    val withTripDuration = filtered.withColumn(
      "trip_duration_min",
      unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")) / 60.0
    )

    // Calculate the percentiles
    val distancePercentiles = withTripDuration.stat.approxQuantile("trip_distance", Array(0.02, 0.98), 0.01)
    val tripDurationPercentiles = withTripDuration.stat.approxQuantile("trip_duration_min", Array(0.02, 0.98), 0.01)

    val distanceLowerBound = distancePercentiles(0)
    val distanceUpperBound = distancePercentiles(1)
    val tripDurationLowerBound = tripDurationPercentiles(0)
    val tripDurationUpperBound = tripDurationPercentiles(1)

    val filteredWithoutOutliers =  withTripDuration.filter(
      col("trip_distance") >= distanceLowerBound && col("trip_distance") <= distanceUpperBound &&
      col("trip_duration_min") >= tripDurationLowerBound && col("trip_duration_min") <= tripDurationUpperBound
    )

    // Feat Engineering
    val dfTemp = filteredWithoutOutliers
      .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .withColumn("day_of_week", date_format(col("tpep_pickup_datetime"),"F").cast("int"))
      .withColumn("is_weekend", col("day_of_week").isin(6,7).cast("int"))
      .withColumn("month", month(col("tpep_pickup_datetime")))
      .withColumn("year", year(col("tpep_pickup_datetime")))
      .withColumn("trip_hour_bucket",
        when(col("hour_of_day").between(0,5), "late_night")
          .when(col("hour_of_day").between(6,9), "morning")
          .when(col("hour_of_day").between(10,15), "midday")
          .when(col("hour_of_day").between(16,19), "evening")
          .otherwise("night")
      )

    dfTemp.show()

  }








}
