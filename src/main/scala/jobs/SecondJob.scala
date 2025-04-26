package jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Commons

object BinningHelper {

  /**
   * Create bins for a numeric column.
   *
   * @param df        The input DataFrame.
   * @param inputCol  The name of the numeric column to bin.
   * @param outputCol The name of the new binned column.
   * @param bins      A list of thresholds. Must be in ascending order.
   * @param labels    A list of labels for the bins. Must be bins.length + 1 long.
   * @return          DataFrame with new binned column.
   */
  def withBinnedColumn(
                        df: DataFrame,
                        inputCol: String,
                        outputCol: String,
                        bins: Seq[Double],
                        labels: Seq[String]
                      ): DataFrame = {
    require(labels.length == bins.length + 1, "You need one more label than bin thresholds.")

    val conditions = bins.zipWithIndex.foldLeft(
      when(col(inputCol) < bins.head, lit(labels.head))
    ) { case (acc, (_, i)) =>
      acc
        .when(
          col(inputCol) >= bins(i) &&
            col(inputCol) < (if (i + 1 < bins.length) bins(i + 1) else Double.MaxValue), labels(i + 1)
        )
    }

    val binCol = conditions.otherwise(labels.last)

    df.withColumn(outputCol, binCol)
  }
}

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
      .filter(col("tip_amount") >= 0)
      .filter(col("payment_type").between(1, 6))
      .filter(col("trip_distance") > 0)
      .filter(col("tip_amount") <= col("fare_amount") * 1.5)
      .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
      .na.drop()
      .dropDuplicates()

    val withTripDuration = filtered.withColumn(
      "trip_duration_min",
      (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
    )

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

    val cachedDf: DataFrame = dfTemp.cache()


    val binConfigs = Map(
      "trip_distance" -> (
        Seq(1.0, 3.0, 6.0),
        Seq("0-1", "1-3", "3-6", "6+")
      ),
      "trip_duration_min" -> (
        Seq(5.0, 15.0, 30.0),
        Seq("0-5", "5-15", "15-30", "30+")
      ),
      "fare_amount" -> (
        Seq(5.0, 10.0, 20.0, 40.0),
        Seq("0-5", "5-10", "10-20", "20-40", "40+")
      ),
    )

    val binnedDf = binConfigs.foldLeft(cachedDf) {
      case (currentDf, (feature, (bins, labels))) =>
        BinningHelper.withBinnedColumn(currentDf, feature, s"${feature}_bin", bins, labels)
    }

    val allBinColumns = Seq(
      "fare_amount_bin",
      "trip_distance_bin",
      "trip_duration_min_bin",
      "trip_hour_bucket"
    )

    val allCombinationsImpact = binnedDf
      .groupBy(allBinColumns.map(col): _*)
      .agg(avg("tip_amount").alias("avg_tip"))
      .orderBy(allBinColumns.map(col): _*)

    allCombinationsImpact.show()

    allCombinationsImpact
      .coalesce(1)
      .write
      .option("header", "true")
      .csv("./output/avg_tip_by_bin_combinations")

  }
}
