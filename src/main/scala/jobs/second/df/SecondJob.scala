package jobs.second.df

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
  private val outputDir = "/output/secondJob"
  private val yellowCab = s"$datasetFolder/yellow_cab"
  private val greenCab = s"$datasetFolder/green_cab"

  private val binConfigs = Map(
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
    "tip_percentage" -> (
      Seq(5.0, 10.0, 20.0, 30.0),
      Seq("0-5%", "5-10%", "10-20%", "20-30%", "30%+")
    ),
    "speed_mph" -> (
      Seq(5.0, 15.0, 30.0),
      Seq("0-5mph", "5-15mph", "15-30mph", "30mph+")
    )
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Second job")
      .getOrCreate()

    if (args.isEmpty) {
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)

    /**
     * Is possible to find a data dictionary at the following link:
     *
     * yellow: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
     * green: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
     */


    /**
     * Step (1)
     *
     * Here are loaded the dataset from both the yellow and green taxi.
     * Both dataset have columns that represent the pickup and drop off of the passengers, these are prefixed with
     * tpep_* for yellow taxi and lpep_* for green taxi.
     * We're aliasing them in order to work uniformly on both remove those prefixes.
     *
     * Finally, we represent data that distinguish both kind of services creating a new column (i.e. service type).
     * The final result is then joined togheter
     */
    val yellowDataset = spark.read
      .option("recursiveFileLookup", "true")
      .parquet(Commons.getDatasetPath(deploymentMode, yellowCab))

    val greenDataset = spark.read
      .option("recursiveFileLookup", "true")
      .parquet(Commons.getDatasetPath(deploymentMode, greenCab))


    val columnsYellow = yellowDataset.select(
      col("VendorID"),
      col("tpep_pickup_datetime").alias("pickup_datetime"),
      col("tpep_dropoff_datetime").alias("dropoff_datetime"),
      col("fare_amount"),
      col("tip_amount"),
      col("payment_type"),
      col("trip_distance"),
      col("total_amount")
    ).withColumn("service_type", lit("yellow"))

    val columnsGreen = greenDataset.select(
      col("VendorID"),
      col("lpep_pickup_datetime").alias("pickup_datetime"),
      col("lpep_dropoff_datetime").alias("dropoff_datetime"),
      col("fare_amount"),
      col("tip_amount"),
      col("payment_type"),
      col("trip_distance"),
      col("total_amount")
    ).withColumn("service_type", lit("green"))

    val joined = columnsYellow.unionByName(columnsGreen)

    /**
     * Step (2)
     *
     * Once we obtain a unified dataset we clean the data.
     *
     * VendorID: Valid 1,2,6,7 for yellow and 1,2,6 for green
     * Fare amount: Only considering rides with a fare amount greater than 0.
     *              Also eliminate anomalies on tips (greater to a certain percentage)
     * Tip amount: Considered all value greater than 0 with also cases where the tip amount is equal to 0 (no tip given)
     * Trip Distance: Considering rides with a number of miles greater than 0. TripDistance is ulterior filtered calculating (with approximation) with
     *                approxQuantiles (https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameStatFunctions.html#approxQuantile(col:String,probabilities:Array%5BDouble%5D,relativeError:Double):Array%5BDouble%5D)
     *                excluding outliers values (<= 0.02 && >= 0.98).
     *
     *
     *
     * dropoff_datetime should be later than the pickup_datetime.
     *
     * Ulterior operations:
     *  - Dropping duplicates
     *  - Dropping na
     *  - Calculate trip_duration_min column, indicating the duration of ride in minutes and filtering with percentiles
     *    (Similar to trip_distance).
     */
    val filtered = joined
      .filter(
        (col("service_type") === "yellow" && col("VendorID").isin(1, 2, 6, 7)) ||
          (col("service_type") === "green" && col("VendorID").isin(1, 2, 6))
      )
      .filter(col("fare_amount") > 0)
      .filter(col("tip_amount") >= 0)
      .filter(col("tip_amount") <= col("fare_amount") * 1.5)
      .filter(col("payment_type").between(1, 6))
      .filter(col("trip_distance") > 0)
      .filter(col("dropoff_datetime") > col("pickup_datetime"))
      .na.drop()
      .dropDuplicates()

    val withTripDuration = filtered.withColumn(
      "trip_duration_min",
      (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
    )

    val distancePercentiles = withTripDuration.stat.approxQuantile("trip_distance", Array(0.02, 0.98), 0.01)
    val tripDurationPercentiles = withTripDuration.stat.approxQuantile("trip_duration_min", Array(0.02, 0.98), 0.01)

    val distanceLowerBound = distancePercentiles(0)
    val distanceUpperBound = distancePercentiles(1)
    val tripDurationLowerBound = tripDurationPercentiles(0)
    val tripDurationUpperBound = tripDurationPercentiles(1)

    val filteredWithoutOutliers = withTripDuration.filter(
      col("trip_distance").between(distanceLowerBound, distanceUpperBound) &&
        col("trip_duration_min").between(tripDurationLowerBound, tripDurationUpperBound)
    )


  /**
   * Step (3)
   *
   * Feature Engineering
   *
   * Obtaining:
   *  -  hour_of_day: Hour of the day when the ride is performed.
   *  -  day_of_week: Day of the week when the ride is performed.
   *  -  is_weekend: Set to true if the day is Saturday or Sunday.
   *  -  trip_hour_bucket: Bucket of hours when rides is performed
   *        (0 , 5]   -> late_night
   *        (6 , 9]   -> morning
   *        (10 , 15] -> midday
   *        (16 , 19] -> evening
   *        (19, 0]   -> night
   *  -  tip_percentage: Percentage of tip given (on the total amount).
   *  -  speed_mph:  Estimates of average speed of the ride.
   *  -  is_rush_hour: Flag set to true if the ride is performed on a rush hour (https://jknylaw.com/blog/worst-traffic-times-in-new-york-city/)
   *  -  is_long_trip: Flag set to true if the record is a long rides (distance over 5 miles or trip duration is over 20 minutes)
   *
   */

    val enrichedDf = filteredWithoutOutliers
      .withColumn("hour_of_day", hour(col("pickup_datetime")))
      .withColumn("day_of_week", date_format(col("pickup_datetime"), "F").cast("int"))
      .withColumn("is_weekend", col("day_of_week").isin(6, 7).cast("int"))
      .withColumn("trip_hour_bucket",
        when(col("hour_of_day").between(0, 5), "late_night")
          .when(col("hour_of_day").between(6, 9), "morning")
          .when(col("hour_of_day").between(10, 15), "midday")
          .when(col("hour_of_day").between(16, 19), "evening")
          .otherwise("night")
      )
      .withColumn("month", month(col("pickup_datetime")))
      .withColumn("year", year(col("pickup_datetime")))
      .withColumn("tip_percentage", (col("tip_amount") / col("total_amount")) * 100)
      .withColumn("speed_mph", col("trip_distance") / (col("trip_duration_min") / 60.0))
      .withColumn("is_rush_hour", col("hour_of_day").isin(8,9,15,16,17,18,19).cast("int"))
      .withColumn("is_long_trip", (col("trip_distance") > 5.0 || col("trip_duration_min") > 20.0).cast("int"))

    val cachedDf: DataFrame = enrichedDf.cache()

    /**
     * Step (4)
     *
     * Analysis of the data
     */
    val binnedDf = binConfigs.foldLeft(cachedDf) {
      case (currentDf, (feature, (bins, labels))) =>
        BinningHelper.withBinnedColumn(currentDf, feature, s"${feature}_bin", bins, labels)
    }

    val allBinColumns = Seq(
      "fare_amount_bin",
      "trip_distance_bin",
      "trip_duration_min_bin",
      "tip_percentage_bin",
      "speed_mph_bin",
      "trip_hour_bucket"
    )

    val allCombinationsImpact = binnedDf
      .groupBy(allBinColumns.map(col): _*)
      .agg(
        avg("tip_amount").alias("avg_tip"),
        avg("trip_duration_min").alias("avg_duration"),
        count("*").alias("count_trips")
      )
      .orderBy(allBinColumns.map(col): _*)

    val rushHourAnalysis = binnedDf
      .groupBy("is_rush_hour")
      .agg(
        avg("trip_duration_min").alias("avg_duration"),
        avg("tip_percentage").alias("avg_tip_pct"),
        avg("speed_mph").alias("avg_speed")
      )

    val monthlyPattern = binnedDf
      .groupBy("year", "month")
      .agg(
        avg("fare_amount").alias("avg_fare"),
        avg("tip_amount").alias("avg_tip"),
        count("*").alias("total_trips")
      )
      .orderBy("year", "month")



    allCombinationsImpact.write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/allCombinationsImpact"))

    rushHourAnalysis.write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/rushHourAnalysis"))

    monthlyPattern.write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/monthlyPattern"))
  }
}
