package jobs.second.rdd

import jobs.second.rdd.DataClasses.{RideFinalOutput, RideWithBins, RideWithEnrichedInformation, RideWithWeather}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import utils.Commons



object BinningHelperRDD {

  def assignBin(value: Double, bins: Seq[Double], labels: Seq[String]): String = {
    require(labels.length == bins.length + 1, "You need one more label than bin thresholds.")

    if (value < bins.head) labels.head
    else {
      val idx = bins.indexWhere(b => value < b)
      if (idx == -1) labels.last
      else labels(idx)
    }
  }

  def generalWeatherLabel(wmoCode: Int): String = wmoCode match {
    case c if Seq(0, 1).contains(c)        => "clear"
    case c if Seq(2, 3, 4).contains(c)     => "cloudy"
    case c if Seq(45, 48).contains(c)      => "foggy"
    case c if (50 to 67).contains(c)       => "rainy"
    case c if (70 to 77).contains(c)       => "snowy"
    case c if (80 to 99).contains(c)       => "stormy"
    case _                                 => "unknown"
  }


}

object UtilFunctions {
  def getQuantile(sortedRDD: org.apache.spark.rdd.RDD[(Long, Double)], quantile: Double, count: Long): Double = {
    val idx = (quantile * count).toLong
    sortedRDD.lookup(idx).headOption.getOrElse(sortedRDD.map(_._2).takeOrdered(1).head)
  }
}

object SecondJob {

  private val datasetFolder = "./dataset"
  private val outputDir = "/output/secondJobRDD"
  private val yellowCab = s"$datasetFolder/yellow_cab"
  private val greenCab = s"$datasetFolder/green_cab"

  private val weatherData = s"$datasetFolder/weather/weather_data_2017_2024.csv"
  private val weatherWmoLookup = s"$datasetFolder/weather/wmo_lookup_codes.csv"

  private val binConfigs = Map(
    "trip_distance" -> (Seq(1.0, 3.0, 6.0), Seq("0-1", "1-3", "3-6", "6+")),
    "trip_duration_min" -> (Seq(5.0, 15.0, 30.0), Seq("0-5", "5-15", "15-30", "30+")),
    "fare_amount" -> (Seq(5.0, 10.0, 20.0, 40.0), Seq("0-5", "5-10", "10-20", "20-40", "40+")),
    "tip_percentage" -> (Seq(5.0, 10.0, 20.0, 30.0), Seq("0-5%", "5-10%", "10-20%", "20-30%", "30%+")),
    "speed_mph" -> (Seq(5.0, 15.0, 30.0), Seq("0-5mph", "5-15mph", "15-30mph", "30mph+"))
  )

  private val commonFields = List(
    StructField("VendorID", IntegerType),
    StructField("fare_amount", DoubleType),
    StructField("tip_amount", DoubleType),
    StructField("payment_type", LongType),
    StructField("trip_distance", DoubleType),
    StructField("total_amount", DoubleType),
    StructField("passenger_count", LongType)
  )

  private val schemaYellow = StructType(
    StructField("tpep_pickup_datetime", TimestampType) ::
    StructField("tpep_dropoff_datetime", TimestampType) ::
    commonFields
  )

  private val schemaGreen = StructType(
    StructField("lpep_pickup_datetime", TimestampType) ::
    StructField("lpep_dropoff_datetime", TimestampType) ::
    commonFields
  )

  private val binFields = Seq(
    "tripDistanceBin",
    "tripDurationBin",
    "fareAmountBin",
    "tipPercentageBin",
    "speedBin"
  )


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Second job with RDDs")
      .getOrCreate()

    import spark.implicits._

    if (args.isEmpty) {
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)

    val weatherFileRDD = spark.read
      .format("CSV")
      .option("header", "true")
      .load(Commons.getDatasetPath(deploymentMode, weatherData))
      .rdd

    // --> WMO Val, Date
    val weatherPairRDD = weatherFileRDD.map { row =>
      val code = row.getString(1).trim.toInt
      val date = row.getString(0).trim
      (code, date)
    }

    val wmoLookupFile = spark.read
      .format("CSV")
      .option("header", "true")
      .load(Commons.getDatasetPath(deploymentMode, weatherWmoLookup))
      .rdd


    // --> WMO Val, Description
    val wmoLookupPairRDD = wmoLookupFile.map { row =>
      // Row is presenting like the following : (0;something)
      val data = row.getString(0).split(";")
      val code =data(0).trim.toInt
      val description = data(1).trim
      (code, description)
    }


    import DataClasses.WeatherInfo
    import java.time.LocalDate
    import java.sql.Timestamp

    val transformedWeatherClassRDD = weatherPairRDD
      .join(wmoLookupPairRDD)
      .map(row => {
        val (id, (date, description)) = row

        val formattedDate  = LocalDate.parse(date)
        val timestamp = Timestamp.valueOf(formattedDate.atStartOfDay())

        WeatherInfo(id, timestamp, description)
      })



    import DataClasses.Ride

    val yellowDataset = spark.read
      .schema(schemaYellow)
      .option("recursiveFileLookup", "true")
      .parquet(Commons.getDatasetPath(deploymentMode, yellowCab))
      .select(
        $"VendorID",
        $"tpep_pickup_datetime".alias("pickup_datetime"),
        $"tpep_dropoff_datetime".alias("dropoff_datetime"),
        $"fare_amount",
        $"tip_amount",
        $"payment_type",
        $"trip_distance",
        $"total_amount",
        $"passenger_count"
      )
      .na.drop()
      .dropDuplicates()
      .rdd
      .map(r => Ride(
        r.getInt(0),
        r.getTimestamp(1),
        r.getTimestamp(2),
        r.getDouble(3),
        r.getDouble(4),
        r.getLong(5).toInt,
        r.getDouble(6),
        r.getDouble(7),
        r.getLong(8).toInt,
        "yellow"
      ))

    val greenDataset = spark.read
      .schema(schemaGreen)
      .option("recursiveFileLookup", "true")
      .parquet(Commons.getDatasetPath(deploymentMode, greenCab))
      .select(
        $"VendorID",
        $"lpep_pickup_datetime".alias("pickup_datetime"),
        $"lpep_dropoff_datetime".alias("dropoff_datetime"),
        $"fare_amount",
        $"tip_amount",
        $"payment_type",
        $"trip_distance",
        $"total_amount",
        $"passenger_count"
      )
      .na.drop()
      .dropDuplicates()
      .rdd
      .map(r => Ride(
        r.getInt(0),
        r.getTimestamp(1),
        r.getTimestamp(2),
        r.getDouble(3),
        r.getDouble(4),
        r.getLong(5).toInt,
        r.getDouble(6),
        r.getDouble(7),
        r.getLong(8).toInt,
        "green"
      ))

    val joined = yellowDataset.union(greenDataset)

    val filtered = joined
      .filter(ride =>
        (ride.serviceType == "yellow" && Set(1, 2, 6, 7).contains(ride.vendorId)) ||
          (ride.serviceType == "green" && Set(1, 2, 6).contains(ride.vendorId))
      )
      .filter(ride => ride.fareAmount > 0)
      .filter(ride => ride.tipAmount >= 0)
      .filter(ride => ride.tipAmount <= ride.fareAmount * 1.5)
      .filter(ride => ride.paymentType >= 1 && ride.paymentType <= 6)
      .filter(ride => ride.tripDistance > 0)
      .filter(ride => ride.dropoffDatetime.after(ride.pickupDatetime))

    import DataClasses.RideWithDurationMinutes

    val withTripDuration = filtered.map(ride => {
      val durationMin = (ride.dropoffDatetime.getTime - ride.pickupDatetime.getTime).toDouble / (1000 * 60)
      RideWithDurationMinutes(ride, durationMin)
    })

    val tripDistances = withTripDuration.map { case ride => ride.info.tripDistance }
    val tripDurations = withTripDuration.map { case ride => ride.durationMinutes }

    val distanceSorted = tripDistances.sortBy(identity).zipWithIndex().map(_.swap)
    val durationSorted = tripDurations.sortBy(identity).zipWithIndex().map(_.swap)


    val count = withTripDuration.count()
    val distanceLower = UtilFunctions.getQuantile(distanceSorted, 0.02, count)
    val distanceUpper = UtilFunctions.getQuantile(distanceSorted, 0.98, count)
    val durationLower = UtilFunctions.getQuantile(durationSorted, 0.02, count)
    val durationUpper = UtilFunctions.getQuantile(durationSorted, 0.98, count)

    val filteredWithoutOutliers = withTripDuration.filter { case ride =>
      ride.info.tripDistance >= distanceLower && ride.info.tripDistance <= distanceUpper &&
        ride.durationMinutes >= durationLower && ride.durationMinutes <= durationUpper
    }

    val enriched = filteredWithoutOutliers.map { case ride =>
      val pickupCalendar = java.util.Calendar.getInstance()
      pickupCalendar.setTime(ride.info.pickupDatetime)

      val hourOfDay = pickupCalendar.get(java.util.Calendar.HOUR_OF_DAY)
      val dayOfWeek = pickupCalendar.get(java.util.Calendar.DAY_OF_WEEK)
      val monthOfYear = pickupCalendar.get(java.util.Calendar.MONTH)
      val year = pickupCalendar.get(java.util.Calendar.YEAR)

      val isWeekend = if (dayOfWeek == java.util.Calendar.SATURDAY || dayOfWeek == java.util.Calendar.SUNDAY) 1 else 0

      val tripHourBucket = hourOfDay match {
        case h if h >= 0 && h <= 5  => "late_night"
        case h if h >= 6 && h <= 9  => "morning"
        case h if h >= 10 && h <= 15 => "midday"
        case h if h >= 16 && h <= 19 => "evening"
        case _ => "night"
      }

      val tipPercentage = if (ride.info.totalAmount != 0) (ride.info.tipAmount / ride.info.totalAmount) * 100 else 0.0
      val speedMph = if (ride.durationMinutes > 0) ride.info.tripDistance / (ride.durationMinutes / 60.0) else 0.0

      val isRushHour = (dayOfWeek >= java.util.Calendar.MONDAY && dayOfWeek <= java.util.Calendar.FRIDAY) &&
        ((hourOfDay >= 7 && hourOfDay <= 9) || (hourOfDay >= 16 && hourOfDay <= 18))

      val isLongTrip = ride.info.tripDistance > 5.0 || ride.durationMinutes  > 20.0

      RideWithEnrichedInformation(
        ride,
        hourOfDay,
        dayOfWeek,
        monthOfYear,
        year,
        isWeekend,
        tripHourBucket,
        tipPercentage,
        speedMph,
        isRushHour,
        isLongTrip
      )
    }

    val binned = enriched.map {
      case ride =>

        val tripDistanceBin = BinningHelperRDD.assignBin(
          ride.rideWithMinutes.info.tripDistance,
          binConfigs("trip_distance")._1,
          binConfigs("trip_distance")._2
        )

        val tripDurationMin =
          (
            ride.rideWithMinutes.info.dropoffDatetime.getTime - ride.rideWithMinutes.info.pickupDatetime.getTime
          ).toDouble / (1000 * 60)

        val tripDurationBin = BinningHelperRDD.assignBin(
          tripDurationMin,
          binConfigs("trip_duration_min")._1,
          binConfigs("trip_duration_min")._2
        )

        val fareAmountBin = BinningHelperRDD.assignBin(
          ride.rideWithMinutes.info.fareAmount,
          binConfigs("fare_amount")._1,
          binConfigs("fare_amount")._2
        )

        val tipPercentageBin = BinningHelperRDD.assignBin(
          ride.tipPercentage,
          binConfigs("tip_percentage")._1,
          binConfigs("tip_percentage")._2
        )
        val speedBin = BinningHelperRDD.assignBin(
          ride.speedMph,
          binConfigs("speed_mph")._1,
          binConfigs("speed_mph")._2
        )

        RideWithBins(
          ride,
          tripDistanceBin,
          tripDurationBin,
          fareAmountBin,
          tipPercentageBin,
          speedBin)
    }

    val weatherByDate = transformedWeatherClassRDD.map(w => (w.dateOfRelevation.toLocalDateTime.toLocalDate, w))
    val rideByDate = binned.map(r => (r.enrichedInfo.rideWithMinutes.info.pickupDatetime.toLocalDateTime.toLocalDate, r))

    val joinedWeather = rideByDate.join(weatherByDate).map {
      case (_, (ride, weather)) =>
        RideWithWeather(ride, weather)
    }

    val finalRDD = joinedWeather.map { r =>
      val generalWeather = BinningHelperRDD.generalWeatherLabel(r.weatherInfo.wmoCode)
      RideFinalOutput(r.ride, r.weatherInfo, generalWeather)
    }

    finalRDD.cache()

    val binFieldPairs = for {
      x <- binFields
      y <- binFields
      if x != y
    } yield (x, y)

    val combinationRDD: RDD[Row] = finalRDD.flatMap { row =>
        binFieldPairs.map { case (x, y) =>
          val matchVal : String => String = {
            case "tripDistanceBin" => row.ride.tripDistanceBin
            case "tripDurationBin" => row.ride.tripDurationBin
            case "fareAmountBin" => row.ride.fareAmountBin
            case "tipPercentageBin" => row.ride.tipPercentageBin
            case "speedBin" => row.ride.speedBin
          }

          val binX = matchVal(x)
          val binY = matchVal(y)

          ((x, y, binX, binY), (row.ride.enrichedInfo.tipPercentage, 1L))
        }
    }
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .map { case ((fieldX, fieldY, binX, binY), (sumTip, count)) =>
        Row(fieldX, fieldY, binX, binY, sumTip / count)
    }


    val allTipByBinRDD: RDD[Row] = finalRDD.flatMap { row =>
        binFields.map { binFeature =>
          val bin = binFeature match {
            case "fareAmountBin" => row.ride.fareAmountBin
            case "tripDistanceBin" => row.ride.tripDistanceBin
            case "tripDurationBin" => row.ride.tripDurationBin
            case "tipPercentageBin" => row.ride.tipPercentageBin
            case "speedBin" => row.ride.speedBin
          }
          ((binFeature, bin), (row.ride.enrichedInfo.tipPercentage, 1L))
        }
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case ((feature, bin), (sumTip, count)) =>
        Row(feature, bin, sumTip / count)
      }

    val allTipByBinSchema = StructType(Seq(
      StructField("feature", StringType),
      StructField("bin", StringType),
      StructField("avg_tip_pct", DoubleType)
    ))

    spark.createDataFrame(allTipByBinRDD, allTipByBinSchema)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/tip_avg_per_bin/all_features"))

    val schema = StructType(Seq(
      StructField("featureX", StringType),
      StructField("featureY", StringType),
      StructField("binX", StringType),
      StructField("binY", StringType),
      StructField("avg_tip_pct", DoubleType)
    ))

    spark.createDataFrame(combinationRDD, schema)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/combination_data"))

  }
}
