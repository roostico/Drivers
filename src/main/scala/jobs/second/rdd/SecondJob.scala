package jobs.second.rdd

import jobs.second.rdd.DataClasses.{RideWithBins, RideWithEnrichedInformation}
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


  private val valueExtractor : Map[String, RideWithBins => Any] =
    Map(
      "trip_distance" -> (_.enrichedInfo.rideWithMinutes.info.tripDistance),
      "speed_mph" -> (_.enrichedInfo.speedMph),
      "passenger_count" -> (_.enrichedInfo.rideWithMinutes.info.passengerCount),
      "trip_duration_min" -> (_.enrichedInfo.rideWithMinutes.durationMinutes)
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

    val allCombinationsImpact = binned
      .map { bins =>
        val key = (
          bins.fareAmountBin,
          bins.tripDistanceBin,
          bins.tripDurationBin,
          bins.tipPercentageBin,
          bins.speedBin,
          bins.enrichedInfo.tripHourBucket
        )
        val value = (
          bins.enrichedInfo.rideWithMinutes.info.tipAmount,
          bins.enrichedInfo.rideWithMinutes.durationMinutes,
          1L
        )
        (key, value)
      }
      .reduceByKey { case ((tip1, duration1, count1), (tip2, duration2, count2)) =>
        (tip1 + tip2, duration1 + duration2, count1 + count2)
      }
      .map { case (key, (sumTip, sumDuration, count)) =>
        (
          key._1, key._2, key._3, key._4, key._5, key._6,
          sumTip / count,
          sumDuration / count
        )
      }

    val rushHourAnalysis = binned
      .map { ride =>
        val key = ride.enrichedInfo.isRushHour
        val value = (
          ride.enrichedInfo.rideWithMinutes.durationMinutes,
          ride.enrichedInfo.tipPercentage,
          ride.enrichedInfo.speedMph,
          1L
        )
        (key, value)
      }
      .reduceByKey { case ((duration1, tipPct1, speed1, count1), (duration2, tipPct2, speed2, count2)) =>
        (duration1 + duration2, tipPct1 + tipPct2, speed1 + speed2, count1 + count2)
      }
      .map { case (isRushHour, (sumDuration, sumTipPct, sumSpeed, count)) =>
        (
          isRushHour,
          sumDuration / count,
          sumTipPct / count,
          sumSpeed / count
        )
      }

    val monthlyPattern = binned
      .map { ride =>
        val key = (ride.enrichedInfo.year, ride.enrichedInfo.monthOfYear)
        val value = (
          ride.enrichedInfo.rideWithMinutes.info.fareAmount,
          ride.enrichedInfo.rideWithMinutes.info.tipAmount,
          1L
        )
        (key, value)
      }
      .reduceByKey { case ((fare1, tip1, count1), (fare2, tip2, count2)) =>
        (fare1 + fare2, tip1 + tip2, count1 + count2)
      }
      .map { case ((year, month), (sumFare, sumTip, count)) =>
        (
          year, month,
          sumFare / count,
          sumTip / count,
          count
        )
      }


//    binned.flatMap { rwb =>
//      val extractValuesWithTip : (RideWithBins, String) => Option[(Any, Double)] = (rwb, col) =>
//        for {
//          extractor <- valueExtractor.get(col)
//          value = extractor(rwb)
//          tip = rwb.enrichedInfo.rideWithMinutes.info.tipAmount
//        } yield (value, tip)
//
//
//      valueExtractor.keySet.flatMap { column =>
//        extractValuesWithTip(rwb, column).map {
//          case (value, tipAmount) => (column, value, tipAmount)
//        }
//      }
//    }
//    .map { case (column, value, tip) => ((column, value), (tip, 1)) }
//    .reduceByKey { case ((sum1, count1), (sum2, count2)) =>
//      (sum1 + sum2, count1 + count2)
//    }
//    .mapValues { case (sum, count) => sum / count }
//    .map {
//      case ((column, bin), avgTip) =>
//        (column, bin, avgTip)
//    }
//    .foreach(println)


    //val perColumnValAndTipDF = perColumnValAndTip.toDF("column", "bin", "avg_tip")

    val allCombinationsImpactDf = allCombinationsImpact.toDF(
      "fare_amount_bin",
      "trip_distance_bin",
      "trip_duration_min_bin",
      "tip_percentage_bin",
      "speed_mph_bin",
      "trip_hour_bucket",
      "avg_tip",
      "avg_duration"
    )

    val rushHourAnalysisDf = rushHourAnalysis.toDF(
      "is_rush_hour",
      "avg_duration",
      "avg_tip_pct",
      "avg_speed"
    )

    val monthlyPatternDf = monthlyPattern.toDF(
      "year",
      "month",
      "avg_fare",
      "avg_tip",
      "total_trips"
    )

//    perColumnValAndTipDF
//      .coalesce(1)
//      .write
//      .mode("overwrite")
//      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/perColumnValAndTipDF"))

    allCombinationsImpactDf
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/allCombinationsImpact"))

    rushHourAnalysisDf
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/rushHourAnalysis"))

    monthlyPatternDf
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/monthlyPattern"))
  }
}
