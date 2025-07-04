package jobs.second.newV

import jobs.second.rdd.BinningHelperRDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import utils.Commons

import java.sql.Timestamp
import java.time.LocalDate

object SecondJob {

  private val datasetName = "yellow"
  private val datasetFolder = "./dataset"
  private val outputDir = s"/output/secondJobRDD/$datasetName"
  private val pathToFiles = s"$datasetFolder/$datasetName"

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
    StructField("total_amount", DoubleType)
  )

  private val schemaYellow = StructType(
    StructField("tpep_pickup_datetime", TimestampType) ::
      StructField("tpep_dropoff_datetime", TimestampType) ::
      commonFields
  )

  private val allowedYellowVendorId = Set(1, 2, 6, 7)
  private val allowedGreenVendorId = Set(1, 2, 6)

  private val schemaGreen = StructType(
    StructField("lpep_pickup_datetime", TimestampType) ::
      StructField("lpep_dropoff_datetime", TimestampType) ::
      commonFields
  )

  private val binFields = Seq(
    "tripDistanceBin",
    "tripDurationBin",
    "fareAmountBin",
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

    val startingTime = System.currentTimeMillis()


    /**
     * Loading main dataset for taxi
     */

    val (schema, pickupCol, dropoffCol) = datasetName match {
      case "yellow" => (schemaYellow, "tpep_pickup_datetime", "tpep_dropoff_datetime")
      case _        => (schemaGreen, "lpep_pickup_datetime", "lpep_dropoff_datetime")
    }

    val loadedDataset = spark.read
      .schema(schema)
      .option("recursiveFileLookup", "true")
      .parquet(Commons.getDatasetPath(deploymentMode, pathToFiles))
      .select(
        $"VendorID",
        col(pickupCol).alias("pickup_datetime"),
        col(dropoffCol).alias("dropoff_datetime"),
        $"fare_amount",
        $"tip_amount",
        $"payment_type",
        $"trip_distance",
        $"total_amount"
      )
      .na.drop()
      .dropDuplicates()
      .rdd


    val filtered = loadedDataset
      .filter { case row =>
        val allowedIds = if (datasetName == "yellow") allowedYellowVendorId else allowedGreenVendorId
        val vendorId = row.getInt(0)
        allowedIds.contains(vendorId)
      }
      .filter(row => row.getDouble(3) > 0)
      .filter(row => row.getDouble(4) >= 0)
      .filter(row => row.getDouble(4) <= row.getDouble(3) * 1.5)
      .filter(row => row.getDouble(6) > 0)
      .filter{ row =>
        val dropOffDateTime = row.getTimestamp(2)
        val pickupDateTime = row.getTimestamp(1)
        dropOffDateTime.after(pickupDateTime)
      }

    val withTripDuration = filtered.map { row =>
      val durationMin = (row.getTimestamp(2).getTime - row.getTimestamp(1).getTime).toDouble / (1000 * 60)
      (row, durationMin)
    }

    val tripDistances = withTripDuration.map { case (row, _) => row.getLong(5).toInt }
    val tripDurations = withTripDuration.map { case (_, duration) => duration }

    // We leverage for a moment on DF
    val distanceDF = tripDistances.toDF("trip_distance")
    val durationDF = tripDurations.toDF("trip_duration")

    val Array(distanceLower, distanceUpper) = distanceDF.stat.approxQuantile("trip_distance", Array(0.02, 0.98), 0.01)
    val Array(durationLower, durationUpper) = durationDF.stat.approxQuantile("trip_duration", Array(0.02, 0.98), 0.01)
    // Continue to filter

    val enriched = withTripDuration
      .filter { case (row, duration) =>
        val tripDistance = row.getLong(5).toInt
        tripDistance >= distanceLower && tripDistance <= distanceUpper &&
        duration >= durationLower && duration <= durationUpper
      }
      .map { case (row, duration) =>
        val pickupCalendar = java.util.Calendar.getInstance()
        pickupCalendar.setTime(row.getTimestamp(1))

        val hourOfDay = pickupCalendar.get(java.util.Calendar.HOUR_OF_DAY)

        val tipAmount = row.getDouble(4)
        val totalAmount = row.getDouble(7)

        val tipPercentage = if (totalAmount != 0) (tipAmount / totalAmount) * 100 else 0.0

        val tripDistance = row.getDouble(6)
        val speedMph = if (duration > 0) tripDistance / (duration / 60.0) else 0.0

        (row, duration, hourOfDay, tipPercentage, speedMph)
      }


    val binned =  enriched.map { case (row, duration, hourOfDay, tipPercentage, speedMph) =>
      val tripDistance = row.getDouble(6)
      val tripDistanceBin = BinningHelperRDD.assignBin(
          tripDistance,
          binConfigs("trip_distance")._1,
          binConfigs("trip_distance")._2
      )

      val tripDurationBin = BinningHelperRDD.assignBin(
          duration,
          binConfigs("trip_duration_min")._1,
          binConfigs("trip_duration_min")._2
      )

      val fareAmount = row.getDouble(3)
      val fareAmountBin = BinningHelperRDD.assignBin(
        fareAmount,
        binConfigs("fare_amount")._1,
        binConfigs("fare_amount")._2
      )

      val speedBin = BinningHelperRDD.assignBin(
        speedMph,
        binConfigs("speed_mph")._1,
        binConfigs("speed_mph")._2
      )

      val hourBin = BinningHelperRDD.tripHourBucket(hourOfDay)
      (row, tripDistanceBin, tripDurationBin, fareAmountBin, speedBin, hourBin, tipPercentage)
    }

    /**
     * Starting to parse weather Data
     *  -> lookup: WMO Val, Description
     *  -> DataFile: WMO Val, Date
     */

    val weatherFileRDD = spark.read
      .format("CSV")
      .option("header", "true")
      .load(Commons.getDatasetPath(deploymentMode, weatherData))
      .rdd
      .map { row =>
        val code = row.getString(1).trim.toInt
        val date = row.getString(0).trim
        (code, date)
      }

    val wmoLookupFile = spark.read
      .format("CSV")
      .option("header", "true")
      .load(Commons.getDatasetPath(deploymentMode, weatherWmoLookup))
      .rdd

    val wmoLookupPairRDD = wmoLookupFile.map { row =>
      val data = row.getString(0).split(";")
      val code =data(0).trim.toInt
      val description = data(1).trim
      (code, description)
    }

    val transformedWeatherClassRDD = weatherFileRDD
      .join(wmoLookupPairRDD)
      .map(row => {
        val (id, (date, description)) = row

        val formattedDate  = LocalDate.parse(date)
        val timestamp = Timestamp.valueOf(formattedDate.atStartOfDay())

        (id, timestamp, description)
      })

    val weatherByDate = transformedWeatherClassRDD.map { row =>
      (row._2.toLocalDateTime.toLocalDate, row)
    }

    val rideByDate = binned.map {data  =>
      val pickupDateTime = data._1.getTimestamp(1).toLocalDateTime.toLocalDate
      (pickupDateTime, data)
    }

    val joinedWeather = rideByDate.join(weatherByDate).map {
      case (_, (ride, weather)) => (ride, weather)
    }

    val finalRDD = joinedWeather.map { case (ride, weather) =>
      val generalWeather = BinningHelperRDD.generalWeatherLabel(weather._1)
      (ride, weather._1, generalWeather)
    }

    /**
     * Obtaining Results
     */
    val keyedRDD = finalRDD.flatMap{ case (ride, code, generalWeather) =>
      binFields.map { field =>
        val bin = field match {
          case "fareAmountBin" => ride._4
          case "tripDistanceBin" => ride._2
          case "tripDurationBin" => ride._3
          case "speedBin" => ride._5
        }
        val key = s"${field}_$bin"
        (key, ride)
      }
    }

//    375668
//    import org.apache.spark.HashPartitioner
//    import org.apache.spark.storage.StorageLevel
//
//    val numPartitions = spark.sparkContext.defaultParallelism
//    val partitioner = new HashPartitioner(numPartitions)

    val distributedKeyedRDD = keyedRDD.mapValues{ case ride =>
      val tipPercentage = ride._7
      (tipPercentage, 1L)
    }
//    .partitionBy(partitioner)
//    .persist(StorageLevel.MEMORY_ONLY)

    val avgTipRDD = distributedKeyedRDD
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map {
        case (id, (sum, count)) => Row(id, sum / count)
      }

    val allTipByBinSchema = StructType(Seq(
      StructField("feature", StringType),
      StructField("avg_tip_pct", DoubleType)
    ))

    spark.createDataFrame(avgTipRDD, allTipByBinSchema)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/tip_avg_per_bin/all_features"))

    val avgTipByWeather = finalRDD
      .map(r => (r._3, (r._1._7, 1L)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (weather, (sumTip, count)) => Row(weather, sumTip / count) }

    val weatherSchema = StructType(Seq(
      StructField("weather", StringType),
      StructField("avg_tip_pct", DoubleType)
    ))

    spark.createDataFrame(avgTipByWeather, weatherSchema)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/avg_tip_by_weather"))

    val tipByHourBucket = finalRDD
      .map(r => (r._1._6, (r._1._7, 1L)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (bucket, (sumTip, count)) => Row(bucket, sumTip / count) }

    val bucketSchema = StructType(Seq(
      StructField("hour_bucket", StringType),
      StructField("avg_tip_pct", DoubleType)
    ))

    spark.createDataFrame(tipByHourBucket, bucketSchema)
      .write
      .mode("overwrite")
      .parquet(Commons.getDatasetPath(deploymentMode, s"$outputDir/avg_tip_by_hour_bucket"))


    val endTime = System.currentTimeMillis()
    println(s"elapsed ${endTime - startingTime}")
  }


}
