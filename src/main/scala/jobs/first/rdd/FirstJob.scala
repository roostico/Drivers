package jobs.first.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils._

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{DayOfWeek, Duration, LocalDate, LocalDateTime}
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.HashPartitioner

object FirstJobConfigs {
  val DEBUG: Boolean = false
  val decimals: Int = 4
  val datasetDir = "dataset"
  val outputDir = "output/firstJobOutput"
  val yellowDatasetDir = s"$datasetDir/yellow_cab"
  val greenDatasetDir = s"$datasetDir/green_cab"
  val fhvDatasetDir = s"$datasetDir/fhv_cab"
  val fhvhvDatasetDir = s"$datasetDir/fhvhv_cab"
  val datasetDirMap: Map[String, String] = Map("yellow" -> yellowDatasetDir, "green" -> greenDatasetDir,
    "fhv" -> fhvDatasetDir, "fhvhv" -> fhvhvDatasetDir)
  val datasetIterator: Iterable[(String, String, String)] = Seq(
    ("yellow", "tpep_dropoff_datetime", "tpep_pickup_datetime"),
    ("green", "lpep_dropoff_datetime", "lpep_pickup_datetime"),
    //("fhv", "tpep_dropoff_datetime", "tpep_pickup_datetime"),
    //("fhvhv", "tpep_dropoff_datetime", "tpep_pickup_datetime"),
  )

  val colDurationMinutes: String = "duration_minutes"
  val colDurationMinutesBinLabel: String = "duration_minutes_bin_label"
  val colYear: String = "year"
  val colWeekdaySurcharge: String = "weekday_surcharge"
  val colAggregateFee: String = "fees"
  val colAggregateFeeBin: String = "agg_fee_bin_label"
  val colDistanceBin: String = "distance_bin_label"
  val colFareAmount: String = "fare_amount"
  val colPricePerDistance: String = "cost_per_distance"
  val colPricePerTime: String = "cost_per_time"
  val colAvgPricePerDistance: String = "avg_cost_per_distance"
  val colAvgPricePerTime: String = "avg_cost_per_time"
  val colPricePerDistanceDiff: String = "cost_per_distance_diff"
  val colPricePerDistanceDiffPcg: String = "cost_per_distance_diff_pcg"
  val colPricePerTimeDiff: String = "cost_per_time_diff"
  val colPricePerTimeDiffPcg: String = "cost_per_time_diff_pcg"
  val colPricePerDistanceDiffPcgLabel: String = colPricePerDistanceDiffPcg + "_label"
  val colPricePerTimeDiffPcgLabel: String =  colPricePerTimeDiffPcg + "_label"

  val timeZoneOver: String = "overnight"
  val timeZones = Map(timeZoneOver -> (20, 6), "regular" -> (6, 20))
  val weekDaySurcharge: Double = 2.5

  val colDurationOvernightPcg: String = s"${timeZoneOver}_duration_pcg"

  val colToUse: Set[String] = Set(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee")

  val colFees: Set[String] = Set(
    "extra",
    "mta_tax",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee")

  val featureFilters: Map[String, Any => Boolean] = Map(
    "passenger_count" -> {
      case i: Int => i > 0
      case f: Float => val i = f.toInt; i > 0
      case d: Double => val i = d.toInt; i > 0
      case _ => false
    },
    "trip_distance" -> {
      case i: Int => i > 0
      case i: Float => i > 0
      case i: Double => i > 0
      case _ => false
    },
    "ratecodeid" -> {
      case i: Int => (i >= 1 && i <= 6) || i == 99
      case f: Float => val i = f.toInt; (i >= 1 && i <= 6) || i == 99
      case d: Double => val i = d.toInt; (i >= 1 && i <= 6) || i == 99
      case _ => false
    },
    "store_and_fwd_flag" -> {
      case i: String => i == "Y" || i == "N"
      case _ => false
    },
    "payment_type" -> {
      case i: Int => i >= 1 && i <= 6
      case f: Float => val i = f.toInt; i >= 1 && i <= 6
      case d: Double => val i = d.toInt; i >= 1 && i <= 6
      case _ => false
    },
    "fare_amount" -> {
      case i: Int => i > 0
      case i: Float => i > 0
      case i: Double => i > 0
      case _ => false
    },
    "tolls_amount" -> {
      case i: Int => i >= 0 && i < 200
      case i: Float => i >= 0 && i < 200
      case i: Double => i >= 0 && i < 200
      case _ => false
    }
  )

  def taxFilter(tax: Any): Boolean = {
    tax match {
      case tax: Int => tax >= 0 && tax < 20
      case tax: Float => tax >= 0 && tax < 20
      case tax: Double => tax >= 0 && tax < 20
      case _ => false
    }
  }

  val colsForClassification: Seq[String] = Seq(
    "passenger_count",
    "store_and_fwd_flag",
    "payment_type",
    colAggregateFeeBin,
    colDurationMinutesBinLabel,
    colDistanceBin,
    colYear,
    s"${colDurationOvernightPcg}_label",
    colPricePerDistanceDiffPcgLabel,
    colPricePerTimeDiffPcgLabel
  )
  val colsForValuesAnalysis: Seq[String] = Seq(
    "passenger_count",
    "store_and_fwd_flag",
    "payment_type",
    colAggregateFeeBin,
    colDurationMinutesBinLabel,
    colDistanceBin,
    colYear,
    s"${colDurationOvernightPcg}_label",
  )
}

object ProcessRDD {

  import FirstJobConfigs.decimals

  def binColByStepValue(rdd: RDD[Row], indexOfColToDiscrete: Int, stepValue: Int = 5): RDD[Row] = {
    rdd.map { row =>
      val value: Double = row.get(indexOfColToDiscrete) match {
        case i: Int => i.toDouble
        case d: Double => d
        case l: Long => l.toDouble
        case s: String => try { s.toDouble } catch { case _: Throwable => Double.NaN }
        case _ => Double.NaN
      }

      val rawBin = (value / stepValue).toInt * stepValue

      val binBase = if (value < 0 && value % stepValue == 0) rawBin + stepValue else rawBin

      val label = if (value < 0) {
        s"[${(binBase - stepValue).toInt}|${binBase.toInt})"
      } else {
        s"[${binBase.toInt}|${(binBase + stepValue).toInt})"
      }

      Row.fromSeq(row.toSeq :+ label)
    }
  }

  def castForFilter(value: Any): Any = value match {
    case s: String =>
      // Try casting to Int or Double, otherwise keep as String
      if (s.matches("""^-?\d+\.\d+$""")) s.toDouble
      else if (s.matches("""^-?\d+$""")) s.toInt
      else s.trim
    case d: Double => d
    case i: Int => i
    case l: Long => l.toDouble
    case f: Float => f.toDouble
    case b: Boolean => b
    case null => null
    case other => other.toString.trim
  }

  def preciseBucketUDF(timeZones: Map[String, (Int, Int)], start: LocalDateTime, end: LocalDateTime): Map[String, Double] = {
    var result = timeZones.keys.map(_ -> 0.0).toMap

    if (!(start == null || end == null)) {

      if (!end.isBefore(start)) {

        var current = start.toLocalDate.atStartOfDay

        def overlap(start1: LocalDateTime, end1: LocalDateTime,
                    start2: LocalDateTime, end2: LocalDateTime): Double = {
          val overlapStart = if (start1.isAfter(start2)) start1 else start2
          val overlapEnd = if (end1.isBefore(end2)) end1 else end2
          if (overlapEnd.isAfter(overlapStart)) BigDecimal(ChronoUnit.MILLIS.between(overlapStart, overlapEnd) / 60000.0).setScale(decimals, RoundingMode.HALF_UP).toDouble   else 0
        }

        while (!current.isAfter(end)) {
          val nextDay = current.plusDays(1)

          timeZones.foreach {
            case (label, (startHour, endHour)) if startHour > endHour =>
              val bucketStartBeforeMidnight = current.withHour(startHour).withMinute(0).withSecond(0).withNano(0)
              val bucketEndBeforeMidnight = current.withHour(23).withMinute(59).withSecond(59)
              val bucketStartAfterMidnight = current.withHour(0).withMinute(0).withSecond(0).withNano(0)
              val bucketEndAfterMidnight = current.withHour(endHour).withMinute(0).withSecond(0).withNano(0)

              val minutesBeforeMidnight = overlap(start, end, bucketStartBeforeMidnight, bucketEndBeforeMidnight)
              val minutesAfterMidnight = overlap(start, end, bucketStartAfterMidnight, bucketEndAfterMidnight)

              result = result.updated(label, result(label) + minutesBeforeMidnight + minutesAfterMidnight)

            case (label, (startHour, endHour)) =>
              val bucketStart = current.withHour(startHour).withMinute(0).withSecond(0).withNano(0)
              val bucketEnd = if (endHour == 24) current.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0)
              else current.withHour(endHour).withMinute(0).withSecond(0).withNano(0)

              val minutes = overlap(start, end, bucketStart, bucketEnd)

              result = result.updated(label, result(label) + minutes)
          }
          current = nextDay
        }
      }
    }
    result
  }

  def isUSHolidayOrWeekend(date: LocalDate): Boolean = {
    val month = date.getMonthValue
    val day = date.getDayOfMonth
    val dayOfWeek = date.getDayOfWeek

    if (month == 7 && day == 4) return true

    if (month == 12 && day == 25) return true

    if (month == 1 && day == 1) return true

    if (month == 9 && dayOfWeek == DayOfWeek.MONDAY && day <= 7) return true

    if (month == 11 && dayOfWeek == DayOfWeek.THURSDAY && (day >= 22 && day <= 28)) {
      val weekOfMonth = (day - 1) / 7 + 1
      if (weekOfMonth == 4) return true
    }

    dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY
  }
}

object DebugRDD {

  def debugDumpRdd[A](rdd: RDD[A], name: String): Unit = {
    rdd.take(1).foreach(row => println(name + " " + row))
  }

  //      val underLimit: Double = -80
  //      val features: Seq[String] = Seq("trip_distance", colDurationMinutes, "fare_amount", "total_amount", colPricePerTime, colAvgPricePerTime, colPricePerTimeDiffPcgLabel, colPricePerDistance, colAvgPricePerDistance, colPricePerDistanceDiffPcgLabel)

  //      debugLim(rddPriceDiffPcgBin,
  //        headersSeq =>
  //          row => row.getAs[Double](headersSeq.indexOf(colPricePerDistanceDiffPcg)) < underLimit ||
  //            row.getAs[Double](headersSeq.indexOf(colPricePerTimeDiffPcg)) < underLimit,
  //        features,
  //        headers
  //      )

  def debugLim(rdd: RDD[Row], filterFunc: Seq[String] => Row => Boolean, features: Seq[String], headers: Seq[String]): Unit = {
    features.foreach(col => print(col + ", "))

    rdd.filter { filterFunc(headers) }
    .map { row =>
      Row.fromSeq(features.map(headers.indexOf(_)).map(row.get))
    }
    .foreach { row =>
      println(row)
    }
  }
}

object FirstJob {

  import FirstJobConfigs._
  import ProcessRDD._
  import DebugRDD._
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("First job")
      .getOrCreate()

    if (args.length == 0) {
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)

    datasetIterator.foreach { ds =>
      val name: String = ds._1
      val dropoff: String = ds._2
      val pickup: String = ds._3

      val dataset = spark.read
        .parquet(Commons.getDatasetPath(deploymentMode, datasetDirMap(name)))

      var headers: Seq[String] = dataset.columns.map(_.toLowerCase)

      val indexesToUse: Seq[Int] = headers.zipWithIndex.collect {
        case (h, i) if colToUse.contains(h.toLowerCase) => i
      }

      val rddCleaned = dataset.rdd.map { row =>
        Row.fromSeq(indexesToUse.map(row.get).map(castForFilter))
      }.filter(!_.toSeq.contains(null))

      headers = headers.filter(head => colToUse.contains(head.toLowerCase))

      val rddPreprocessed = rddCleaned.filter { row =>
        headers.zip(row.toSeq).forall { case (header: String, value) =>
          val taxFilterCondition = if (colFees.contains(header.toLowerCase)) taxFilter(value) else true
          featureFilters.get(header.toLowerCase) match {
            case Some(filterFunc) => taxFilterCondition && filterFunc(value)
            case None => taxFilterCondition // no filter defined for this column, so accept it
          }
        }
      }

      if (DEBUG) {
        debugDumpRdd(rddPreprocessed, "After rddPreprocessed")
      }

      val rddWithDurationAndYear = rddPreprocessed.map { row =>
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm[:ss]")

        val pickupStr = row.getAs[String](headers.indexOf(pickup)).trim
        val dropoffStr = row.getAs[String](headers.indexOf(dropoff)).trim

        val pickupTS = LocalDateTime.parse(pickupStr, formatter)
        val dropoffTS = LocalDateTime.parse(dropoffStr, formatter)
        val durationMillis = Duration.between(pickupTS, dropoffTS).toMillis
        val durationMinutes = BigDecimal(durationMillis / 60000.0)
          .setScale(decimals, RoundingMode.HALF_UP)
          .toDouble

        val pickupYear = pickupTS.getYear

        Row.fromSeq(row.toSeq ++ Seq(durationMinutes, pickupYear))
      }.filter { row =>
        row.getAs[Double](row.toSeq.length-2) > 0.0
      }
      headers = headers ++ Seq(colDurationMinutes, colYear)

      val rddWithDurationBin = binColByStepValue(rddWithDurationAndYear, headers.indexOf(colDurationMinutes), 5)
      headers = headers :+ colDurationMinutesBinLabel

      if (DEBUG) {
        debugDumpRdd(rddWithDurationBin, "After rddWithDurationBin")
      }

      val rddWithTimeZones = rddWithDurationBin.map { row =>
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm[:ss]")

        val timeZonesDuration: Map[String, Double] = preciseBucketUDF(timeZones,
          LocalDateTime.parse(row.getAs[String](headers.indexOf(pickup)).trim, formatter),
          LocalDateTime.parse(row.getAs[String](headers.indexOf(dropoff)).trim, formatter)
        )

        val weekday_surcharge: Double =
          if (isUSHolidayOrWeekend(LocalDateTime.parse(row.getAs[String](headers.indexOf(pickup)).trim, formatter).toLocalDate)) 0
          else weekDaySurcharge

        val colsToAdd: Seq[Double] = timeZones.keys.toSeq.flatMap { tz =>
          val duration = timeZonesDuration.getOrElse(tz, 0.0)
          val totalDuration = row.getAs[Double](headers.indexOf(colDurationMinutes))
          Seq(duration, BigDecimal(duration * 100 / totalDuration).setScale(decimals, RoundingMode.HALF_UP).toDouble)
        }

        Row.fromSeq((row.toSeq ++ colsToAdd) :+ weekday_surcharge)
      }

      val headersToAdd: Seq[String] = timeZones.keys.toSeq.flatMap { tz =>
        Seq(tz + "_duration", tz + "_duration_pcg")
      } :+ colWeekdaySurcharge

      headers = headers ++ headersToAdd

      if (DEBUG) {
        debugDumpRdd(rddWithTimeZones, "After rddWithTimeZones")
      }

      val rddWithAggregateFees = rddWithTimeZones.map { row =>
        val fees = colFees
          .filter(col => headers.contains(col.toLowerCase))
          .map(col => row.getAs[Double](headers.indexOf(col.toLowerCase))).sum

        Row.fromSeq(row.toSeq :+ fees)
      }

      headers = headers :+ colAggregateFee

      if (DEBUG) {
        debugDumpRdd(rddWithAggregateFees, "After rddWithAggregateFees")
      }

      val rddWithAggregateFeesBin = binColByStepValue(rddWithAggregateFees, headers.indexOf(colAggregateFee), 2)

      headers = headers :+ colAggregateFeeBin

      if (DEBUG) {
        debugDumpRdd(rddWithAggregateFeesBin, "After rddWithAggregateFeesBin")
      }

      val rddWithPriceDistanceAndTime = rddWithAggregateFeesBin.map { row =>
        val pricePerTime = Math.round(row.getAs[Double](headers.indexOf(colFareAmount)) / row.getAs[Double](headers.indexOf(colDurationMinutes)) * 100) / 100.0
        val pricePerDistance = Math.round(row.getAs[Double](headers.indexOf(colFareAmount)) / row.getAs[Double](headers.indexOf("trip_distance")) * 100) / 100.0

        Row.fromSeq(row.toSeq ++ Seq(pricePerTime, pricePerDistance))
      }

      headers = headers ++ Seq(colPricePerTime, colPricePerDistance)

      if (DEBUG) {
        debugDumpRdd(rddWithPriceDistanceAndTime, "After rddWithPriceDistanceAndTime")
      }

      val rddDurationBin = binColByStepValue(rddWithPriceDistanceAndTime, headers.indexOf("trip_distance"), 5)

      headers = headers :+ colDistanceBin

      if (DEBUG) {
        debugDumpRdd(rddDurationBin, "After rddDurationBin")
      }

      val rddTZDurationPcgLabel = binColByStepValue(rddDurationBin, headers.indexOf(colDurationOvernightPcg), 5)

      headers = headers :+ (colDurationOvernightPcg + "_label")

      val actualHeader = headers

      if (DEBUG) {
        debugDumpRdd(rddTZDurationPcgLabel, "After rddTZDurationPcgLabel")
      }

      val rddWithKey = rddTZDurationPcgLabel.map { row =>
        val key = colsForClassification
          .filter(col => actualHeader.contains(col.toLowerCase))
          .map(col => row.get(actualHeader.indexOf(col.toLowerCase)))
          .mkString("_")
        (key, row)
      }

      val numPartitions = spark.sparkContext.defaultParallelism
      val partitioner = new HashPartitioner(numPartitions)

      val rddWithKeyForAvg = rddWithKey
        .mapValues { row =>
          val costPerDistance = row.getAs[Double](headers.indexOf(colPricePerDistance))
          val costPerTime = row.getAs[Double](headers.indexOf(colPricePerTime))
          (costPerDistance, costPerTime, 1L)
        }
        .partitionBy(partitioner)
        .persist(StorageLevel.MEMORY_AND_DISK)

      val rddAvgPrices = rddWithKeyForAvg
        .aggregateByKey((0.0, 0.0, 0L))(
          (acc, v) => (acc._1 + v._1, acc._2 + v._2, acc._3 + v._3),
          (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
        )
        .mapValues { case (sumDist, sumTime, count) =>
          val avgDist = BigDecimal(sumDist / count).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
          val avgTime = BigDecimal(sumTime / count).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
          (avgDist, avgTime)
        }
        .filter { case (_, (dist, time)) => dist > 0.0 && time > 0.0 }

      rddWithKeyForAvg.unpersist()

      if (DEBUG) {
        debugDumpRdd(rddAvgPrices, "After rddAvgPrices")
      }

      val broadcastAvgPrices: Broadcast[Map[String, (Double, Double)]] = spark.sparkContext.broadcast(rddAvgPrices.collectAsMap().toMap)

      val rddJoined = rddWithKey.flatMap { case (key, originalRow) =>
        broadcastAvgPrices.value.get(key).map { case (avgCostPerDistance, avgCostPerTime) =>
          Row.fromSeq(originalRow.toSeq ++ Seq(avgCostPerDistance, avgCostPerTime))
        }
      }

      headers = headers ++ Seq(colAvgPricePerDistance, colAvgPricePerTime)

      if (DEBUG) {
        debugDumpRdd(rddJoined, "After rddJoined")
      }

      val rddPriceComparisons = rddJoined.map { row =>
        val priceColsToAdd: Seq[Double] =
          Seq((colPricePerDistance, colAvgPricePerDistance), (colPricePerTime, colAvgPricePerTime))
            .flatMap { case (colPrice, colAvgPrice) =>
              val price = row.getAs[Double](headers.indexOf(colPrice))
              val priceAvg = row.getAs[Double](headers.indexOf(colAvgPrice))
              val priceDiff = BigDecimal(price - priceAvg).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
              val priceDiffPcg = BigDecimal(priceDiff / priceAvg * 100).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble

              Seq(priceDiff, priceDiffPcg)
            }

        Row.fromSeq(row.toSeq ++ priceColsToAdd)
      }

      headers = headers ++ Seq(colPricePerDistanceDiff, colPricePerDistanceDiffPcg, colPricePerTimeDiff, colPricePerTimeDiffPcg)

      if (DEBUG) {
        debugDumpRdd(rddPriceComparisons, "After rddPriceComparisons")
      }

      val rddPriceDiffPcgBin = binColByStepValue(
        binColByStepValue(
          rddPriceComparisons,
          headers.indexOf(colPricePerDistanceDiffPcg), 5),
        headers.indexOf(colPricePerTimeDiffPcg), 5
      )

      headers = headers ++ Seq(colPricePerDistanceDiffPcgLabel, colPricePerTimeDiffPcgLabel)

      if (DEBUG) {
        debugDumpRdd(rddPriceDiffPcgBin, "After rddPriceDiffPcgBin")
      }

      val headersForAnalysis = headers.zipWithIndex.filter(head => colsForClassification.contains(head._1.toLowerCase))

      val headersForAnalysisIdxs = headersForAnalysis.map(_._2)
      val headersForAnalysisCols = headersForAnalysis.map(_._1)

      val rddForAnalysis = rddPriceDiffPcgBin.map { row =>
        Row.fromSeq(headersForAnalysisIdxs.map(row.get))
      }

      val totalCount = rddForAnalysis.count()

      val rddFeatures = colsForValuesAnalysis.map { colName =>
        val groupCols = Seq(colPricePerDistanceDiffPcgLabel, colPricePerTimeDiffPcgLabel) :+ colName

        val grouped = rddForAnalysis
          .map { row =>
            val key = groupCols.map(col => row.get(headersForAnalysisCols.indexOf(col.toLowerCase)))
            (key, 1)
          }
          .reduceByKey(_ + _)
          .map { case (keySeq, count) =>
            val value = keySeq.last.toString
            val costDistLabel = keySeq(0).toString
            val costTimeLabel = keySeq(1).toString
            val pcg = BigDecimal(count.toDouble / totalCount * 100).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
            Row.fromSeq(Seq(colName, value, count, pcg, costDistLabel, costTimeLabel))
          }
        grouped
      }

      if (DEBUG) {
        debugDumpRdd(rddFeatures(0), "After rddFeatures")
      }

      val headersForSchema = Seq(
        StructField("feature", StringType),
        StructField("value", StringType),
        StructField("count", IntegerType),
        StructField("pcg", DoubleType),
        StructField("cost_distance_label", StringType),
        StructField("cost_time_label", StringType)
      )

      val schema = StructType(headersForSchema)

      val dfForAnalysis = spark.createDataFrame(rddFeatures.reduce(_ union _), schema)

      dfForAnalysis
        .write
        .mode("overwrite")
        .parquet(Commons.getDatasetPath(deploymentMode, outputDir + f"/$name"))
    }
  }
}
