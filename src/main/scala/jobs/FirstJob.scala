package jobs

import jobs.DebugRDD.debugLim
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import java.time.{DayOfWeek, Duration, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Row, SparkSession}
import utils._

import java.time.temporal.ChronoUnit
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode

object FirstJobConfigs {
  val datasetDir = "/dataset"
  val outputDir = "/output/firstJobOutput"
  val yellowDatasetDir = s"$datasetDir/dataset_yellow"
  val greenDatasetDir = s"$datasetDir/dataset_green"
  val fhvDatasetDir = s"$datasetDir/dataset_fhv"
  val fhvhvDatasetDir = s"$datasetDir/dataset_fhvhv"
  val datasetDirMap: Map[String, String] = Map("yellow" -> yellowDatasetDir, "green" -> greenDatasetDir,
    "fhv" -> fhvDatasetDir, "fhvhv" -> fhvhvDatasetDir)
  val datasetIterator: Iterable[(String, String, String)] = Seq(
    ("green", "lpep_dropoff_datetime", "lpep_pickup_datetime"),
    ("yellow", "tpep_dropoff_datetime", "tpep_pickup_datetime"),
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

  val sparkFilters: Map[String, Any => Boolean] = Map(
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

  def binColByStepValue(rdd: RDD[Row], indexOfColToDiscrete: Int, stepValue: Int = 5): RDD[Row] = {
    rdd.map { row =>
      val value: Double = row.get(indexOfColToDiscrete) match {
        case i: Int => i.toDouble
        case d: Double => d
        case l: Long => l.toDouble
        case s: String => try { s.toDouble } catch { case _ => Double.NaN }
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
          if (overlapEnd.isAfter(overlapStart)) BigDecimal(ChronoUnit.MILLIS.between(overlapStart, overlapEnd) / 60000.0).setScale(2, RoundingMode.HALF_UP).toDouble   else 0
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

  import ProcessRDD._
  import FirstJobConfigs._
  
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
          sparkFilters.get(header.toLowerCase) match {
            case Some(filterFunc) => taxFilterCondition && filterFunc(value)
            case None => taxFilterCondition // no filter defined for this column, so accept it
          }
        }
      }

      val rddWithDurationAndYear = rddPreprocessed.map { row =>
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm[:ss]")

        val pickupStr = row.getAs[String](headers.indexOf(pickup)).trim
        val dropoffStr = row.getAs[String](headers.indexOf(dropoff)).trim

        val pickupTS = LocalDateTime.parse(pickupStr, formatter)
        val dropoffTS = LocalDateTime.parse(dropoffStr, formatter)
        val durationMillis = Duration.between(pickupTS, dropoffTS).toMillis
        val durationMinutes = BigDecimal(durationMillis / 60000.0)
          .setScale(2, RoundingMode.HALF_UP)
          .toDouble

        val pickupYear = pickupTS.getYear

        Row.fromSeq(row.toSeq ++ Seq(durationMinutes, pickupYear))
      }.filter { row =>
        row.getAs[Double](row.toSeq.length-2) > 0.0
      }
      headers = headers ++ Seq(colDurationMinutes, colYear)

      val rddWithDurationBin = binColByStepValue(rddWithDurationAndYear, headers.indexOf(colDurationMinutes), 5)
      headers = headers :+ colDurationMinutesBinLabel

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
          Seq(duration, BigDecimal(duration * 100 / totalDuration).setScale(2, RoundingMode.HALF_UP).toDouble)
        }

        Row.fromSeq((row.toSeq ++ colsToAdd) :+ weekday_surcharge)
      }

      val headersToAdd: Seq[String] = timeZones.keys.toSeq.flatMap { tz =>
        Seq(tz + "_duration", tz + "_duration_pcg")
      } :+ colWeekdaySurcharge

      headers = headers ++ headersToAdd

      val rddWithAggregateFees = rddWithTimeZones.map { row =>
        val fees = colFees.filter(col =>
          headers.contains(col.toLowerCase)).map(col => row.getAs[Double](headers.indexOf(col.toLowerCase))).sum

        Row.fromSeq(row.toSeq :+ fees)
      }

      headers = headers :+ colAggregateFee

      val rddWithAggregateFeesBin = binColByStepValue(rddWithAggregateFees, headers.indexOf(colAggregateFee), 2)

      headers = headers :+ colAggregateFeeBin

      val rddWithPriceDistanceAndTime = rddWithAggregateFeesBin.map { row =>
        val pricePerTime = Math.round(row.getAs[Double](headers.indexOf(colFareAmount)) / row.getAs[Double](headers.indexOf(colDurationMinutes)) * 100) / 100.0
        val pricePerDistance = Math.round(row.getAs[Double](headers.indexOf(colFareAmount)) / row.getAs[Double](headers.indexOf("trip_distance")) * 100) / 100.0

        Row.fromSeq(row.toSeq ++ Seq(pricePerTime, pricePerDistance))
      }

      headers = headers ++ Seq(colPricePerTime, colPricePerDistance)

      val rddDurationBin = binColByStepValue(rddWithPriceDistanceAndTime, headers.indexOf("trip_distance"), 5)

      headers = headers :+ colDistanceBin

      val rddTZDurationPcgLabel = binColByStepValue(rddDurationBin, headers.indexOf(colDurationOvernightPcg), 5)

      headers = headers :+ (colDurationOvernightPcg + "_label")

      val actualHeader = headers

      val rddWithKey = rddTZDurationPcgLabel.map { row =>
        val key = colsForClassification.filter(col => actualHeader.contains(col.toLowerCase)).map(col => row.get(headers.indexOf(col.toLowerCase))).mkString("_")
        (key, row)
      }

      headers = Seq("key") ++ headers

      val rddAvgPrices = rddWithKey
        .map { case (key, originalRow) =>
          val row = Row.fromSeq(originalRow.toSeq)
          val costPerDistance = row.getAs[Double](headers.indexOf(colPricePerDistance))
          val costPerTime = row.getAs[Double](headers.indexOf(colPricePerTime))

          (key, (costPerDistance, costPerTime, 1)) // attach counts for averaging
        }
        .reduceByKey { (a, b) =>
          // Sum cost_per_distance, cost_per_time, and counts
          (a._1 + b._1, a._2 + b._2, a._3 + b._3)
        }
        .mapValues { case (totalCostPerDistance, totalCostPerTime, count) =>
          val avgCostPerDistance = BigDecimal(totalCostPerDistance / count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          val avgCostPerTime = BigDecimal(totalCostPerTime / count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          (avgCostPerDistance, avgCostPerTime)
        }
        .filter { case (_, (avgDistance, avgTime)) =>
          avgDistance > 0 && avgTime > 0
        }

      val rddJoined = rddWithKey.join(rddAvgPrices).map { case (_, (originalRow, (avgCostPerDistance, avgCostPerTime))) =>
        Row.fromSeq(originalRow.toSeq ++ Seq(avgCostPerDistance, avgCostPerTime))
      }

      headers = headers.drop(1) ++ Seq(colAvgPricePerDistance, colAvgPricePerTime)

      val rddPriceComparisons = rddJoined.map { row =>
        val priceColsToAdd: Seq[Double] =
          Seq((colPricePerDistance, colAvgPricePerDistance), (colPricePerTime, colAvgPricePerTime))
            .flatMap { case (colPrice, colAvgPrice) =>
              val price = row.getAs[Double](headers.indexOf(colPrice))
              val priceAvg = row.getAs[Double](headers.indexOf(colAvgPrice))
              val priceDiff = BigDecimal(price - priceAvg).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              val priceDiffPcg = BigDecimal(priceDiff / priceAvg * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

              Seq(priceDiff, priceDiffPcg)
            }

        Row.fromSeq(row.toSeq ++ priceColsToAdd)
      }

      headers = headers ++ Seq(colPricePerDistanceDiff, colPricePerDistanceDiffPcg, colPricePerTimeDiff, colPricePerTimeDiffPcg)

      val rddPriceDiffPcgBin = binColByStepValue(
        binColByStepValue(
          rddPriceComparisons,
          headers.indexOf(colPricePerDistanceDiffPcg), 5),
        headers.indexOf(colPricePerTimeDiffPcg), 5
      )

      headers = headers ++ Seq(colPricePerDistanceDiffPcgLabel, colPricePerTimeDiffPcgLabel)

      val underLimit: Double = -80
      val features: Seq[String] = Seq("trip_distance", colDurationMinutes, "fare_amount", "total_amount", colPricePerTime, colAvgPricePerTime, colPricePerTimeDiffPcgLabel, colPricePerDistance, colAvgPricePerDistance, colPricePerDistanceDiffPcgLabel)

      // trip_distance, duration_minutes, fare_amount, total_amount, cost_per_time, avg_cost_per_time, cost_per_time_diff_pcg_label, cost_per_distance, avg_cost_per_distance, cost_per_distance_diff_pcg_label
//      debugLim(rddPriceDiffPcgBin,
//        headersSeq =>
//          row => row.getAs[Double](headersSeq.indexOf(colPricePerDistanceDiffPcg)) < underLimit ||
//            row.getAs[Double](headersSeq.indexOf(colPricePerTimeDiffPcg)) < underLimit,
//        features,
//        headers
//      )

      val headersForAnalysis = headers.filter(head => colsForClassification.contains(head.toLowerCase))

      val rddForAnalysis = rddPriceDiffPcgBin.map { row =>
        Row.fromSeq(headersForAnalysis.map(head => row.get(headers.indexOf(head))))
      }

      val totalCount = rddForAnalysis.count()

      val rddFeatures = colsForValuesAnalysis.map { colName =>
        val groupCols = Seq(colPricePerDistanceDiffPcgLabel, colPricePerTimeDiffPcgLabel) :+ colName

        val grouped = rddForAnalysis
          .map { row =>
            val key = groupCols.map(col => {row.get(headersForAnalysis.indexOf(col.toLowerCase))})
            (key, 1)
          }
          .reduceByKey(_ + _)
          .map { case (keySeq, count) =>
            val value = keySeq.last.toString
            val costDistLabel = keySeq(0).toString
            val costTimeLabel = keySeq(1).toString
            val pcg = BigDecimal(count.toDouble / totalCount * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            Row.fromSeq(Seq(colName, value, count, pcg, costDistLabel, costTimeLabel))
          }
        grouped
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
