package utils
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time._
import java.time.temporal.ChronoUnit

object Preprocess {
  
  private val pickUpCol: String = "pickup_ts"
  private val dropOffCol: String = "dropoff_ts"
  private val surchargeWeekdays: String = "weekday_surcharge"
  private val durationCol: String = "duration_minutes"
  private val amountMinusTipsCol: String = "total_amount_no_tips"
  private val aggregateFeeCol: String = "aggregate_fee"
  private val weekDaySurcharge: Double = 2.5

  def dropNullValues(df: DataFrame): DataFrame = {
    df.na.drop()
  }
  
  private def addWeekdaysSurchargeCol(df: DataFrame): DataFrame = {
    df
      .withColumn(surchargeWeekdays, lit(addWeekdaySurcharge(weekDaySurcharge)(col(pickUpCol))))
  }

  def addDurationRemovingNegatives(df: DataFrame, dropoff_col: String, pickup_col: String): DataFrame = {
    df
      .withColumn(pickUpCol, to_timestamp(col(pickup_col), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn(dropOffCol, to_timestamp(col(dropoff_col), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn(durationCol, round((unix_timestamp(col(dropOffCol)) - unix_timestamp(col(pickUpCol))) / 60, 2))
      .filter(col(durationCol) > 0)
  }

  private def addTimeZones(df: DataFrame, timeZones: Map[String, (Int, Int)]): DataFrame = {

    val bucket_col = "bucket_times"
    var dfWithBucketTimes = df
      .withColumn(bucket_col, lit(preciseBucketUDF(timeZones)(col(pickUpCol), col(dropOffCol))))
    
    timeZones.keys.zipWithIndex.foreach { case (label, idx) =>
      dfWithBucketTimes = dfWithBucketTimes
        .withColumn(s"${durationCol}_$label", col(bucket_col).getItem(idx))
    }
    
    dfWithBucketTimes.drop(bucket_col)
  }

  private def addYear(df: DataFrame, pickup_col: String): DataFrame = {
    df
      .withColumn("year", year(to_timestamp(col(pickup_col))))
  }

  def binColByStepValue(df: DataFrame, colToDiscrete: String, stepValue: Int = 5): DataFrame = {
    val dfBin = df.withColumn(s"${colToDiscrete}_bin", (col(colToDiscrete) / stepValue).cast("int") * stepValue)

    val rawBin = (col(colToDiscrete) / stepValue).cast("int") * stepValue

    // Shift binBase by +stepValue when it's a negative exact multiple of step
    val binBase = when(
      col(colToDiscrete) < 0 && (col(colToDiscrete) % stepValue === lit(0)),
      rawBin + stepValue
    ).otherwise(rawBin)

    val isNegative = col(colToDiscrete) < 0

    val binLabel = when(isNegative,
      concat(
        lit("["),
        (binBase - stepValue).cast("int"),
        lit("|"),
        binBase.cast("int"),
        lit(")")
      )
    ).otherwise(
      concat(
        lit("["),
        binBase.cast("int"),
        lit("|"),
        (binBase + stepValue).cast("int"),
        lit(")")
      )
    )

    dfBin.withColumn(s"${colToDiscrete}_bin_label", binLabel)
  }

  def binColByEqualQuantiles(df: DataFrame, colToDiscrete: String, scale: Int = 5): DataFrame = {
    val step = 1.0 / scale
    val steps = (0 to scale).map(i => i * step).toArray

    val quantiles = df.stat.approxQuantile(colToDiscrete, steps, 0.01)
    val splits = quantiles.distinct.sorted

    val binLabels = splits.sliding(2).map {
      case Array(start, end) => s"[$start|$end)"
    }.toArray

    val binLabelUDF = udf((index: Double) =>
      if (index >= 0 && index < binLabels.length) binLabels(index.toInt) else "Unknown"
    )

    val bucketizer = new Bucketizer()
      .setInputCol(colToDiscrete)
      .setOutputCol(s"${colToDiscrete}_bin")
      .setSplits(splits)

    val transformedDf = bucketizer.transform(df)

    transformedDf.withColumn(
      s"${colToDiscrete}_bin_label",
      binLabelUDF(col(s"${colToDiscrete}_bin"))
    )
  }

  private def preciseBucketUDF(timeZones: Map[String, (Int, Int)]) = udf((pickup: Timestamp, dropoff: Timestamp) => {
    var result = timeZones.keys.map(_ -> 0L).toMap
    
    if (!(pickup == null || dropoff == null)) {
      
      val start = pickup.toLocalDateTime
      val end = dropoff.toLocalDateTime
      if (!end.isBefore(start)) {

        var current = start.toLocalDate.atStartOfDay

        def overlap(start1: LocalDateTime, end1: LocalDateTime,
                    start2: LocalDateTime, end2: LocalDateTime): Long = {
          val overlapStart = if (start1.isAfter(start2)) start1 else start2
          val overlapEnd = if (end1.isBefore(end2)) end1 else end2
          if (overlapEnd.isAfter(overlapStart)) ChronoUnit.MINUTES.between(overlapStart, overlapEnd) else 0L
        }

        while (!current.isAfter(end)) {
          val nextDay = current.plusDays(1)

          timeZones.foreach {
            case (label, (startHour, endHour)) if startHour > endHour =>
              val bucketStartBeforeMidnight = current.withHour(startHour).withMinute(0)
              val bucketEndBeforeMidnight = current.plusDays(1).withHour(0).withMinute(0)
              val bucketStartAfterMidnight = bucketEndBeforeMidnight
              val bucketEndAfterMidnight = current.plusDays(1).withHour(endHour).withMinute(0)
              
              val minutesBeforeMidnight = overlap(start, end, bucketStartBeforeMidnight, bucketEndBeforeMidnight)
              val minutesAfterMidnight = overlap(start, end, bucketStartAfterMidnight, bucketEndAfterMidnight)
              
              result = result.updated(label, result(label) + minutesBeforeMidnight + minutesAfterMidnight)
              
            case (label, (startHour, endHour)) =>
              val bucketStart = current.withHour(startHour).withMinute(0)
              val bucketEnd = if (endHour == 24) current.plusDays(1).withHour(0).withMinute(0) else current.withHour(endHour).withMinute(0)
  
              val minutes = overlap(start, end, bucketStart, bucketEnd)
              result = result.updated(label, result(label) + minutes)
          }

          current = nextDay
        }
      }
    }
    result.values.toSeq
  })

  private def addWeekdaySurcharge(surcharge: Double) = udf((pickup: Timestamp) => {
    if (isUSHolidayOrWeekend(pickup.toLocalDateTime.toLocalDate)) 0 else surcharge
  })

  private def isUSHolidayOrWeekend(date: LocalDate): Boolean = {
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

  private def aggregateExtrasAndFees(df: DataFrame, filterFunc: Column => Column): DataFrame = {
    var dfFiltered = df

    val taxes = List(col("extra"), col("mta_tax"), col("tolls_amount"), col("improvement_surcharge"), col("congestion_surcharge"), col("Airport_fee"), col("weekday_surcharge"))

    taxes.foreach(col => dfFiltered = dfFiltered.filter(filterFunc(col)))

    binColByEqualQuantiles(
      dfFiltered.withColumn(aggregateFeeCol, round(col("extra") + col("mta_tax") + col("tolls_amount") + col("improvement_surcharge") + col("congestion_surcharge") + col("Airport_fee") + col("weekday_surcharge"), 2)),
      aggregateFeeCol,
      10
    )
  }
  
  private def addPricePerMile(df: DataFrame): DataFrame = {
    val dfWithNoTips = df.withColumn(amountMinusTipsCol, round(col("total_amount") - col("tip_amount"), 2))
    dfWithNoTips.withColumn("cost_per_distance", round(col(amountMinusTipsCol) / col("trip_distance"), 2))
  }
  
  def getPercentage(df: DataFrame, colNameForPercentage: String, colNameToDivide: String): DataFrame = {
    df.withColumn(f"${colNameForPercentage}_pcg", round(when(col(colNameToDivide) =!= 0 && col(colNameToDivide).isNotNull,
      lit(100) * col(colNameForPercentage) / col(colNameToDivide)
    ), 2))
  }

  def addCostPerDistanceComparison(df: DataFrame): DataFrame = {
    df.withColumn("cost_per_distance_diff_pcg", round(lit(100) * (col("cost_per_distance") - col("avg_cost_per_distance")) / col("avg_cost_per_distance"), 2))
  }
  
  def applyAllPreprocess(df: DataFrame, filters: Map[String, Column], taxFilter: Column => Column, timeZones: Map[String, (Int, Int)], dropoff_col: String, pickup_col: String): DataFrame = {
    val existingFilters = filters.filter { case (colName, _) => df.columns.contains(colName) }

    val filteredDF = existingFilters.values.foldLeft(df) { (accDF, filterExpr) =>
      accDF.filter(filterExpr)
    }

    addPricePerMile(
      aggregateExtrasAndFees(
        addWeekdaysSurchargeCol(
          addTimeZones(
            addYear(
              binColByEqualQuantiles(
                addDurationRemovingNegatives(filteredDF, dropoff_col, pickup_col), "duration_minutes", 25),
              pickup_col
            ),
            timeZones
          )
        ),
        taxFilter
      )
    )
  }
}
