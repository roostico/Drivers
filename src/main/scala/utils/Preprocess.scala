package utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Bucketizer

object Preprocess {

  def dropNullValues(df: DataFrame): DataFrame = {
    df.na.drop()
  }

  def addDurationRemovingNegatives(df: DataFrame, 
                  dropoff_col: String = "tpep_dropoff_datetime",
                  pickup_col: String = "tpep_pickup_datetime"): DataFrame = {
    df
      .withColumn("pickup_ts", to_timestamp(col(pickup_col), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("dropoff_ts", to_timestamp(col(dropoff_col), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("duration_minutes", round((unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60, 2))
      .filter(col("duration_minutes") > 0)
  }
  
  def binColByStepValue(df: DataFrame, colToDiscrete: String, stepValue: Int = 5): DataFrame = {
    df
      .withColumn(s"${colToDiscrete}_bin", (col(colToDiscrete) / stepValue).cast("int") * stepValue)
  }

  def binColByEqualQuantiles(df: DataFrame, colToDiscrete: String, scale: Int = 5): DataFrame = {
    val step = 1.0 / scale
    val steps = (0 to scale).map(i => i * step).toArray

    val quantiles = df.stat.approxQuantile(colToDiscrete, steps, 0.01)
    val splits = quantiles.distinct.sorted

    val binLabels = splits.sliding(2).map {
      case Array(start, end) => s"[${start}–${end})"
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
}


//VendorID
//tpep_pickup_datetime
//tpep_dropoff_datetime
//passenger_count
//trip_distance
//RatecodeID
//store_and_fwd_flag
//PULocationID
//DOLocationID
//payment_type
//fare_amount
//extra
//mta_tax
//tip_amount
//tolls_amount
//improvement_surcharge
//total_amount
//congestion_surcharge
//airport_fee
//cbd_congestion_fee 