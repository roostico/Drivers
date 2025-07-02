package jobs.second.rdd

object UtilFunctions {
  def getQuantile(sortedRDD: org.apache.spark.rdd.RDD[(Long, Double)], quantile: Double, count: Long): Double = {
    val idx = (quantile * count).toLong
    sortedRDD.lookup(idx).headOption.getOrElse(sortedRDD.map(_._2).takeOrdered(1).head)
  }
}
