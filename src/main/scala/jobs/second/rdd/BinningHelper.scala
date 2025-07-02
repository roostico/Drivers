package jobs.second.rdd

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
    case c if Seq(0, 1).contains(c)              => "clear"
    case c if Seq(2, 3, 4).contains(c)           => "cloudy"
    case c if Seq(45, 48).contains(c)            => "foggy"
    case c if (50 to 67).contains(c)       => "rainy"
    case c if (70 to 77).contains(c)       => "snowy"
    case c if (80 to 99).contains(c)       => "stormy"
    case _                                       => "unknown"
  }
}