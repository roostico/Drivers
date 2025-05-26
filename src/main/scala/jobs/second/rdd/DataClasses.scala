package jobs.second.rdd

import java.sql.Timestamp

object DataClasses {

  case class WeatherInfo(
     wmoCode: Int,
     dateOfRelevation: Timestamp,
     description: String
  )


  case class Ride(
     vendorId: Int,
     pickupDatetime: Timestamp,
     dropoffDatetime: Timestamp,
     fareAmount: Double,
     tipAmount: Double,
     paymentType: Int,
     tripDistance: Double,
     totalAmount: Double,
     passengerCount: Int,
     serviceType: String
   )

  case class RideWithDurationMinutes(info: Ride, durationMinutes: Double)

  case class RideWithEnrichedInformation(
       rideWithMinutes: RideWithDurationMinutes,
       hourOfDay: Int,
       dayOfWeek: Int,
       monthOfYear: Int,
       year: Int,
       isWeekend: Int,
       tripHourBucket: String,
       tipPercentage: Double,
       speedMph: Double,
       isRushHour: Boolean,
       isLongTrip: Boolean
   )

  case class RideWithBins(
     enrichedInfo: RideWithEnrichedInformation,
     tripDistanceBin: String,
     tripDurationBin: String,
     fareAmountBin: String,
     tipPercentageBin: String,
     speedBin: String
  )

  case class RideWithWeather(
    ride: DataClasses.RideWithBins,
    weatherInfo: DataClasses.WeatherInfo
  )

  case class RideFinalOutput(
    ride: DataClasses.RideWithBins,
    weather: DataClasses.WeatherInfo,
    generalWeather: String
  )



}
