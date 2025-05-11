package jobs.second.rdd

import java.sql.Timestamp

object DataClasses {

  case class Ride(
     vendorId: Int,
     pickupDatetime: Timestamp,
     dropoffDatetime: Timestamp,
     fareAmount: Double,
     tipAmount: Double,
     paymentType: Int,
     tripDistance: Double,
     totalAmount: Double,
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



}
