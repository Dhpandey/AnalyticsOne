package allstate.lionbridge.parser

/**
  * Created by dpanw on 8/29/16.
  */
object TripSummaryParser extends java.io.Serializable {

  case class TripSummaryContainer(tripSummaryUpload: TripSummaryUpload) extends java.io.Serializable

  class TripSummaryUpload(totalTripMiles: Double, tripAccelScore: Double, eventDetails: Array[EventDetails1],
                          networkTime: String, tripTODScore: Double, averageSpeed: Double, lastSuccessDateTime: String,
                          tripStart_TS: String, memberDeviceId: String, tripRejectReason: String, mobileAppVersion: String,
                          BrakeInterval_1: Double, BrakeInterval_2: Double, maxSpeed: Double, tripMilesTime_1: Double,
                          BrakeInterval_3: Double, tripMilesTime_2: Double, BrakeInterval_4: Int, tripBrakeScore: Double,
                          tripMilesTime_3: Double, BrakeInterval_5: Double, tripMilesTime_4: Double, tripMilesTime_5: Double,
                          GeoPoint: Array[GeoPoint], tripStartLocation: String, speedGrade: String, tripAccelGrade: String,
                          tripTotalScore: Double, speedScore: Double, tripRemove_TS: String, tripMilesScore: Double,
                          distance: Double, tripTerminateId: Double, AccelInterval_1: Double, tripId: String,
                          deviceProgram: Array[DeviceProgram], AccelInterval_2: Double, AccelInterval_3: Double,
                          OverrideType: String, AccelInterval_4: Double, AccelInterval_5: Double, duration: Double,
                          tripBrakeGrade: String, tripEndLocation: String, idleTime: Double, tripTerminateReason: Double,
                          tripTotalGrade: String, mobileAppDevice: String, eventCount: Double, mobileOsVersion: String,
                          tripMilesGrade: String, tripUpload_TS: String, MilesAtorOverMaxSpeed: Double, tripTODGrade: String,
                          tripEnd_TS: String) extends Product with Serializable {

    def canEqual(that: Any) = that.isInstanceOf[TripSummaryUpload]

    def productArity = 55 // number of columns

    def productElement(idx: Int) = idx match {
      case 0 => totalTripMiles
      case 1 => tripAccelScore
      case 2 => eventDetails
      case 3 => networkTime
      case 4 => tripTODScore
      case 5 => averageSpeed
      case 6 => lastSuccessDateTime
      case 7 => tripStart_TS
      case 8 => memberDeviceId
      case 9 => tripRejectReason
      case 10 => mobileAppVersion
      case 11 => BrakeInterval_1
      case 12 => BrakeInterval_2
      case 13 => maxSpeed
      case 14 => tripMilesTime_1
      case 15 => BrakeInterval_3
      case 16 => tripMilesTime_2
      case 17 => BrakeInterval_4
      case 18 => tripBrakeScore
      case 19 => tripMilesTime_3
      case 20 => BrakeInterval_5
      case 21 => tripMilesTime_4
      case 22 => tripMilesTime_5
      case 23 => GeoPoint
      case 24 => tripStartLocation
      case 25 => speedGrade
      case 26 => tripAccelGrade
      case 27 => tripTotalScore
      case 28 => speedScore
      case 29 => tripRemove_TS
      case 30 => tripMilesScore
      case 31 => distance
      case 32 => tripTerminateId
      case 33 => AccelInterval_1
      case 34 => tripId
      case 35 => deviceProgram
      case 36 => AccelInterval_2
      case 37 => AccelInterval_3
      case 38 => OverrideType
      case 39 => AccelInterval_4
      case 40 => AccelInterval_5
      case 41 => duration
      case 42 => tripBrakeGrade
      case 43 => tripEndLocation
      case 44 => idleTime
      case 45 => tripTerminateReason
      case 46 => tripTotalGrade
      case 47 => mobileAppDevice
      case 48 => eventCount
      case 49 => mobileOsVersion
      case 50 => tripMilesGrade
      case 51 => tripUpload_TS
      case 52 => MilesAtorOverMaxSpeed
      case 53 => tripTODGrade
      case 54 => tripEnd_TS
    }
  }

  case class EventDetails1(eventGPSSignalStrength: Double, eventSampleSpeed: Double, eventSensorDetectionMthd: Double,
                           eventStartlocation: String, eventType: Double, eventMilesDriven: Double, eventDuration: Double,
                           eventEndlocation: String, eventStart_TS: String, eventEnd_TS: String,
                           eventSpeedChange: Double)
    extends java.io.Serializable


  case class DeviceProgram(programId: Int) extends java.io.Serializable

  case class GeoPoint(GPSAccuracy: Double, GPSPosition: String, GPSSpeed: Double, GPSTime: String) extends java.io.Serializable

}
