/**
  * Created by dpanw on 8/18/16.
  */
object Test {

  class MemTrips(val memberDeviceID: String, val tripID: String, val timestamp: String, val timestampEpoch: String, val accelerometerX: Double, val
  accelerometerY: Double, val accelerometerZ: Double, val magnetometerX: Double, val magnetometerY: Double, val
                 magnetometerZ: Double, val deviceOrientation: String, val gyroRotationX: Double, val gyroRotationY: Double, val
                 gyroRotationZ: Double, val pitch: Double, val roll: Double, val yaw: Double, val rotationRateX: Double, val
                 rotationRateY: Double, val rotationRateZ: Double, val activity: String, val confidence: Double, val
                 relativeAltitude: String) extends Product with Serializable {

    def canEqual(that: Any) = that.isInstanceOf[MemTrips]

    def productArity = 23 // number of columns

    def productElement(idx: Int) = idx match {
      case 0 => memberDeviceID
      case 1 => tripID
      case 2 => timestamp
      case 3 => timestampEpoch
      case 4 => accelerometerX
      case 5 => accelerometerY
      case 6 => accelerometerZ
      case 7 => magnetometerX
      case 8 => magnetometerY
      case 9 => magnetometerZ
      case 10 => deviceOrientation
      case 11 => gyroRotationX
      case 12 => gyroRotationY
      case 13 => gyroRotationZ
      case 14 => pitch
      case 15 => roll
      case 16 => yaw
      case 17 => rotationRateX
      case 18 => rotationRateY
      case 19 => rotationRateZ
      case 20 => activity
      case 21 => confidence
      case 22 => relativeAltitude
    }
  }

  def main(args: Array[String]) {

  }
}
