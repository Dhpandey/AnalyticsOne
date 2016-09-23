package utility

/**
  * Created by dpanw on 8/17/16.
  */
class EmptyValueHandler {
  def emptyStringHandler(x: String): String = {
    if (x.trim.nonEmpty) {
      x
    }
    else
      "null"
  }

  def emptyNumberHandler(x: String): Double = {
    if (x.trim.nonEmpty) {
      x.toDouble
    }
    else
      0
  }
}
