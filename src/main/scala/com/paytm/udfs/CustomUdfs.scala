package com.paytm.udfs

import com.paytm.models.ConsecutiveTracker
import org.apache.spark.sql.functions.udf

object CustomUdfs {


  val isTornadoOrCloud = udf((x: Int) => {
    x % 10 match {
      case 1 => 1
      case _ => 0
    }
  })


  /*
  it find the max consecutive days based on a date which is converted to a long (time in milli seconds)
   */
  val maxConsecutiveDays = udf((x: Seq[Long]) => {

    x.tail.foldLeft(ConsecutiveTracker(x.headOption.getOrElse(0), 0, 0))((a, b) => {

      if ((b - a.dateInLong) < ((1 * 60 * 60 * 24) + 1)) { // one second more than a day
        val _temp = a.currentMax + 1
        ConsecutiveTracker(b, _temp, if (_temp > a.overAllMax) _temp else a.overAllMax)
      } else {
        ConsecutiveTracker(b, 0, a.overAllMax)
      }
    }).overAllMax
  })


}
