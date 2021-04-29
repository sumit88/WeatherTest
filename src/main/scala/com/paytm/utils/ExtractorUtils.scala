package com.paytm.utils

import org.apache.spark.sql.DataFrame

object ExtractorUtils {


  def extractCountryNames(dataFrame :DataFrame) : Array[String] = {
    dataFrame.collect()
      .map(_.getAs[String]("COUNTRY_FULL"))
  }
}
