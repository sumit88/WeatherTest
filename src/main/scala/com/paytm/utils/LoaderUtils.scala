package com.paytm.utils

import com.paytm.exceptions.SourceException
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object LoaderUtils {

  def readCsv(path: String,
              properties: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    Try {
      spark.read.options(properties).csv(path)
    } match {
      case Success(df) => df
      case Failure(_) => throw SourceException(path)
    }
  }
}
