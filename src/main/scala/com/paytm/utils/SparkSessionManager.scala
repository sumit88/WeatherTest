package com.paytm.utils

import com.paytm.exceptions.SparkSessionInitError
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

trait SparkSessionManager {

  val sparkConf: SparkConf = getSparkConf
  implicit val spark: SparkSession = getSparkSession

  import spark.implicits._


  def getSparkConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")
  }

  def getSparkSession: SparkSession = {

    Try {

      SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()


    } match {
      case Success(s) => s
      case Failure(_) => throw SparkSessionInitError(sparkConf)
    }


  }


}