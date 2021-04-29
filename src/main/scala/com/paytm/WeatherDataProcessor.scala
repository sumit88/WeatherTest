package com.paytm

import com.paytm.utils.{LoaderUtils, SparkSessionManager}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.DataTypes
import scala.util.{Failure, Success, Try}

object WeatherDataProcessor extends SparkSessionManager with App {

  val logger = LoggerFactory.getLogger(getClass)

  Try {

    import spark.implicits._

    val csvProperties = Map("header" -> "true", "inferSchema" -> "true")

    val stations = LoaderUtils.readCsv(
      this.getClass.getClassLoader.getResource("stationlist.csv").getPath, csvProperties)
      .repartition($"COUNTRY_ABBR")

    val countries = LoaderUtils.readCsv(
      this.getClass.getClassLoader.getResource("countrylist.csv").getPath, csvProperties)
      .repartition($"COUNTRY_ABBR")

    val rawWeatherData = LoaderUtils.readCsv(
      this.getClass.getClassLoader.getResource("data/2019").getPath, csvProperties)
      .repartition($"STN---")


    val stationWithCountries = stations
      .join(countries, "COUNTRY_ABBR")
      .repartition($"STN_NO")


    val countryWeatherData = rawWeatherData
      .join(stationWithCountries, rawWeatherData("STN---") === stationWithCountries("STN_NO"))
      .withColumn("DATE", to_timestamp($"YEARMODA".cast(DataTypes.StringType), "yyyyMMdd"))
      .select("TEMP", "WDSP", "DATE", "COUNTRY_FULL", "STN_NO", "FRSHTT")
      .repartition($"COUNTRY_FULL", $"STN_NO")
      .cache()
    val dataProcessor = DataProcessor()

    val countriesWithHighestAverageMeanTemp = dataProcessor.getCountryWithHottestAverageMeanTemp(countryWeatherData)
    logger.info(s"Country[s] with highest average mean temp : ${countriesWithHighestAverageMeanTemp.mkString(", ")}")

    val secondHighestAverageMeanWindCountry = dataProcessor.getCountrySecondHighestAverageMeanWind(countryWeatherData)
    logger.info(s"Country[s] with second highest average mean winde speed : ${secondHighestAverageMeanWindCountry.mkString(", ")}")

    val countriesWithMostHurricaneOrCloud = dataProcessor.getCountyWithMaxConsecutivelyTornadoOrCloud(countryWeatherData)
    logger.info(s"Country[s] with most hurricane or funnel clouds: ${countriesWithMostHurricaneOrCloud.mkString(", ")}")

  }

  match {
    case Success(_) => logger.info(s"Process Completed")
    case Failure(f) => logger.error(
      s"""Fatal Exception
         |${
        f.toString
      }
         |""".stripMargin)
      throw f

  }

}
