package com.paytm

import com.paytm.udfs.CustomUdfs.{isTornadoOrCloud, maxConsecutiveDays}
import com.paytm.utils.ExtractorUtils
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, collect_set, dense_rank, lit, size}
import org.apache.spark.sql.types.DataTypes

case class DataProcessor()(implicit val spark: SparkSession) {

  import spark.implicits._

  val tempSpecs: WindowSpec = Window.orderBy($"AvgTemp".desc)
  val windSpecs: WindowSpec = Window.orderBy($"AvgWindSpeed".desc)
  val tornadoOrCloudSpecs: WindowSpec = Window.partitionBy("COUNTRY_FULL").orderBy($"DATE".asc)
  val maxConsecutiveDaysSpec: WindowSpec = Window.orderBy($"MAX_CONSECTIVE_DAY".desc)


  /*
  Finds the list of countries with max average mean temp
  Using collect instead of head or take 1 as there is rare chance that there could be multiple countries with rank 1
   */
  def getCountryWithHottestAverageMeanTemp(countryWeatherData: DataFrame): Array[String] = {
    val _df = countryWeatherData
      .filter($"TEMP" =!= lit(9999.9D))
      .groupBy("COUNTRY_FULL", "STN_NO")
      .agg(avg($"TEMP").as("AvgTemp"))
      .withColumn("TempRank", dense_rank().over(tempSpecs))
      .filter($"TempRank" === lit(1))
    ExtractorUtils.extractCountryNames(_df)

  }

  /*
  Finds the list of countries with second average mean wind speed
  Using collect instead of head or take 1 as there is rare chance that there could be multiple countries with rank 1
   */
  def getCountrySecondHighestAverageMeanWind(countryWeatherData: DataFrame): Array[String] = {
    val _df = countryWeatherData
      .filter($"WDSP" =!= lit(999.9D))
      .groupBy("COUNTRY_FULL", "STN_NO")
      .agg(avg($"WDSP").as("AvgWindSpeed"))
      .withColumn("TempRank", dense_rank().over(windSpecs))
      .filter($"TempRank" === lit(2))
    ExtractorUtils.extractCountryNames(_df)

  }

  /*
  Finds the number of country with maximum consecutive days of cloud funnel or tornado
   */
  def getCountyWithMaxConsecutivelyTornadoOrCloud(countryWeatherData: DataFrame): Array[String] = {
    val _df = countryWeatherData.select(
      $"COUNTRY_FULL",
      $"DATE".cast(DataTypes.LongType),
      isTornadoOrCloud($"FRSHTT").as("TORNADO_OR_CLOUD")
    ).filter($"TORNADO_OR_CLOUD" === lit(1))
      .groupBy($"COUNTRY_FULL")
      .agg(collect_set($"DATE").as("TORNADO_OR_CLOUD_DAYS"))
      .filter(size($"TORNADO_OR_CLOUD_DAYS") =!= lit(0))
      .orderBy($"TORNADO_OR_CLOUD_DAYS".asc)
      .withColumn("MAX_CONSECTIVE_DAY", maxConsecutiveDays($"TORNADO_OR_CLOUD_DAYS"))
      .select($"COUNTRY_FULL", (dense_rank() over maxConsecutiveDaysSpec).as("COUNTRY_RANK"))
      .filter($"COUNTRY_RANK" === lit(1))

    ExtractorUtils.extractCountryNames(_df)

  }
}
