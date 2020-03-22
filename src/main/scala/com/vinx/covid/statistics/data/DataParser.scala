package com.vinx.covid.statistics.data

import java.io.FileNotFoundException

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/*case class NcovDataRow(
                        sno: String, date: String, provinceCountry: String, country: String,
                        lastUpdate: String, confirmed: Float, deaths: Float, recovered: Float )
*/

class DataParser (input_path : String){

  private val spark = SparkSession.builder().appName("covd_statistics").master("local[*]").getOrCreate()

  private def parseData : Some[DataFrame] = {
    import spark.implicits._

    val resource_path = "src/main/resources/" + input_path
    val data = spark.read.
      option("header","true").
      csv(resource_path).
      withColumn("sno", $"sno".cast(sql.types.IntegerType)).
      withColumn("Date", to_timestamp(col("Date"),"MM/dd/yyyy HH:mm:ss")).
      //withColumn("Date", $"Date".cast(sql.types.StringType)).
      withColumn("Confirmed", $"Confirmed".cast(sql.types.FloatType)).
      withColumn("Deaths", $"Deaths".cast(sql.types.FloatType)).
      withColumn("Recovered", $"Recovered".cast(sql.types.FloatType))

    Some(data)
  }

  def getData : DataFrame = {
    val someDataframe = parseData

    if (someDataframe.isEmpty)
      throw new RuntimeException("Data_not_found")

    someDataframe.x
  }
}