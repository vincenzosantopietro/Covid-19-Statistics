package com.vinx.covid.statistics.data

import java.io.FileNotFoundException

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class ItalianCNMDataParser(inputPath : String) extends DataParser {

  private val spark = SparkSession.builder().appName("covd_statistics").master("local[*]").getOrCreate()

  private def parseData : Some[DataFrame] = {
    import spark.implicits._

    val resource_path = "src/main/resources/" + inputPath
    //TODO: translate cols to english
    val data = spark.read.
      option("header","true").
      csv(resource_path).
      //withColumn("sno", $"sno".cast(sql.types.IntegerType)).
      withColumn("data", to_timestamp(col("data"),"yyyy-MM-dd HH:mm:ss")).
      //withColumn("Date", $"Date".cast(sql.types.StringType)).
      withColumn("deceduti", $"deceduti".cast(sql.types.IntegerType)).
      withColumn("totale_attualmente_positivi", $"totale_attualmente_positivi".cast(sql.types.IntegerType)).
      withColumn("totale_ospedalizzati", $"totale_ospedalizzati".cast(sql.types.IntegerType)).
      withColumn("terapia_intensiva", $"nuovi_attualmente_positivi".cast(sql.types.IntegerType)).
      withColumn("dimessi_guariti", $"dimessi_guariti".cast(sql.types.IntegerType)).
      withColumn("tamponi", $"tamponi".cast(sql.types.IntegerType)).
      withColumn("totale_casi", $"totale_casi".cast(sql.types.IntegerType)).
      withColumn("stato", $"stato".cast(sql.types.StringType)).
      withColumn("isolamento_domiciliare", $"isolamento_domiciliare".cast(sql.types.IntegerType)).
      withColumn("ricoverati_con_sintomi", $"ricoverati_con_sintomi".cast(sql.types.IntegerType))

    Some(data)
  }

  override def getData : DataFrame = {
    val someDataframe = parseData

    if (someDataframe.isEmpty)
      throw new RuntimeException("Data_not_found")

    someDataframe.x
  }
}