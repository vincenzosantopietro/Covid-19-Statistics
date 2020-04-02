package com.vinx.covid.statistics.data

import java.io.FileNotFoundException

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class ItalianCNMDataParser(inputPath : String) extends DataParser {

  private val spark = SparkSession.builder().appName("covid_statistics").master("local[*]").getOrCreate()

  private def parseData : Some[DataFrame] = {
    import spark.implicits._

    val resource_path = "src/main/resources/" + inputPath
    //TODO: translate cols to english
    val data = spark.read.
      option("header","true").
      csv(resource_path).
      withColumnRenamed("data","date").
      withColumn("date", to_timestamp(col("date"),"yyyy-MM-dd\'T\'HH:mm:ss")).
      withColumnRenamed("ricoverati_con_sintomi","hospital_symptoms").
      withColumn("hospital_symptoms", $"hospital_symptoms".cast(sql.types.IntegerType)).
      withColumnRenamed("terapia_intensiva","intensive_care").
      withColumn("intensive_care", $"intensive_care".cast(sql.types.IntegerType)).
      withColumnRenamed("totale_ospedalizzati","total_through_hospital").
      withColumn("total_through_hospital", $"total_through_hospital".cast(sql.types.IntegerType)).
      withColumnRenamed("isolamento_domiciliare","home_isolation").
      withColumn("home_isolation", $"home_isolation".cast(sql.types.IntegerType)).
      withColumnRenamed("totale_positivi","total_infected").
      withColumn("total_infected", $"total_infected".cast(sql.types.IntegerType)).
      withColumnRenamed("variazione_totale_positivi","infected_variation").
      withColumn("infected_variation", $"infected_variation".cast(sql.types.IntegerType)).
      withColumnRenamed("dimessi_guariti","recovered").
      withColumn("recovered", $"recovered".cast(sql.types.IntegerType)).
      withColumnRenamed("deceduti","dead").
      withColumn("dead", $"dead".cast(sql.types.IntegerType)).
      withColumnRenamed("totale_casi","total_cases").
      withColumn("total_cases", $"total_cases".cast(sql.types.IntegerType)).
      withColumnRenamed("tamponi","tampons").
      withColumn("tampons", $"tampons".cast(sql.types.IntegerType)).
      drop("nuovi_positivi").
      drop("note_it").
      drop("note_en").
      drop("stato")

    Some(data)
  }

  override def getData : DataFrame = {
    val someDataframe = parseData

    if (someDataframe.isEmpty)
      throw new RuntimeException("Data_not_found")

    someDataframe.x
  }
}