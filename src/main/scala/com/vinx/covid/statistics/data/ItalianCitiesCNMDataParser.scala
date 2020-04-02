package com.vinx.covid.statistics.data

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ItalianCitiesCNMDataParser(inputPath : String) extends DataParser {

  private val spark = SparkSession.builder().appName("covid_statistics").master("local[*]").getOrCreate()

  private def parseData : Some[DataFrame] = {
    import spark.implicits._

    val resourcePath = "src/main/resources/" + inputPath

    val data = spark.read.
      option("header","true").
      csv(resourcePath).
      withColumnRenamed("data", "date").
      withColumn("date", to_timestamp(col("date"),"yyyy-MM-dd\'T\'HH:mm:ss")).
      withColumnRenamed("stato", "country").
      withColumn("country",$"country".cast(StringType)).
      withColumnRenamed("denominazione_regione","region").
      withColumn("region",$"region".cast(StringType)).
      withColumnRenamed("denominazione_provincia","city").
      withColumn("city",$"city".cast(StringType)).
      withColumn("lat",$"lat".cast(FloatType)).
      withColumn("long",$"long".cast(FloatType)).
      withColumnRenamed("totale_casi","cases").
      withColumn("cases",$"cases".cast(IntegerType)).
      drop("stato").
      drop("codice_regione").
      drop("codice_provincia").
      drop("sigla_provincia").
      drop("note_it").
      drop("note_en").
      filter("city != \"In fase di definizione/aggiornamento\"")

    data.show(2)
    Some(data)
  }

  override def getData : DataFrame = {
    val someDataframe = parseData

    if (someDataframe.isEmpty)
      throw new RuntimeException("Data_not_found")

    someDataframe.x
  }

}
