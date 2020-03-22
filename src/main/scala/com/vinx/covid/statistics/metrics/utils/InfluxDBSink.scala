package com.vinx.covid.statistics.metrics.utils
import java.net
import java.net.URI

import com.pygmalios.reactiveinflux._
import com.pygmalios.reactiveinflux.{ReactiveInfluxConfig, ReactiveInfluxDbName}
import com.pygmalios.reactiveinflux.sync.{SyncReactiveInflux, SyncReactiveInfluxDb}
import com.typesafe.config.Config
import java.time.format.DateTimeFormatter

import com.pygmalios.reactiveinflux.Point.FieldKey

import scala.concurrent.duration._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.concurrent.duration._

class InfluxDBSink(dbUrl:String, dbName: String) extends MetricsWriter {

  implicit val awaitAtMost = 10.seconds
  val spark = SparkSession.builder.getOrCreate

  import spark.implicits._
  private def createDbClient(): Unit = {
    syncInfluxDb(new URI(dbUrl), "covid-italy") { db =>

      /*val point = Point(
        time = DateTime.now(),
        measurement = "measurement",
        tags = Map("t1" -> "A", "t2" -> "B"),
        fields = Map(
          "f1" -> 10.3, // BigDecimal field
          "f2" -> "x",  // String field
          "f3" -> -1,   // Long field
          "f4" -> true) // Boolean field
      )
      db.write(point)*/


      // Synchronously read the written point
      //val queryResult = db.query("SELECT * FROM measurement1")

      // Print the single point to the console
      //println(queryResult.row.mkString)
    }
  }

  def createFields(row: Row, schema: Array[String]) = {
    var field = Map[FieldKey,FieldValue]()

    for(i <- 2 until schema.length){
      if(i == 2)
        field.+=((schema(i), StringFieldValue(row(i).toString)))
      else
        field.+=((schema(i), BigDecimalFieldValue(row(i).toString.toInt)))
    }
    field
  }


  override def storeMetrics(metrics: DataFrame): Unit = {
    // List of column names
    val schema = metrics.columns
    val data = metrics.collect()

    val list = data.toList

    syncInfluxDb(new URI(dbUrl), "covid-stats") { db =>
      for(elem <- list){
        val field = createFields(elem, schema)
        val point = Point(
          time = DateTime.parse(elem(0).toString.substring(0,10)),
          measurement = "contry-overall",
          tags = Map("Time"->"Infected"),
          fields = field
        )
        db.write(point)
      }
    }
  }
}
