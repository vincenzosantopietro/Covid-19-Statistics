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
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class InfluxDBSink(dbUrl:String, dbName: String, measurementName: String, data : ListBuffer[Point]) extends MetricsWriter {

  implicit val awaitAtMost: FiniteDuration = 10.seconds
  val spark: SparkSession = SparkSession.builder.getOrCreate


  override def storeMetrics(): Unit = {
    syncInfluxDb(new URI(dbUrl), dbName) { db =>
      for(elem <- data){
        db.write(elem)
      }
    }

    // List of column names
    /*val schema = metrics.columns
    val data = metrics.collect()

    val list = data.toList

    syncInfluxDb(new URI(dbUrl), "covid-stats") { db =>
      for(elem <- list){
        val field = createFields(elem, schema)
        val point = Point(
          time = DateTime.parse(elem(0).toString.substring(0,10)),
          measurement = measurementName,
          tags = Map("Time"->"Infected"),
          fields = field
        )
        db.write(point)
      }
    }*/
  }
}
