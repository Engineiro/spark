package com.demo.spark

import java.util.Date
import javax.ws.rs.core.MediaType

import au.com.bytecode.opencsv.CSVParser
import com.sun.jersey.api.client.ClientResponse
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write


/**
 * Created by geovaniemarquez on 1/3/15.
 */
object SparkCSVParser {

  def main(args: Array[String]) {

    val hoursInSec = 3600

    /** Used to timeShift the date by 1 day*/
    val dayInSeconds = 86400
    val numOfDays = args(1).toInt
    val timeShift = 86400 * numOfDays

    val toSeconds = 1000
    val today = new Date().getTime

    val rclientResponseClazz = classOf[ClientResponse]
    val otherNameToPos = Map(

      "INDEX" -> 0,

      /**
       * ASSUMING HOURS
       */
      "TIME_UNIT" -> 1,

      /**
       * Settings
       */
      "OP_SET1" -> 2,
      "OP_SET2" -> 3,
      "OP_SET3" -> 4
    )

    /**
     * Sensors
     */
    val sensorNameToPos = Map(

      "SENSOR1" -> 5,
      "SENSOR2" -> 6,
      "SENSOR3" -> 7,
      "SENSOR4" -> 8,
      "SENSOR5" -> 9,
      "SENSOR6" -> 10,
      "SENSOR7" -> 11,
      "SENSOR8" -> 12,
      "SENSOR9" -> 13,
      "SENSOR10" -> 14,
      "SENSOR11" -> 15,
      "SENSOR12" -> 16,
      "SENSOR13" -> 17,
      "SENSOR14" -> 18,
      "SENSOR15" -> 19,
      "SENSOR16" -> 20,
      "SENSOR17" -> 21,
      "SENSOR18" -> 22,
      "SENSOR19" -> 23,
      "SENSOR20" -> 24,
      "SENSOR21" -> 25
    )

    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark Parser")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val sc = new SparkContext(sparkConf)
    val FILE_BEING_PARSED = args(0)

    // split each document by delimiter
    // this may fail with a large enough CSV:
    // http://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/dont_call_collect_on_a_very_large_rdd.html

    // Here we can try different StorageLevels for performance comparisons.
    val tokenized = sc.textFile(FILE_BEING_PARSED).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // http://www.markhneedham.com/blog/2014/11/16/spark-parse-csv-file-and-group-by-column-value/
    tokenized.mapPartitions(lines => {
      val parser = new CSVParser(',')

      lines.map( line => {
        // write method below requires this implicit call.
        implicit val formats = Serialization.formats(NoTypeHints)

        val columns = parser.parseLine(line)
        var sensorMetrics = ArrayBuffer[JsonAST.JObject]()

        for ((sName, sValuePos) <- sensorNameToPos) {
          // json4s DSL for sending data to TSD
          val json =  ("metric" -> sName) ~
            ("timestamp" -> (hoursInSec * columns(otherNameToPos("TIME_UNIT")).toInt).+(today / toSeconds - timeShift)) ~
            ("value" -> columns(sValuePos).toDouble) ~
            ("tags" ->  Map {"index" -> columns(otherNameToPos("INDEX")).toInt})

          sensorMetrics += json
        }

        /**
         * Throttle just to see the graphs show up slower in real time
         * Otherwise a file of this size shows up immediately.
         */
        Thread.sleep(10)
        write(sensorMetrics.toList)
      })

//    }).take(1).foreach(println)
    }).foreach(RestClient.webResource
          .accept(MediaType.APPLICATION_JSON_TYPE)
          .`type`(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
          .post(rclientResponseClazz, _))

    tokenized.unpersist()
    sc.stop()

  }


}
