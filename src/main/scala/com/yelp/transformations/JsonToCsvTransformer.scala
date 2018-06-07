package com.yelp.transformations

import com.yelp.entities._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

case class JsonToCsvTransformer(inputDir: String, outputDir: String)(implicit val spark: SparkSession, implicit val top: Int = 0) {
  import spark.implicits._

  def transform : Unit= {
    transformBusinessData()
    transformReviewData()
    transformCheckinData()
    transformUserData()
    transformTipData()
  }

  def transformBusinessData(): Unit = {
    val businessDS = getDataset(JsonToCsvTransformer.businessSchema, JsonToCsvTransformer.businessFilename).as[business]
    saveBusinessData(businessDS)
  }

  def transformReviewData(): Unit ={
    val reviewDS = getDataset(JsonToCsvTransformer.reviewSchema, JsonToCsvTransformer.reviewFilename).as[review]
    saveReviewData(reviewDS)
  }

  def transformUserData(): Unit = {
    val userDS = getDataset(JsonToCsvTransformer.userSchema, JsonToCsvTransformer.userFilename).as[user]
    saveUserData(userDS)
  }

  def transformCheckinData(): Unit = {
    val checkinDS = getDataset(JsonToCsvTransformer.checkinSchema, JsonToCsvTransformer.checkinFilename).as[checkin]
    saveCheckinData(checkinDS)
  }

  def transformTipData(): Unit = {
    val tipDS = getDataset(JsonToCsvTransformer.tipSchema, JsonToCsvTransformer.tipFilename).as[tip]
    saveTipData(tipDS)
  }

  private def getDataset(schema: StructType, filename: String): DataFrame = {
    val df = spark
      .read
      .option("inferSchema", "false")
      .schema(schema)
      .json(s"${inputDir}/${filename}")
      .persist(StorageLevel.MEMORY_AND_DISK)

    if (top > 0)
      df.limit(top)
    else
      df
  }

  private def saveBusinessData(ds :Dataset[business]) : Unit = {
    val mapToStringUfd = udf((hours: Map[String, String]) => hours.mkString(","))

    ds.select(flattenSchema(JsonToCsvTransformer.businessSchema): _*)
      .withColumn("categories", concat_ws(",",col("categories")))
      .withColumn("hours", mapToStringUfd(col("hours")))
      .write
      .option("delimiter", "\t")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv(s"${outputDir}/business")
  }

  private def saveReviewData(ds :Dataset[review]) : Unit = {
    ds.withColumn("text", regexp_replace($"text", "\n+|\r+", ""))
      .write
      .option("delimiter", "\t")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv(s"${outputDir}/review")
  }

  private def saveUserData(ds :Dataset[user]) : Unit = {
    val toStringUfd = udf((elite: mutable.WrappedArray[Int]) => elite.map(e => e.toString).mkString(","))

    ds.withColumn("friends", concat_ws(",",col("friends")))
      .withColumn("elite", toStringUfd(col("elite")))
      .write
      .option("delimiter", "\t")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv(s"${outputDir}/user")
  }

  private def saveCheckinData(ds :Dataset[checkin]) : Unit = {
    val getTimeScheduleUfd = udf(( time: Map[String, Int]) => {
      var i:Int = 0
      if(time != null)
        time.foldLeft(""){(k , v) =>
          val comma = if( i < time.size - 1) "," else ""
          val r = k + s"${v._1}(${v._2})${comma}"
          i += 1
          r
        }
      else
        ""
    })

    ds.select($"business_id",
      getTimeScheduleUfd($"time.Monday").as("Monday"),
      getTimeScheduleUfd($"time.Tuesday").as("Tuesday"),
      getTimeScheduleUfd($"time.Wednesday").as("Wednesday"),
      getTimeScheduleUfd($"time.Thursday").as("Thursday"),
      getTimeScheduleUfd($"time.Friday").as("Friday"),
      getTimeScheduleUfd($"time.Saturday").as("Saturday"),
      getTimeScheduleUfd($"time.Sunday").as("Sunday"))
      .write
      .option("delimiter", "\t")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv(s"${outputDir}/checkin")
  }

  private def saveTipData(ds :Dataset[tip]) : Unit = {
    ds.write
      .option("delimiter", "\t")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv(s"${outputDir}/tip")
  }

  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }
}

object JsonToCsvTransformer{
  final val businessSchema = Encoders.product[business].schema
  final val reviewSchema = Encoders.product[review].schema
  final val userSchema = Encoders.product[user].schema
  final val checkinSchema = Encoders.product[checkin].schema
  final val tipSchema = Encoders.product[tip].schema

  final val businessFilename = "business.json"
  final val reviewFilename = "review.json"
  final val userFilename = "user.json"
  final val checkinFilename = "checkin.json"
  final val tipFilename = "tip.json"

  def main( args:Array[String] ):Unit = {
    val sparkConf = new SparkConf().setAppName("yelp-data-challenge").setMaster("local[*]")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val input = args
    val inputDir: String = getInput("--sourceDir")
    val outputDir: String = getInput("--outputDir")

    JsonToCsvTransformer.apply(inputDir, outputDir).transform
  }

  def getInput(current: String)(implicit args:Array[String]): String = {
    args.sliding(2, 2).foreach {
      case Array(key: String, value: String) if current == key => {
        return value
      }
      case _ => ""
    }
    ""
  }
}
