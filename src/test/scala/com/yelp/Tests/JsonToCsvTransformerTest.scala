package com.yelp.Tests

import com.yelp.transformations.JsonToCsvTransformer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FlatSpec}

class JsonToCsvTransformerTest extends FlatSpec with BeforeAndAfterAllConfigMap {
  var inputDir = "input/dataset"
  var outputDir = "output/"

  val sparkConf = new SparkConf().setAppName("yelp-data-challenge-test").setMaster("local[*]")
  implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  override def beforeAll(configMap: ConfigMap): Unit = {
    inputDir = configMap.get("input").getOrElse(inputDir).toString
    outputDir= configMap.get("output").getOrElse(outputDir).toString

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
  }

  "transformBusinessData" should " produce the same number of records as in business.json" in {
//    JsonToCsvTransformer.apply(inputDir, outputDir).transformBusinessData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.businessSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.businessFilename}")

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/business/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Business expected:" + expected)
    println("Business actual:" + actual)

    assert(expected == actual)
  }

  "transformReviewData" should " produce the same number of records as in review.json" in {
//    JsonToCsvTransformer.apply(inputDir, outputDir).transformReviewData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.reviewSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.reviewFilename}")

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/review/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Review expected:" + expected)
    println("Review actual:" + actual)

//    The hero that found the existence of CR in the text
//    outputDF.as("df1").join(inputDF.as("df2"), Seq("review_id"), "left").where(inputDF("review_id").isNull)
//      .select(outputDF("*"))
//      .write
//      .option("header","true")
//      .mode(SaveMode.Overwrite)
//      .csv(s"${outputDir}/review_2")

    assert(expected == actual)
  }

  "transformUserData" should " produce the same number of records as in user.json" in {
//    JsonToCsvTransformer.apply(inputDir, outputDir).transformUserData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.userSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.userFilename}")

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/user/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()
    println("User expected:" + expected)
    println("User actual:" + actual)

    assert(expected == actual)
  }

  "transformCheckinData" should " produce the same number of records as in checkin.json" in {
//    JsonToCsvTransformer.apply(inputDir, outputDir).transformCheckinData()

    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.checkinSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.checkinFilename}")

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/checkin/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Checkin expected:" + expected)
    println("Checkin actual:" + actual)

    assert(expected == actual)
  }

  "transformTipData" should " produce the same number of records as in tip.json" in {
//    JsonToCsvTransformer.apply(inputDir, outputDir).transformTipData()

    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.tipSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.tipFilename}")

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/tip/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Checkin expected:" + expected)
    println("Checkin actual:" + actual)

    assert(expected == actual)
  }

  "runQueries" should " show queries results" in {
    JsonToCsvTransformer.apply(inputDir, outputDir).runQueries()

    assert(true)
  }
}
