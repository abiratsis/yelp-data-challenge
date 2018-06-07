package com.yelp.Tests

import com.yelp.transformations.JsonToCsvTransformer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FlatSpec}

class JsonToCsvTransformerTest extends FlatSpec with BeforeAndAfterAllConfigMap {
  var inputDir = "C:\\Users\\abiratsis.OLBICO\\Desktop\\dataset"
  var outputDir = "C:\\Users\\abiratsis.OLBICO\\Desktop\\yelp-data-challenge\\out"

  implicit val top: Int = 1000
  override def beforeAll(configMap: ConfigMap): Unit = {
    inputDir = configMap.get("input").getOrElse(inputDir).toString
    outputDir= configMap.get("output").getOrElse(outputDir).toString

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
  }

  "saveBusinessData" should " produce the same number of records as in business.json" in {
    val sparkConf = new SparkConf().setAppName("yelp-data-challenge-test").setMaster("local[*]")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    JsonToCsvTransformer.apply(inputDir, outputDir).transformBusinessData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.businessSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.businessFilename}")
      .limit(top)

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/business/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Business expected:" + expected)
    println("Business actual:" + actual)
    spark.stop()

    assert(expected == actual)
  }

  "saveReviewData" should " produce the same number of records as in review.json" in {
    val sparkConf = new SparkConf().setAppName("yelp-data-challenge-test").setMaster("local[*]")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    JsonToCsvTransformer.apply(inputDir, outputDir).transformReviewData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .json(s"${inputDir}/${JsonToCsvTransformer.reviewFilename}")
      .limit(top)

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/review/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()
    println("Review expected:" + expected)
    println("Review actual:" + actual)
    spark.stop()
//    The hero that found the existence of CR in the text
//    outputDF.as("df1").join(inputDF.as("df2"), Seq("review_id"), "left").where(inputDF("review_id").isNull)
//      .select(outputDF("*"))
//      .write
//      .option("header","true")
//      .mode(SaveMode.Overwrite)
//      .csv(s"${outputDir}/review_2")

    assert(expected == actual)
  }

  "saveUserData" should " produce the same number of records as in user.json" in {
    val sparkConf = new SparkConf().setAppName("yelp-data-challenge-test").setMaster("local[*]")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    JsonToCsvTransformer.apply(inputDir, outputDir).transformUserData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.userSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.userFilename}")
      .limit(top)

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/user/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()
    println("User expected:" + expected)
    println("User actual:" + actual)
    spark.stop()

    assert(expected == actual)
  }

  "saveCheckinData" should " produce the same number of records as in checkin.json" in {
    val sparkConf = new SparkConf().setAppName("yelp-data-challenge-test").setMaster("local[*]")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    JsonToCsvTransformer.apply(inputDir, outputDir).transformCheckinData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.checkinSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.checkinFilename}")
      .limit(top)

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/checkin/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Checkin expected:" + expected)
    println("Checkin actual:" + actual)
    spark.stop()

    assert(expected == actual)
  }

  "saveTipData" should " produce the same number of records as in tip.json" in {
    val sparkConf = new SparkConf().setAppName("yelp-data-challenge-test").setMaster("local[*]")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    JsonToCsvTransformer.apply(inputDir, outputDir).transformTipData()
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.tipSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.tipFilename}")
      .limit(top)

    val outputDF = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/tip/*.csv")

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Checkin expected:" + expected)
    println("Checkin actual:" + actual)
    spark.stop()

    assert(expected == actual)
  }

  override def afterAll(configMap: ConfigMap): Unit = {

  }
}
