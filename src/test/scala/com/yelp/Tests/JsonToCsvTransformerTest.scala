package com.yelp.Tests

import com.yelp.entities._
import com.yelp.transformations.JsonToCsvTransformer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FlatSpec}

class JsonToCsvTransformerTest extends FlatSpec with BeforeAndAfterAllConfigMap {
  val sparkConf = new SparkConf().setAppName("yelp-data-challenge").setMaster("local[*]")
  implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  var inputDir = "C:\\Users\\abiratsis.OLBICO\\Desktop\\dataset"
  var outputDir = "C:\\Users\\abiratsis.OLBICO\\Desktop\\yelp-data-challenge\\out"

  var trans: JsonToCsvTransformer = _

  import spark.implicits._
  override def beforeAll(configMap: ConfigMap): Unit = {
    inputDir = configMap.get("input").getOrElse(inputDir).toString
    outputDir= configMap.get("output").getOrElse(outputDir).toString

    JsonToCsvTransformer.businessSchema.printTreeString()
    JsonToCsvTransformer.reviewSchema.printTreeString()
    JsonToCsvTransformer.userSchema.printTreeString()
    JsonToCsvTransformer.checkinSchema.printTreeString()

    trans = JsonToCsvTransformer.apply(inputDir, outputDir)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
  }

  "saveBusinessData" should " produce the same number of records as in business.json" in {
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.businessSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.businessFilename}")
      .as[business]

    trans.transformBusinessData()

    val outputDF = spark
      .read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", "\t")
      .schema(JsonToCsvTransformer.businessSchema)
      .csv(s"${outputDir}/business/*.csv")
      .as[business]

    val expected = inputDF.count()
    val actual = outputDF.count()

    println("Business expected:" + expected)
    println("Business actual:" + actual)
    assert(expected == actual)
  }

  "saveReviewData" should " produce the same number of records as in review.json" in {
    import spark.implicits._
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.reviewSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.reviewFilename}")
      .as[review]

    trans.transformReviewData()

    val outputDF = spark
      .read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", "\t")
      .schema(JsonToCsvTransformer.reviewSchema)
      .csv(s"${outputDir}/review/*.csv")
      .as[review]

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

  "saveUserData" should " produce the same number of records as in user.json" in {
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.userSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.userFilename}")
      .as[user]

    trans.transformUserData()

    val outputDF = spark
      .read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", "\t")
      .schema(JsonToCsvTransformer.userSchema)
      .csv(s"${outputDir}/user/*.csv")
      .as[user]

    val expected = inputDF.count()
    val actual = outputDF.count()
    println("User expected:" + expected)
    println("User actual:" + actual)

    assert(expected == actual)
  }

  "saveCheckinData" should " produce the same number of records as in checkin.json" in {
    val inputDF = spark
      .read
      .option("inferSchema", "false")
      .schema(JsonToCsvTransformer.checkinSchema)
      .json(s"${inputDir}/${JsonToCsvTransformer.checkinFilename}")
      .as[checkin]

    trans.transformCheckinData()

    val outputDF = spark
      .read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", "\t")
      .schema(JsonToCsvTransformer.checkinSchema)
      .csv(s"${outputDir}/checkin/*.csv")
      .as[checkin]

    val expected = inputDF.count()
    val actual = outputDF.count()
    println("Checkin expected:" + expected)
    println("Checkin actual:" + actual)
    assert(expected == actual)
  }

  override def afterAll(configMap: ConfigMap): Unit = {

  }
}
