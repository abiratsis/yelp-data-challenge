name := "yelp-data-challenge"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,

    "org.json4s" %% "json4s-jackson" % "3.5.3",
    "org.json4s" %% "json4s-native" % "3.5.3",
    "com.propensive" %% "rapture-json-jackson" % "1.0.6",

    "org.scala-lang" % "scala-reflect" % "2.11.8",
    "org.scala-lang" % "scala-compiler" % "2.11.8",

    "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
    "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Runtime,

    "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test",
    "org.scalacheck" %% "scalacheck" % "1.14.0"
  )
}