ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "io.supertable"

lazy val root = (project in file("."))
  .settings(
    name := "spark-supertable",
    
    // Spark dependencies (provided scope for cluster deployment)
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      
      // HTTP client for REST catalog
      "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
      "com.softwaremill.sttp.client3" %% "circe" % "3.9.0",
      
      // JSON parsing
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",
      
      // Arrow for data exchange
      "org.apache.arrow" % "arrow-vector" % "14.0.0",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % "test"
    ),
    
    // Assembly settings for fat JAR
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
