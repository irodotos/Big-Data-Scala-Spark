name := "Song Release Model"

version := "1.0"

scalaVersion := "2.12.18"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"
