name := "Web Logger"

version := "1.0"

scalaVersion := "2.12.18"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"

//ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
//Global / excludeLintKeys += classLoaderLayeringStrategy
