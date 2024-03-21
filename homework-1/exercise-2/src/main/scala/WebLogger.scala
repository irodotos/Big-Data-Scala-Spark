import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WebLogger{

  private case class Log(host: String, date: String, requestURI: String, status: Int, bytes: Int)

  private def getLogFields(str: String):Log = {
    val patternHost = """^([^\s]+\s)""".r
    val patternTime = """^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""".r
    val patternRequest = """^.*"\w+\s+([^\s]+)\s*.*"""".r
    val patternStatus = """^.*"\s+([^\s]+)""".r
    val patternBytes = """^.*\s+(\d+)$""".r

    val host = patternHost.findFirstMatchIn(str).map(_.group(1)).getOrElse("0")
    val time = patternTime.findFirstMatchIn(str).map(_.group(1)).getOrElse("0")
    val request = patternRequest.findFirstMatchIn(str).map(_.group(1)).getOrElse("0")
    val status = patternStatus.findFirstMatchIn(str).map(_.group(1).toInt).getOrElse(0)
    val bytes = patternBytes.findFirstMatchIn(str).map(_.group(1).toInt).getOrElse(0)

    Log(host, time, request, status, bytes)
  }

  private def convertToLog(base: RDD[String]): RDD[Log] = {
    base.map(getLogFields)
  }

  private def exploreContentSize(logs: RDD[Log]): Unit = {
    val contentSizes = logs.map(_.bytes)

    val minContentSize = contentSizes.min()
    val maxContentSize = contentSizes.max()
    val avgContentSize = contentSizes.mean()

    println(s"Min content size: $minContentSize")
    println(s"Max content size: $maxContentSize")
    println(s"Average content size: $avgContentSize")
  }

  private def analyzeHttpStatus(logs: RDD[Log]): Unit = {
    val statusCounts = logs
      .groupBy(_.status)
      .mapValues(_.size)

    val sortedStatusCounts = statusCounts
      .sortBy(_._2, ascending = false)
      .take(100)

    sortedStatusCounts.foreach { case (status, count) =>
      println(s"Status: $status, Frequency: $count")
    }
  }

  private def findFrequentHosts(logs: RDD[Log]): Unit = {
    val hostCounts = logs
      .groupBy(_.host)
      .mapValues(_.size)

    val frequentHosts = hostCounts
      .filter(_._2 > 10)
      .take(10)

    frequentHosts.foreach { case (host, count) =>
      println(s"Host: $host, Access Count: $count")
    }
  }

  private def findTopErrorPaths(logs: RDD[Log]): Unit = {
    val errorLogs = logs.filter(_.status != 200)

    val errorPathCounts = errorLogs
      .groupBy(_.requestURI)
      .mapValues(_.size)

    val sortedErrorPathCounts = errorPathCounts
      .sortBy(_._2, ascending = false)
      .take(10)

    sortedErrorPathCounts.foreach { case (requestURI, count) =>
      println(s"RequestURI: $requestURI, Error Count: $count")
    }
  }

  private def countUniqueHosts(logs: RDD[Log]): Unit = {
    val uniqueHosts = logs.map(_.host).distinct()

    val uniqueHostsCount = uniqueHosts.count()

    println(s"Number of unique hosts: $uniqueHostsCount")
  }

  private def count404Responses(logs: RDD[Log]): Unit = {

    val count404 = logs.count()

    println(s"Count of 404 Response Codes: $count404")
  }

  private def distinct404RequestURIs(logs: RDD[Log]): Unit = {

    val distinct404URIs = logs.map(_.requestURI).distinct().take(40)

    println("Distinct RequestURIs generating 404 errors:")
    distinct404URIs.foreach(uri => println(uri))
  }

  private def top404ErrorPaths(logs: RDD[Log]): Unit = {

    val errorPathCounts = logs
      .groupBy(_.requestURI)
      .mapValues(_.size)

    val sortedErrorPathCounts = errorPathCounts
      .sortBy(_._2, ascending = false)
      .take(20)

    println("Top 20 Paths generating the most 404 errors:")
    sortedErrorPathCounts.foreach { case (requestURI, count) =>
      println(s"RequestURI: $requestURI, Error Count: $count")
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WebLogger")
      .setMaster("local[*]")
//      .set("spark.kryo.unsafe", "false")

    val sc = new SparkContext(conf)
    val baseRdd = sc.textFile("/home/irodotos/Big-Data-Scala-Spark/homework-1/exercise-2/NASA_access_log_Jul95")


    val patternHost = """^([^\s]+\s)""".r
    val patternTime = """^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""".r
    val patternRequest = """^.*"\w+\s+([^\s]+)\s*.*"""".r
    val patternStatus = """^.*"\s+([^\s]+)""".r
    val patternBytes = """^.*\s+(\d+)$""".r


//    val badRddHost = baseRdd.filter(x => patternHost.findFirstIn(x) == None)
//    println("bad host")
//    println(badRddHost.count)
//    badRddHost.take(3).foreach(println)
//
//    val badRddTime = baseRdd.filter(x => patternTime.findFirstIn(x) == None)
//    println("bad time")
//    println(badRddTime.count)
//    badRddTime.take(3).foreach(println)
//
//    val badRddRequest = baseRdd.filter(x => patternRequest.findFirstIn(x) == None)
//    println("bad request")
//    println(badRddRequest.count)
//    badRddRequest.take(3).foreach(println)
//
//    val badRddStatus = baseRdd.filter(x => patternStatus.findFirstIn(x) == None)
//    println("bad status")
//    println(badRddStatus.count)
//    badRddStatus.take(3).foreach(println)
//
//    val badRddBytes = baseRdd.filter(x => patternBytes.findFirstIn(x) == None)
//    println("bad bytes")
//    println(badRddBytes.count)
//    badRddBytes.take(3).foreach(println)

    val cleanRDD = convertToLog(baseRdd).cache()
    println(cleanRDD.count)
    cleanRDD.take(5).foreach(println)


    exploreContentSize(cleanRDD)

    analyzeHttpStatus(cleanRDD)

    findFrequentHosts(cleanRDD)

    findTopErrorPaths(cleanRDD)

//    countUniqueHosts(cleanRDD)

    val error404RDD = cleanRDD.filter(_.status == 404).cache() // create this rdd because i need it 3 times so i cache it also

    count404Responses(error404RDD)

//    distinct404RequestURIs(error404RDD)

    top404ErrorPaths(error404RDD)
  }
}




