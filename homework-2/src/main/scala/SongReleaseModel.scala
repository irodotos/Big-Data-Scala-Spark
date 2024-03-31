import breeze.linalg.DenseVector
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{LabeledPoint => MLabeledPoint}
import org.apache.spark.ml.linalg.{Vectors => MLVectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

import scala.collection.mutable.ListBuffer


///***** Quadratic Feature extraction for 5.1 ********/

///*****************************************************/
//
//
///**** Use of pipelines ******************************/
//import org.apache.spark.ml.feature.PolynomialExpansion
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.evaluation.RegressionEvaluator
//
//val numIters = 500
//val reg = 1e-10
//val alpha = .2
//val useIntercept = true
//val polynomial_expansion = (new PolynomialExpansion).setInputCol("features").setOutputCol("polyFeatures").setDegree(?)
//val lr3 = new LinearRegression()
//lr3.setMaxIter(?).setRegParam(?).setElasticNetParam(?).setFitIntercept(?).setFeaturesCol("polyFeatures")
//
//val pipeline = new Pipeline()
//pipeline.setStages(?) //there are two stages here that you have to set.
//
//val model=? //need to fit. Use the train Dataframe
//val predictionsDF=? //Produce predictions on the test set. Use method transform.
//val evaluator = new RegressionEvaluator()
//evaluator.setMetricName("rmse")
//val rmseTestPipeline = evaluator.evaluate(predictionsDF)
//println(rmseTestPipeline)

object SongReleaseModel {

  private def stringToLabeledPoint(str: String): LabeledPoint = {
    val parts = str.split(",")
    val label = parts.head.toDouble
    val features = Vectors.dense(parts.tail.map(_.toDouble))
    LabeledPoint(label, features)
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("WebLogger")
      .setMaster("local[*]")


    val spark = SparkSession.builder
      .appName("demo")
      .config(conf)
      .getOrCreate()

//    val sc = new SparkContext(conf)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val baseRdd = sc.textFile("/home/irodotos/Big-Data-Scala-Spark/homework-2/dataset.csv")

    // ---------------- EXERCISE 1.2 --------------------
    val numOfDataPoints = baseRdd.count()
    println("num of data points = ", numOfDataPoints)

    println("THe top 5 data points are")
    baseRdd.take(5).foreach(println)
    // ---------------------------------------------------

    // ---------------- EXERCISE 1.3 ---------------------
    val parsedPointsRdd = baseRdd.map(stringToLabeledPoint)

    val firstLabel = parsedPointsRdd.first().label
    println("The label of the first element is = " ,firstLabel)

    val firstFeatures = parsedPointsRdd.first().features
    println("The features of the first element are = ", firstFeatures)

    val firstFeaturesLength = firstFeatures.size
    println("The length of the features of the first element is = ", firstFeaturesLength)

    val labels = parsedPointsRdd.map(_.label)
    val smallestLabel = labels.min()
    val largestLabel = labels.max()
    println("The smallest label is = ", smallestLabel)
    println("The largest label is = ", largestLabel)
    // -------------------------------------------------------

    // --------------------- EXERCISE 1.4 -----------------------
    val shiftedPointsRdd = parsedPointsRdd.map(lp => LabeledPoint(lp.label - smallestLabel, lp.features))

    val shiftedLabels = shiftedPointsRdd.map(_.label)
    val minShiftedLabel = shiftedLabels.min()
    val maxShiftedLabel = shiftedLabels.max()
    println("The min label of shiftedPointsRdd is = ", minShiftedLabel)
    println("The max label of shiftedPointsRdd is = ", maxShiftedLabel)
    // ----------------------------------------------------------

    // ----------------------- EXERCISE 1.5 ------------------------
    val weights = Array(.8, .1, .1)
    val seed = 42
    val Array(trainData, valData, testData) = shiftedPointsRdd.randomSplit(weights, seed)

    trainData.cache()
    valData.cache()
    testData.cache()

    val trainDataCount = trainData.count()
    val valDataCount = valData.count()
    val testDataCount = testData.count()
    val totalDataCount = trainDataCount + valDataCount + testDataCount
    val shiftedPointsRddCount = shiftedPointsRdd.count()
    println("Element count of trainData = ", trainDataCount)
    println("Element count of valData = ", valDataCount)
    println("Element count of testData = ", testDataCount)
    println("Total element count of trainData, valData, and testData = ", totalDataCount)
    println("Element count of shiftedPointsRdd = ", shiftedPointsRddCount)
    // ---------------------------------------------------------------

    // --------------------- EXERCSICE 2.1 -------------------------
    val totalLabel = trainData.map(_.label).reduce(_ + _)
    val count = trainData.count()
    val averageLabel = totalLabel / count
    println("The average label on the training set is = ", averageLabel)

    def baseLineModel(lp: LabeledPoint): Double = averageLabel
    // -------------------------------------------------------------

    // --------------------- EXERCISE 2.2 -----------------------
    def calcRmse(predictionsAndLabels: RDD[(Double, Double)]): Double = {
      val metrics = new RegressionMetrics(predictionsAndLabels)
      metrics.rootMeanSquaredError
    }
    // -----------------------------------------------------------

    // ------------------ EXERCISE 2.3 -----------------------------
    val predsNLabelsTrain = trainData.map(lp => (baseLineModel(lp), lp.label))
    val predsNLabelsVal = valData.map(lp => (baseLineModel(lp), lp.label))
    val predsNLabelsTest = testData.map(lp => (baseLineModel(lp), lp.label))

    val rmseTrain = calcRmse(predsNLabelsTrain)
    val rmseVal = calcRmse(predsNLabelsVal)
    val rmseTest = calcRmse(predsNLabelsTest)
    println("RMSE of trainData = ", rmseTrain)
    println("RMSE of valData = ", rmseVal)
    println("RMSE of testData = ", rmseTest)
    // -------------------------------------------------------------

    // ------------------ EXERCISE 3.1 ------------------------------
    def gradientSummand(weights: DenseVector[Double], lp: LabeledPoint): DenseVector[Double] = {
      val features = DenseVector(lp.features.toArray)
      val dotProduct = weights.dot(features)
      val error = dotProduct - lp.label
      features * error
    }
    val example_w = DenseVector(1.0, 1.0, 1.0)
    val example_lp = LabeledPoint(2.0, Vectors.dense(3, 1, 4))
    println("Gradient summand of the first example = ", gradientSummand(example_w, example_lp))

    val example_w2 = DenseVector(.24, 1.2, -1.4)
    val example_lp2 = LabeledPoint(3.0, Vectors.dense(-1.4, 4.2, 2.1))
    println("Gradient summand of the second example = ", gradientSummand(example_w2, example_lp2))
    // -----------------------------------------------------------------------

    // ------------------- EXERCISE 3.2 ------------------------------
    def getLabeledPrediction(weights: DenseVector[Double], lp: LabeledPoint): (Double, Double) = {
      val features = DenseVector(lp.features.toArray)
      val prediction = weights.dot(features)
      (prediction, lp.label)
    }
    // --------------------------------------------------------------------

    // ------------------- EXERCISE 3.3 --------------------
    def lrgd(trData: RDD[LabeledPoint], numIter: Int): (DenseVector[Double], List[Double]) = {
      val n = trData.count
      val d = trData.first.features.size
      val alpha = 0.001
      val errorTrain = new ListBuffer[Double]
      var weights = new DenseVector(Array.fill[Double](d)(0.0))
      for (i <- 0 until numIter){
        val gradient = trData.map(lp => gradientSummand(weights, lp)).reduce(_ + _)
        val alpha_i = alpha / (n * Math.sqrt(i+1))
        weights -= alpha_i * gradient 
        //update errorTrain
        val predsNLabelsTrain = trData.map(lp => getLabeledPrediction(weights, lp)) //convert the training set into an RDD of (predictions, labels)
        errorTrain += calcRmse(predsNLabelsTrain)
      }
      (weights, errorTrain.toList)
    }

//    val exampleN = 4
//    val exampleD = 3
//    val exampleData = sc.parallelize(trainData.take(exampleN)).map(lp => LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.slice(0, exampleD))))
    val exampleNumIters = 150
    val (exampleWeights, exampleErrorTrain) = lrgd(trainData, exampleNumIters)
    println("example weights in the lrgd function = ", exampleWeights)
    println("example error train in the lrgd function = ",exampleErrorTrain)
    // ----------------------------------------------------------

    // ---------------------- EXERCISE 3.4 -----------------------
    val predsAndLabelsValid = valData.map(lp => getLabeledPrediction(exampleWeights, lp))
    val rmseValid = calcRmse(predsAndLabelsValid)
    println("RMSE on validation set = ", rmseValid)
    // ----------------------------------------------------------

    // --------------------- EXERCISE 4.1 -------------------------
    import spark.implicits._

    val trainDataDF = trainData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
    val valDataDF = valData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
    val testDataDF = testData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF

    val lr = new LinearRegression().setMaxIter(50).setRegParam(0.1).setFitIntercept(true)
    val lrModel = lr.fit(trainDataDF)

    println("Coefficients = ", lrModel.coefficients)
    println("Intercept = ", lrModel.intercept)
    val trainingSummary = lrModel.summary
    println("RMSE = ", trainingSummary.rootMeanSquaredError)

    val valPredictions = lrModel.transform(valDataDF)
    println("10 predictions = ")
    val valPredictionsFirst10 = valPredictions.select("prediction").take(10)
    valPredictionsFirst10.foreach(println)
    // -------------------------------------------------------------

    // --------------------- EXERCISE 4.2 ---------------------------
    val regParams = Array(1e-10, 1e-5, 1)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, regParams)
      .build()

    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel = cv.fit(trainDataDF)
    val bestModel = cvModel.bestModel.asInstanceOf[LinearRegressionModel]
    val bestRmse = cvModel.avgMetrics.min

    println("Best RMSE = ", bestRmse)
    println("Best regularization parameter = ", bestModel.getRegParam)
    // -------------------------------------------------------------

    // -------------------- EXERCISE 5.1 -------------------------
    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }

    def quadFeatures(lp: LabeledPoint) = {
      val crossFeatures = lp.features.toArray.toList cross lp.features.toArray.toList
      val sqFeatures = crossFeatures.map(x => x._1 * x._2).toArray
      LabeledPoint(lp.label, Vectors.dense(sqFeatures))
    }
    val crossTrainDataRDD = trainData.map(lp => quadFeatures(lp))
    val crossTrainDataDF = crossTrainDataRDD.toDF()

    val crossValDataRDD = valData.map(lp => quadFeatures(lp))
    val crossValDataDF = crossValDataRDD.toDF()

    val crossTestDataRDD = testData.map(lp => quadFeatures(lp))
    val crossTestDataDF = crossTestDataRDD.toDF()
    // ----------------------------------------------------------

    // ------------------- EXERCISE 5.2 -------------------------
    var numIterations = 500
    var stepSize = 0.00000001
    var regParam = 1e-10
    import org.apache.spark.mllib.regression.LinearRegressionWithSGD

    val model = LinearRegressionWithSGD.train(crossTrainDataRDD, numIterations, stepSize, regParam)
//    val model = LinearRegressionWithSGD.train(crossTrainDataRDD, numIterations, stepSize, regParam)
    // -------------------------------------------------------

  }


}