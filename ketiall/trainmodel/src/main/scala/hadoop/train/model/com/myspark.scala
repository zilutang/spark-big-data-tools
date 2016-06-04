package hadoop.train.model.com

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
//spark-submit --master local[1] --jars trainmodel.jar--classhadoop.cluster.ad.com.myspark trainmodel.jar kmeans100000.txt model2 100 20


object myspark {

  def printUsage(): Unit = {
    System.out.println("Usage: " + "<data.txt> <model1> <100> <20>")
  }
  def main(args: Array[String]): Unit = {
    println("Starting...")
    println("args: " + args.mkString("\n"))
    if (args.length < 4) {
      printUsage
      System.exit(1)
    }

    val traindata = args(0).toString
    val modelname = args(1).toString
    val numofcluster = args(2).toInt
    val numofiter = args(3).toInt
    val hostname = "dev.adc.cloudera.com"
    //val hostname = "192.168.1.9"

    val sconf = new SparkConf
    sconf.setAppName("")
    val sc = new SparkContext(sconf)
    //val data = sc.textFile("k:/data/kmeans100000.txt")

    println("traindata path: " + "hdfs://" + hostname + ":8020/user/hdfs/traindata/" + traindata)
    val data = sc.textFile("hdfs://" + hostname + ":8020/user/hdfs/traindata/" + traindata)
    //val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = numofcluster
    val numIterations = numofiter
    val timestart = System.currentTimeMillis()
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val timeend = System.currentTimeMillis()


    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Save and load model
    clusters.save(sc, "hdfs://" + hostname + ":8020/user/hdfs/modelsbox/" + modelname)
    val clustercenters = clusters.clusterCenters
    println("Clusters number: " + numofcluster)
    println("The centers of the " + numofcluster + " clusters are: ")
    clustercenters.foreach(println _)

    println("num of Cluster: " + numClusters)
    println("num of Iterations: " + numIterations)
    println("The time cost in training models is: "+ (timeend - timestart)/1000 + " seconds")
  }
}
