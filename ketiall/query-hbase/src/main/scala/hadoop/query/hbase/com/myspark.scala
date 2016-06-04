package hadoop.query.hbase.com

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get,Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//spark-submit --master local[1] --jars cluster-ad.jar --class hadoop.cluster.ad.com.myspark cluster-ad.jar ad1.txt model1


object myspark {

  def printUsage(): Unit = {
    System.out.println("Usage: " + "<ad-data> <modelname>")
  }
  def main(args: Array[String]): Unit = {
    println("Starting...")
    println("args: " + args.mkString("\n"))
    if (args.length < 2) {
      printUsage
      System.exit(1)
    }


    val addata = args(0).toString

    val conf = new HBaseConfiguration()
    val admin = new HBaseAdmin(conf)
    val table = new HTable(conf, "ad_clustering_table")
    val theScan = new Scan()
    println("querying cluster id: " + addata)


  }
}
