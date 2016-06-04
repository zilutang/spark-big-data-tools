package hadoop.cluster.ad.com

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
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
    val modelname = args(1).toString
    val hostname = "dev.adc.cloudera.com"

    val sconf = new SparkConf
    sconf.setAppName("")
    val sc = new SparkContext(sconf)

    println("addata path: " + "hdfs://" + hostname + ":8020/user/hdfs/addata/" + addata)

    //read ad data
    val data = sc.textFile("hdfs://" + hostname + ":8020/user/hdfs/addata/" + addata)

    val themodel = KMeansModel.load(sc, "hdfs://" + hostname + ":8020/user/hdfs/modelsbox/" + modelname)
    var xx = sc.accumulator(0)


    /*val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
*/
    data.foreach(s => {
      println(s)
      var x = Vectors.dense(s.split(',').map(_.toDouble))
      println(x)
      var cluster_id = themodel.predict(x)
      println("cluster_id: " + cluster_id)

      var ad_id_all = "ad_" + xx
      xx += 1
      val conf = new HBaseConfiguration()
      val admin = new HBaseAdmin(conf)
      val table = new HTable(conf, "ad_clustering_table")
      val theput= new Put(Bytes.toBytes(ad_id_all))
      println("adding ad_id_all" + ad_id_all)

      table.put(theput.add(Bytes.toBytes("ad_info"), Bytes.toBytes("ad_id"), Bytes.toBytes(ad_id_all)).add(Bytes.toBytes("ad_info"), Bytes.toBytes("ad_clustering_id"), Bytes.toBytes(cluster_id.toString)))

    })
    println("end... ")
  }
}
