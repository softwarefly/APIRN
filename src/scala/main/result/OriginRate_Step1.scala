package result

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计各步骤的查全率、查准率
 * Created by yzk on 2016/12/12.
 */
object OriginRate_Step1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val savePath = "/tpin/yzk/newExperiment/"

    for (month <- 2 to 12) {
      // val month = 2
      // 从HDFS获取初始识别结果集
      val recognitionResult = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + month)

      val tradeEdges = hiveContext.sql("SELECT src_id,dst_id,je FROM tpin_edge_yzk where month = " + month + " and je > 0").rdd.repartition(120).map(row => ((row.getAs[Long]("src_id"), row.getAs[Long]("dst_id"), row.getAs[Double]("je")))).map(x => Edge(x._1, x._2, x._3))
      val tradeGraph = Graph.fromEdges(tradeEdges, None)

      val setC = tradeGraph.vertices.join(recognitionResult)

      val setB = recognitionResult.filter(_._2._1._1 == "Black")

      val B = setB.count().toDouble
      val C = setC.count().toDouble
      val C_and_B = setC.join(setB).count().toDouble

      val recall = C_and_B / B
      val precision = C_and_B / C

      println((B.toLong, C.toLong, C_and_B.toLong, recall, precision))
    }

    sc.stop()
  }
}
