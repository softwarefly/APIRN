package data

import org.apache.spark.graphx.Edge
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 模式结果数量对比：限制投资比例20%和不限制
 * Created by yzk on 2016/12/10.
 */
object ModelAddLength {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    // =================================================================================================================
    // UTILS

    def getLength(edges: String, modelType: Int): Int = {
      var length = 0

      if (modelType == 1) {
        //        val edges = "3235771,1840605;3235771,2642412;1840605,2642412"

        // 将edges字符串转换为起点ID的List（去除交易边）
        val edgeList = edges.split(";").dropRight(1).map(x => x.split(",")(0))
        // 获得模式1中顶点的第二次（最后一次）的位置
        val topIndex = edgeList.lastIndexOf(edgeList(0))
        // 左链长度
        val lengthLeft = topIndex
        // 右链长度
        val lengthRight = edgeList.length - topIndex
        // 为链长度赋值，取左右链中较大的
        if (lengthLeft >= lengthRight) length = lengthLeft else length = lengthRight
      }

      if (modelType == 4) {

        //        val edges = "4093296,2230516;2230516,2711077;2230516,4093296;4093296,1378077;2711077,1378077"
        // 将edges字符串转换为起点ID的List（去除交易边）
        val edgeList = edges.split(";").dropRight(1).map(x => x.split(","))
        // 获得模式4中互锁边的第二次（最后一次）的位置【与第一次互锁边的起点终点相反】
        var topIndex = 0
        for (i <- 0 to (edgeList.length - 1)) {
          if (edgeList(i)(0) == edgeList(0)(1) && edgeList(i)(1) == edgeList(0)(0)) {
            topIndex = i
          }
        }
        // 左链长度
        val lengthLeft = topIndex - 1
        // 右链长度
        val lengthRight = edgeList.length - topIndex - 1
        // 为链长度赋值，取左右链中较大的
        if (lengthLeft >= lengthRight) length = lengthLeft else length = lengthRight
      }

      length
    }

    // =================================================================================================================
    // 获取所有的边
    val edges = hiveContext.sql("SELECT * FROM tpin_edge_wwd").rdd.repartition(120).map(row => Edge(row.getAs[Long]("src_id"), row.getAs[Long]("dst_id"), (row.getAs[Double]("control_weight"), row.getAs[Double]("investment_weight"), row.getAs[Double]("trade_weight"))))

    val model = hiveContext.sql("SELECT * FROM tpin_model_yzk_2_v2").rdd.repartition(120).map(row => (row.getAs[Long]("id"), (row.getAs[Int]("type"), row.getAs[String]("vertices"), row.getAs[String]("edges"), row.getAs[Long]("community_id"), row.getAs[Int]("exception_l"), row.getAs[Int]("exception_r"), row.getAs[Int]("month"))))

    // 长度分析
    val modelWithLengthAndWeight = model.map(x => (x._1, x._2, getLength(x._2._3, x._2._1)))
    modelWithLengthAndWeight.map(_._2._3).map(x => x.split(";")).map(x => x.length).distinct.collect
    modelWithLengthAndWeight.map(_._3).distinct.collect

    val model1 = model.filter(_._2._1 == 1).map(x => (x._1, x._2, getLength(x._2._3, x._2._1)))
    model1.map(_._2._3).map(x => x.split(";")).map(x => x.length).distinct.collect
    model1.map(_._3).distinct.collect

    val model4 = model.filter(_._2._1 == 4).map(x => (x._1, x._2, getLength(x._2._3, x._2._1)))
    model4.map(_._2._3).map(x => x.split(";")).map(x => x.length).distinct.collect
    model4.map(_._3).distinct.collect

    sc.stop()
  }
}
