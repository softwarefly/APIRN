package data

import org.apache.spark.graphx.Edge
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 模式结果数量对比：限制投资比例20%和不限制
 * Created by yzk on 2016/12/10.
 */
object ModelAddLengthAndWeight {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val savePath = "/tpin/yzk/newExperiment/"
    // 获取所有的边
    val all_edges = hiveContext.sql("SELECT * FROM tpin_edge_wwd").rdd.repartition(120).map(row => Edge(row.getAs[Long]("src_id"), row.getAs[Long]("dst_id"), (row.getAs[Double]("control_weight"), row.getAs[Double]("investment_weight"), row.getAs[Double]("trade_weight"))))
    // =================================================================================================================
    // 工具方法

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
    val hiveTable = "tpin_model_yzk_v2"
//    val hiveTable = "tpin_model_yzk_2_v2"
    val model_old = hiveContext.sql("SELECT * FROM " + hiveTable).rdd.repartition(120).map(row => (row.getAs[Long]("id"), (row.getAs[Int]("type"), row.getAs[String]("vertices"), row.getAs[String]("edges"), row.getAs[Long]("community_id"), row.getAs[Int]("exception_l"), row.getAs[Int]("exception_r"), row.getAs[Int]("month"))))

    // 重新编号，因为有重复
    val model = model_old.repartition(1).zipWithIndex().map(x => (x._2, x._1._2)).repartition(120)

    // 长度分析
    val modelWithLength = model.map(x => (x._1, (x._2, getLength(x._2._3, x._2._1))))

    // 权重分析
    def combineString(edges: (Long, Array[String])): String = {
      var array = ""
      for (edge <- edges._2) {
        array = array + (edge + "," + edges._1) + ";"
      }
      array.substring(0, array.length - 1)
    }
    val model_edges = model.map(x => (x._1, x._2._3.split(";").dropRight(1))).map(x => combineString(x)).flatMap(x => x.split(";")).map(x => x.split(",")).map(x => ((x(0).toLong, x(1).toLong), x(2).toLong))
    val model_weight = model_edges.join(all_edges.map(e => ((e.srcId, e.dstId), e.attr))).map(x => x._2).reduceByKey((a, b) => {
      (if (a._1 > b._1) a._1 else b._1, if (a._2 > b._2) a._2 else b._2, if (a._3 > b._3) a._3 else b._3)
    })
    val modelWithLengthAndWeight = modelWithLength.join(model_weight).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._2)))
    modelWithLengthAndWeight.repartition(120).saveAsObjectFile(savePath + hiveTable)

    sc.stop()
  }
}
