package data

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 模板类，在创建Scala类时，可以直接复制此类
 * Created by yzk on 2016/11/3.
 */
object GraphChangeHyInfo {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)
    val savePath = "/tpin/yzk/dynamic/"

    // =================================================================================================================

    println("Step1:获取行业信息hyInfo1.csv,hyInfo2.csv,并对其进行合并处理")

    /**
     * 整合行业信息
     * @return 返回二元组形式，（行业编号：4位字符串，（行业：文字说明，标志位：是否匹配到税负预警值，税负预警值：此处的税负为百分数，即1.45，实际对应的数值为1.45%，若匹配不到则返回0.0））
     */
    def combineHyInfo(): RDD[(String, (String, Boolean, Double))] = {
      val path1 = savePath + "hyInfo1.csv"
      val path2 = savePath + "hyInfo2.csv"

      val hyInfo1 = sc.textFile(path1)
      val hyInfo2 = sc.textFile(path2)

      // 有税负预警值的行业
      val hyHaveSF = hyInfo2.map(x => (x.split(","))).filter(_.length == 3).map(x => (x(0), x(1), true, x(2).toDouble))

      // 未匹配到税负预警值的行业
      val temp1 = hyInfo1.map(x => (x.split(","))).map(x => (x(0), x(1)))
      val temp2 = (temp1.join(hyHaveSF.map(x => (x._1, x._2))).map(x => (x._1, (x._2._1, 1))) ++ temp1.map(x => (x._1, (x._2, 1)))).reduceByKey((a, b) => (a._1, a._2 + b._2)).filter(_._2._2 == 1).map(x => (x._1, x._2._1))
      val hyNotHaveSF = (temp2 ++ hyInfo2.map(x => (x.split(","))).filter(_.length == 2).map(x => (x(0), x(1)))).reduceByKey((a, b) => a).map(x => (x._1, x._2, false, 0.0))

      (hyHaveSF ++ hyNotHaveSF).map(x => (x._1, (x._2, x._3, x._4)))
    }

    // 行业信息
    val hyInfo = combineHyInfo()

    // =================================================================================================================

    println("Step2:获取纳税人行业信息")

    /**
     * 获取纳税人行业信息
     * @param hiveContext
     * @return 返回二元组形式，（ID，识别号，行业编号）
     */
    def getNsrHyInfo(hiveContext: HiveContext): RDD[(Long, String, String)] = {
      val tradePath = "/tpin/csv/2015TradeDataComplete.csv"
      val tradeData = sc.textFile(tradePath).repartition(120).map(x => x.split(","))

      val nsrWithHyInfo = tradeData.map(x => (x(0), x(1), x(5), x(6)))

      val xf = nsrWithHyInfo.filter(x => (x._3 != "\"\"")).map(x => (x._1, x._3)).distinct()
      val gf = nsrWithHyInfo.filter(x => (x._4 != "\"\"")).map(x => (x._2, x._4)).distinct()

      // 注意：在此处的纳税人行业匹配结果中，有1083位纳税人匹配到行业多余一种，其中1077位纳税人2种，5位3种，1位4种，在为纳税人节点添加行业相关的说明及税负预警值信息时，采用行业税负预警值最低的行业作为其行业
      val nsrHyInfo = (xf ++ gf).distinct().map(x => (x._1.replace("\"", ""), x._2.replace("\"", ""))).filter(_._2.length == 14).map(x => (x._1, x._2.substring(8, 12)))
      val verticesInfo = hiveContext.sql("SELECT id, type, sbh, community_id FROM tpin_vertex_wwd").rdd.repartition(120).map(row => (row.getAs[Long]("id"), (row.getAs[Int]("type"), row.getAs[String]("sbh"), row.getAs[Long]("community_id"))))
      verticesInfo.filter(_._2._1 % 2 == 1).map(x => (x._2._2, x._1)).join(nsrHyInfo).map(x => (x._2._1, x._1, x._2._2))
    }

    // 纳税人行业信息
    val nsrHyInfo = getNsrHyInfo(hiveContext)

    // =================================================================================================================

    println("Step3:为纳税人节点添加行业相关的说明及税负预警值信息")

    /**
     * 为纳税人节点添加行业相关的说明及税负预警值信息，在匹配到多个行业时选取具有税负预警值的行业，若都有，则选取税负预警值最低的行业
     * @param nsrHyInfo
     * @param hyInfo
     * @return 返回二元组形式，（ID,(识别号，行业编号，行业说明，匹配到税负预警值标志位，税负预警值)）
     */
    def addMoreNsrHyInfo(nsrHyInfo: RDD[(Long, String, String)], hyInfo: RDD[(String, (String, Boolean, Double))]): RDD[(Long, (String, String, String, Boolean, Double))] = {

      val nsrWithMoreHyInfo = nsrHyInfo.map(x => (x._3, (x._1, x._2))).join(hyInfo).map(x => (x._2._1._1, (x._2._1._2, x._1, x._2._2._1, x._2._2._2, x._2._2._3)))

      // 对于匹配到多个行业的纳税人进行行业的筛选
      nsrWithMoreHyInfo.reduceByKey((a, b) => {
        var temp = ("未匹配到纳税人识别号", "未匹配到纳税人行业编号", "没有对应的行业说明", false, 0.00)
        if (a._4 && b._4) {
          if (a._5 <= b._5) temp = a else temp = b
        } else if (a._4 && !b._4) {
          temp = a
        } else if (!a._4 && b._4) {
          temp = b
        }
        temp
      })
    }

    val nsrWithMoreHyInfo = addMoreNsrHyInfo(nsrHyInfo, hyInfo)
    // =================================================================================================================

    println("Step4:获得每个月的点集")

    /**
     * 获得每个月的点集
     * @param hiveContext
     * @param month
     */
    def getMonthGraphVertices(hiveContext: HiveContext, month: Int): RDD[(Long, (Int, String, Long, Double, String, Double, Int, Int))] = {
      //      val month = 2
      val vertices = hiveContext.sql("SELECT id, type, sbh, community_id, sf, hybh, hyyjz, ychz, month FROM tpin_vertex_yzk WHERE month = " + month)
      vertices.rdd.repartition(120).map(row => (row.getAs[Long]("id"), (row.getAs[Int]("type"), row.getAs[String]("sbh"), row.getAs[Long]("community_id"), row.getAs[Double]("sf"), row.getAs[String]("hybh"), row.getAs[Double]("hyyjz"), row.getAs[Int]("ychz"), row.getAs[Int]("month"))))
    }

    // =================================================================================================================

    println("Step5:对点集添加行业相关信息")

    val vertices2 = getMonthGraphVertices(hiveContext, 2)
    vertices2.join(nsrWithMoreHyInfo)

    for (i <- 2 to 12) {
      println("===================")
      val vertices = getMonthGraphVertices(hiveContext, i)
      val join = vertices.join(nsrWithMoreHyInfo)
      println(vertices.filter(_._2._5 != "").count)
      println(join.filter(x => (x._2._1._5 == x._2._2._2)).count)
    }

    /*
    ===================
    53822
    53563
    ===================
    70708
    70395
    ===================
    86109
    85758
    ===================
    87260
    86898
    ===================
    79265
    78908
    ===================
    75297
    74965
    ===================
    72885
    72563
    ===================
    74217
    73882
    ===================
    75219
    74852
    ===================
    77956
    77582
    ===================
    85188
    84783
    */

    sc.stop()
  }
}
