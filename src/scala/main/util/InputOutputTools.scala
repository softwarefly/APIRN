package util

import java.io.{File, PrintWriter}
import java.net.URI

import entity.{EdgeAttr, VertexAttr, VertexType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
  * Created by david on 6/29/16.
  */
object InputOutputTools {

    val partition = 24

    def saveAsCsvFile(graph: Graph[VertexAttr, EdgeAttr]) = {

        val writer1 = new PrintWriter(new File("res/OutputCsv/vertices2.csv"))
        val writer2 = new PrintWriter(new File("res/OutputCsv/edges2.csv"))
        writer1.write("id,name,type\n")
        writer2.write("source,target,relation\n")
        for ((vid, vattr) <- graph.vertices.collect()) {
            writer1.write(s"$vid, ${vattr.toCsvString}\n")
        }
        for (edge <- graph.edges.collect()) {
            writer2.write(s"${edge.srcId},${edge.dstId},${edge.attr.toCsvString}\n")
        }
        writer1.close()
        writer2.close()
        println("SaveAsCsvFile finished!")
    }

    def getFromProgram(sc: SparkContext): Graph[VertexAttr, EdgeAttr] = {
        val vertices = sc.parallelize(Seq(
            (1L, VertexAttr(VertexType.FDDBR_ONLY, "142635199504031019", true)),
            (2L, VertexAttr(VertexType.NSR_ONLY, "202160628", true))
        ))
        val edges = sc.parallelize(Seq(
            Edge(1L, 2L, EdgeAttr(w_control = 1.0))
        ))
        Graph(vertices, edges)
    }

    def getFromCsv(sc: SparkContext, vertexPath: String, edgePath: String): Graph[VertexAttr, EdgeAttr] = {
        //    val edgesTxt=sc.textFile("file:///home/david/IdeaProjects/Find_IL_Edge/lib/InputCsv/edges1.csv")
        //    val vertexTxt=sc.textFile("file:///home/david/IdeaProjects/Find_IL_Edge/lib/InputCsv/vertices1.csv")
        val edgesTxt = sc.textFile(edgePath)
        val vertexTxt = sc.textFile(vertexPath)
        val vertices = vertexTxt.filter(!_.startsWith("id")).map(_.split(",")).map {
            case v =>
                val ishuman = {
                    if (v(2).equals("company")) false else true
                }
                (v(0).toLong, VertexAttr(VertexType.UNDEFINE, v(1), ishuman))
        }
        val edges = edgesTxt.filter(!_.startsWith("source")).map(_.split(",")).map {
            case e =>
                Edge(e(0).toLong, e(1).toLong, EdgeAttr.fromString(e(2)))
        }
        Graph(vertices, edges)
    }

    /**
      * 重构
      **/
    //annotation of david: 保存带社团编号的TPIN到Hive点表和边表中
    def saveAsHiveTable(sqlContext: SQLContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]): Unit = {
        // 顶点表名称
        val vertexTableName = "tpin_vertex_w_IL"
        // 边表名称
        val edgeTableName = "tpin_edge_w_IL"

        // 顶点表结构
        val vertexStructType = StructType(StructField("id", LongType)
            :: StructField("type", IntegerType) :: StructField("sbh", StringType)
            :: StructField("ishuman", BooleanType) :: StructField("community_id", LongType) :: Nil)
        // 边表结构
        val edgeStructType = StructType(StructField("src_id", LongType) :: StructField("dst_id", LongType)
            :: StructField("control_weight", DoubleType) :: StructField("investment_weight", DoubleType)
            :: StructField("lock_weight", DoubleType) :: StructField("trade_weight", DoubleType)
            :: StructField("community_id", LongType)
            :: StructField("trade_je", DoubleType) :: StructField("tz_je", DoubleType) :: StructField("trade_se", DoubleType) :: Nil)
        // 顶点集写入顶点表
        sqlContext.createDataFrame(tpinWithCommunityId.vertices.map { case (vid, vattr) => Row(
            vid, vattr.vertex_type.toInt, vattr.nsrsbh, vattr.ishuman, vattr.community_id)
        }, vertexStructType)
            .write.save(s"/tpin/table/$vertexTableName")
        // 边集写入边表
        sqlContext.createDataFrame(tpinWithCommunityId.edges.map { case Edge(src, dst, eattr) => Row(
            src, dst, eattr.w_control, eattr.w_tz, eattr.w_IL, eattr.w_trade, eattr.community_id, eattr.trade_je, eattr.tz_je, eattr.se)
        }, edgeStructType)
            .write.save(s"/tpin/table/$edgeTableName")
    }

    //annotation of david:从Hive点表和边表中读取带社团编号的TPIN
    def getFromHiveTable(hiveContext: HiveContext) = {
        val verticesInfo = hiveContext.sql("SELECT id,type,sbh,community_id,ishuman FROM tpin_vertex_w_IL").rdd.repartition(120)

        val vertices = verticesInfo.map { case row =>
            val vertexAttr = VertexAttr(VertexType.fromInt(row.getAs[Int]("type")), row.getAs[String]("sbh"), row.getAs[Boolean]("ishuman"))
            vertexAttr.community_id = row.getAs[Long]("community_id")
            (row.getAs[Long]("id"), vertexAttr)
        }

        val edgeInfo = hiveContext.sql("SELECT * FROM tpin_edge_w_IL").rdd.repartition(120)

        val edges = edgeInfo.map { case row =>
            val src_id = row.getAs[Long]("src_id")
            val dst_id = row.getAs[Long]("dst_id")
            val control_weight = row.getAs[Double]("control_weight")
            val investment_weight = row.getAs[Double]("investment_weight")
            val trade_weight = row.getAs[Double]("trade_weight")
            val lock_weight = row.getAs[Double]("lock_weight")
            val community_id = row.getAs[Long]("community_id")
            val trade_je = row.getAs[Double]("trade_je")
            val tz_je = row.getAs[Double]("tz_je")
            val trade_se = row.getAs[Double]("trade_se")

            val edgeAttr = EdgeAttr(control_weight, investment_weight, trade_weight)
            edgeAttr.community_id = community_id
            edgeAttr.trade_je = trade_je
            edgeAttr.se = trade_se
            edgeAttr.tz_je = tz_je
            if (lock_weight == 1.0)
                edgeAttr.is_IL = true
            Edge(src_id, dst_id, edgeAttr)
        }
        Graph(vertices, edges)
    }

    //annotation of david:从Hive原始数据中构建TPIN
    def getFromHiveData(hiveContext: HiveContext): Graph[VertexAttr, EdgeAttr] = {
        //annotation of david:纳税人扩展表提取法定代表人、纳税人
        // 两个ID均不可能为空，且因为经常有个人控制个人，使得ID不能直接作为VertexID，仍然需要重新编号
        val controlRows = hiveContext.sql("SELECT FDDBRZJHM_ID, NSRSBH_ID FROM tpin_nsr_kz").rdd.persist
        // 法定代表人
        val legalRepresentatives = controlRows.map(row => row.getAs[Double]("FDDBRZJHM_ID").toString).distinct().persist
        // 纳税人扩展表中的纳税人
        val taxpayersInControlRows = controlRows.map(row => row.getAs[Double]("NSRSBH_ID").toString).distinct().persist
        // 控制关系（权重固定1.0）
        val controlRelations = controlRows.map(row => ((row.getAs[Double]("FDDBRZJHM_ID").toString, row.getAs[Double]("NSRSBH_ID").toString), 1.0)).distinct.persist

        //annotation of david:纳税人投资方表提取投资方、纳税人和投资比例（条件：编号合法、投资比例大于0小于等于1）
        val investmentRows_man = hiveContext.sql("SELECT ZJHM_ID, NSRSBH_ID,tzbl,tzje FROM tpin_nsr_tzf where TZFXZ_ID LIKE '4%'")
            .rdd.filter(row =>
            row.getAs("tzbl") != null && row.getAs[Double]("tzbl") > 0.0 &&
                row.getAs[Double]("tzbl") <= 1.0).persist
        // 投资方
        val investors_man = investmentRows_man.map(row => row.getAs[String]("ZJHM_ID")).distinct().persist
        // 纳税人投资方表中的纳税人
        val taxpayersInInvestmentRows_man = investmentRows_man.map(row => row.getAs[Double]("NSRSBH_ID").toString).distinct().persist
        // 投资关系（权重大于0小于等于1）
        val investmentRelations_man = investmentRows_man.map(row =>
            ((row.getAs[String]("ZJHM_ID"), row.getAs[Double]("NSRSBH_ID").toString), (row.getAs[Double]("tzbl"), row.getAs[Double]("tzje"))))
            .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
            .map(relation => if (relation._2._1 < 1.0) relation else ((relation._1._1, relation._1._2), (1.0, relation._2._2))).persist


        // 纳税人投资方表提取投资方、纳税人和投资比例（条件：编号合法、投资比例大于0小于等于1）
        val investmentRows_company = hiveContext.sql("SELECT ZJHM_ID, NSRSBH_ID,tzbl,tzje,TZFXZ_ID FROM tpin_nsr_tzf")
            .rdd.filter(row =>
            !row.getAs[String]("TZFXZ_ID").startsWith("4") &&
                row.getAs("tzbl") != null && row.getAs[Double]("tzbl") > 0.0 &&
                row.getAs[Double]("tzbl") <= 1.0).persist
        // 投资方
        val investors_company = investmentRows_company.map(row => row.getAs[String]("ZJHM_ID")).distinct().persist
        // 纳税人投资方表中的纳税人
        val taxpayersInInvestmentRows_company = investmentRows_company.map(row => row.getAs[Double]("NSRSBH_ID").toString).distinct().persist
        // 投资关系（权重大于0小于等于1）
        val investmentRelations_company = investmentRows_company.map(row =>
            ((row.getAs[String]("ZJHM_ID"), row.getAs[Double]("NSRSBH_ID").toString), (row.getAs[Double]("tzbl"), row.getAs[Double]("tzje"))))
            .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
            .map(relation => if (relation._2._1 < 1.0) relation else ((relation._1._1, relation._1._2), (1.0, relation._2._2))).persist


        // 发票表提权销方纳税人、购方纳税人和交易金额（条件：编号合法、交易金额大于0）
        val tradeData = hiveContext.sql("SELECT XFNSRSBH_ID, GFNSRSBH_ID,se,je FROM tpin_2015_fp")
            .rdd.filter(row =>
            row.getAs("se") != null && row.getAs[Double]("se") > 0.0).persist
        // 同一销方纳税人的所有交易金额求和
        val tradeSums = tradeData.map(row => (row.getAs[Double]("XFNSRSBH_ID").toString, row.getAs[Double]("je"))).reduceByKey((amount1, amount2) => amount1 + amount2).persist
        // 交易关系（权重大于0小于等于1）
        val tradeRelations = tradeData.map(row => (row.getAs[Double]("XFNSRSBH_ID").toString,
            (row.getAs[Double]("GFNSRSBH_ID").toString, row.getAs[Double]("je"), row.getAs[Double]("se"))))
            .join(tradeSums).map { case (xf, ((gf, je, se), je_sum)) => ((xf, gf), (je / je_sum, je, se)) }
            .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) }
            .map { case ((xf, gf), (jybl, je, se)) =>
                if (jybl < 1.0)
                    ((xf, gf), (jybl, je, se))
                else
                    ((xf, gf), (1.0, je, se))
            }.persist

        // 合并纳税人扩展表中纳税人和纳税人投资方表中纳税人
        val taxpayers = taxpayersInControlRows.union(taxpayersInInvestmentRows_company)
            .union(taxpayersInInvestmentRows_man).distinct().persist

        // 纳税人顶点（类型为二进制001） RDD[(String, (String, Int))]
        val taxpayerVertices = taxpayers.map(taxpayer => (taxpayer, VertexAttr(VertexType.NSR_ONLY, taxpayer, false))).persist
        // 法定代表人顶点（类型为二进制010）
        val legalRepresentativesVertices = legalRepresentatives.map(legalRepresentative =>
            (legalRepresentative, VertexAttr(VertexType.FDDBR_ONLY, legalRepresentative, true))).persist
        // 投资方顶点（类型为二进制100）
        val investorVertices_company = investors_company.map(investor => (investor, VertexAttr(VertexType.TZF_ONLY, investor, false))).persist
        val investorVertices_man = investors_man.map(investor => (investor, VertexAttr(VertexType.TZF_ONLY, investor, true))).persist

        //annotation of david: 合并纳税人顶点、法定代表人顶点和投资方顶点（类型为二进制求或），重新编号
        // index,type,sbh,ishuman
        val vertices = taxpayerVertices.union(legalRepresentativesVertices).union(investorVertices_company).union(investorVertices_man)
            .reduceByKey(VertexAttr.combine).zipWithIndex
            .map { case ((sbh, attr), index) => (index, attr) }.persist(StorageLevel.MEMORY_AND_DISK)

        // 控制关系边（起点、终点分别和编号后顶点Join获取新编号，类型为（控制权重，0，0））
        val controlEdges = controlRelations.map(relation => (relation._1._1, (relation._1._2, relation._2)))
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (srcsbh, ((dstsbh, w_control), index)) => (dstsbh, (index, w_control)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (dstsbh, ((srcindex, w_control), dstindex)) => ((srcindex, dstindex), EdgeAttr(w_control, 0.0, 0.0)) }.persist
        // 投资关系边（起点、终点分别和编号后顶点Join获取新编号，类型为（0，投资权重，0））
        val investmentEdges = investmentRelations_man.union(investmentRelations_company)
            .map(relation => (relation._1._1, (relation._1._2, relation._2._1, relation._2._2)))
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (srcsbh, ((dstsbh, w_tz, tzje), index)) => (dstsbh, (index, w_tz, tzje)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (dstsbh, ((srcindex, w_tz, tzje), dstindex)) =>
                val edgeAttr = EdgeAttr(0.0, w_tz, 0.0)
                edgeAttr.tz_je = tzje
                ((srcindex, dstindex), edgeAttr)
            }.persist
        // 交易关系边（起点、终点分别和编号后顶点Join获取新编号，类型为（0，0，交易权重））
        val tradeEdges = tradeRelations.map { case ((src, dst), attr) => (src, (dst, attr)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (src, ((dst, attr), srcindex)) => (dst, (srcindex, attr)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (dst, ((srcindex, (w_trade, je, se)), dstindex)) =>
                val edgeAttr = EdgeAttr(0.0, 0.0, w_trade)
                edgeAttr.se = se
                edgeAttr.trade_je = je
                ((srcindex, dstindex), edgeAttr)
            }.persist
        // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
        val edges = controlEdges.union(investmentEdges).union(tradeEdges)
            .reduceByKey(EdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2)
            .map(edge => Edge(edge._1._1, edge._1._2, edge._2)).persist
        //annotation of david:获取度大于0的顶点
        // Vertices with no edges are not returned in the resulting RDD.
        val degrees = Graph(vertices, edges).degrees.persist
        // 使用度大于0的顶点和边构建图
        Graph(vertices.join(degrees).map(vertex => (vertex._1, vertex._2._1)), edges).persist()
    }

    /**
      * 重构
      *
      * @param sqlContext
      * @return
      */
    def getFromMySQL(sqlContext: SQLContext): Graph[VertexAttr, EdgeAttr] = {
        val nsr = MySQLUtil.getTable(sqlContext,"wd_nsr_kz").select("nsrdzdah", "nsrsbh", "fddbrzjhm").repartition(partition).persist()

        //annotation of david:纳税人扩展表提取法定代表人、纳税人
        // 两个ID均不可能为空，且因为经常有个人控制个人，使得ID不能直接作为VertexID，仍然需要重新编号
        val controlRows = nsr.rdd.filter(row => row.getAs("fddbrzjhm") != null &&
            row.getAs[String]("fddbrzjhm").matches("[0-9A-Za-z\\-]{8,}") && row.getAs("nsrsbh") != null &&
            row.getAs[String]("nsrsbh").matches("[0-9A-Za-z\\-]{8,}")).persist
        // 法定代表人
        val legalRepresentatives = controlRows.map(row => row.getAs[String]("fddbrzjhm")).distinct().persist
        // 纳税人扩展表中的纳税人
        val taxpayersInControlRows = controlRows.map(row => row.getAs[String]("nsrsbh")).distinct().persist
        // 控制关系（权重固定1.0）
        val controlRelations = controlRows.map(row => ((row.getAs[String]("fddbrzjhm"), row.getAs[String]("nsrsbh")), 1.0)).distinct.persist

        //annotation of david:纳税人投资方表提取投资方、纳税人和投资比例（条件：编号合法、投资比例大于0小于等于1）
        val tzf = MySQLUtil.getTable(sqlContext, "nsr_tzf").select("zjhm", "nsrdzdah", "tzbl", "tzje", "tzfxz_id").repartition(partition).persist()
        val investmentRows_man = tzf.join(nsr, tzf("nsrdzdah") === nsr("nsrdzdah")).where("tzfxz_id like '4%'")
            .rdd.filter(row => row.getAs("zjhm") != null &&
            row.getAs[String]("zjhm").matches("[0-9A-Za-z\\-]{15,}") &&
            row.getAs("nsrsbh") != null && row.getAs[String]("nsrsbh").matches("[0-9A-Za-z\\-]{8,}") &&
            row.getAs("tzbl") != null && row.getAs[java.math.BigDecimal]("tzbl").doubleValue() > 0.0 &&
            row.getAs[java.math.BigDecimal]("tzbl").doubleValue() <= 1.0).persist
        // 投资方
        val investors_man = investmentRows_man.map(row => row.getAs[String]("zjhm")).distinct().persist
        // 纳税人投资方表中的纳税人
        val taxpayersInInvestmentRows_man = investmentRows_man.map(row => row.getAs[String]("nsrsbh")).distinct().persist
        // 投资关系（权重大于0小于等于1）
        val investmentRelations_man = investmentRows_man.map(row =>
            ((row.getAs[String]("zjhm"), row.getAs[String]("nsrsbh")), (row.getAs[java.math.BigDecimal]("tzbl").doubleValue(), row.getAs[java.math.BigDecimal]("tzje").doubleValue())))
            .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
            .map(relation => if (relation._2._1 < 1.0) relation else ((relation._1._1, relation._1._2), (1.0, relation._2._2))).persist

        // 纳税人投资方表提取投资方、纳税人和投资比例（条件：编号合法、投资比例大于0小于等于1）
        val investmentRows_company = tzf.join(nsr, tzf("nsrdzdah") === nsr("nsrdzdah"))
            .rdd.filter(row =>
            row.getAs("tzfxz_id") != null &&
                !row.getAs[String]("tzfxz_id").startsWith("4") &&
                row.getAs("zjhm") != null &&
                row.getAs[String]("zjhm").matches("[0-9A-Za-z\\-]{8,}") &&
                row.getAs("nsrsbh") != null && row.getAs[String]("nsrsbh").matches("[0-9A-Za-z\\-]{8,}") &&
                row.getAs("tzbl") != null && row.getAs[java.math.BigDecimal]("tzbl").doubleValue() > 0.0 &&
                row.getAs[java.math.BigDecimal]("tzbl").doubleValue() <= 1.0).persist
        // 投资方
        val investors_company = investmentRows_company.map(row => row.getAs[String]("zjhm")).distinct().persist
        // 纳税人投资方表中的纳税人
        val taxpayersInInvestmentRows_company = investmentRows_company.map(row => row.getAs[String]("nsrsbh")).distinct().persist
        // 投资关系（权重大于0小于等于1）
        val investmentRelations_company = investmentRows_company.map(row =>
            ((row.getAs[String]("zjhm"), row.getAs[String]("nsrsbh")), (row.getAs[java.math.BigDecimal]("tzbl").doubleValue(), row.getAs[java.math.BigDecimal]("tzje").doubleValue())))
            .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
            .map(relation => if (relation._2._1 < 1.0) relation else ((relation._1._1, relation._1._2), (1.0, relation._2._2))).persist


        // 发票表提权销方纳税人、购方纳税人和交易金额（条件：编号合法、交易金额大于0）
        val jy = MySQLUtil.getTable(sqlContext, "ls_dxj_2015_fp_fwsk_cglmx").repartition(partition).persist()
        val tradeData = jy.rdd.filter(row =>
            row.getAs("xfnsrsbh") != null && row.getAs[String]("xfnsrsbh").matches("[0-9A-Za-z\\-]{8,}")
                && row.getAs("gfnsrsbh") != null && row.getAs[String]("gfnsrsbh").matches("[0-9A-Za-z\\-]{8,}")
                && row.getAs("se") != null && row.getAs[java.math.BigDecimal]("se").doubleValue() > 0.0).persist
        // 同一销方纳税人的所有交易金额求和
        val tradeSums = tradeData.map(row => (row.getAs[String]("xfnsrsbh"), row.getAs[java.math.BigDecimal]("je").doubleValue())).reduceByKey((amount1, amount2) => amount1 + amount2).persist
        // 交易关系（权重大于0小于等于1）
        val tradeRelations = tradeData.map(row => (row.getAs[String]("xfnsrsbh"),
            (row.getAs[String]("gfnsrsbh"), row.getAs[java.math.BigDecimal]("je").doubleValue(), row.getAs[java.math.BigDecimal]("se").doubleValue())))
            .join(tradeSums).map { case (xf, ((gf, je, se), je_sum)) =>
            ((xf, gf), (je / je_sum, je, se))
        }
            .reduceByKey { case (a, b) =>
                (a._1 + b._1, a._2 + b._2, a._3 + b._3)
            }.map { case ((xf, gf), (jybl, je, se)) =>
            if (jybl < 1.0)
                ((xf, gf), (jybl, je, se))
            else
                ((xf, gf), (jybl, je, se))
        }.persist

        // 合并纳税人扩展表中纳税人和纳税人投资方表中纳税人
        val taxpayers = taxpayersInControlRows.union(taxpayersInInvestmentRows_company)
            .union(taxpayersInInvestmentRows_man).distinct().persist

        // 纳税人顶点（类型为二进制001） RDD[(String, (String, Int))]
        val taxpayerVertices = taxpayers.map(taxpayer => (taxpayer, VertexAttr(VertexType.NSR_ONLY, taxpayer, false))).persist
        // 法定代表人顶点（类型为二进制010）
        val legalRepresentativesVertices = legalRepresentatives.map(legalRepresentative =>
            (legalRepresentative, VertexAttr(VertexType.FDDBR_ONLY, legalRepresentative, true))).persist
        // 投资方顶点（类型为二进制100）
        val investorVertices_company = investors_company.map(investor => (investor, VertexAttr(VertexType.TZF_ONLY, investor, false))).persist
        val investorVertices_man = investors_man.map(investor => (investor, VertexAttr(VertexType.TZF_ONLY, investor, true))).persist

        //annotation of david: 合并纳税人顶点、法定代表人顶点和投资方顶点（类型为二进制求或），重新编号
        // index,type,sbh,ishuman
        val vertices = taxpayerVertices.union(legalRepresentativesVertices).union(investorVertices_company).union(investorVertices_man)
            .reduceByKey(VertexAttr.combine).zipWithIndex
            .map { case ((sbh, attr), index) => (index, attr) }.persist(StorageLevel.MEMORY_AND_DISK)

        // 控制关系边（起点、终点分别和编号后顶点Join获取新编号，类型为（控制权重，0，0））
        val controlEdges = controlRelations.map(relation => (relation._1._1, (relation._1._2, relation._2)))
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (srcsbh, ((dstsbh, w_control), index)) => (dstsbh, (index, w_control)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (dstsbh, ((srcindex, w_control), dstindex)) => ((srcindex, dstindex), EdgeAttr(w_control, 0.0, 0.0)) }.persist
        // 投资关系边（起点、终点分别和编号后顶点Join获取新编号，类型为（0，投资权重，0））
        val investmentEdges = investmentRelations_man.union(investmentRelations_company)
            .map(relation => (relation._1._1, (relation._1._2, relation._2._1, relation._2._2)))
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (srcsbh, ((dstsbh, w_tz, tzje), index)) => (dstsbh, (index, w_tz, tzje)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (dstsbh, ((srcindex, w_tz, tzje), dstindex)) =>
                val edgeAttr = EdgeAttr(0.0, w_tz, 0.0)
                edgeAttr.tz_je = tzje
                ((srcindex, dstindex), edgeAttr)
            }.persist
        // 交易关系边（起点、终点分别和编号后顶点Join获取新编号，类型为（0，0，交易权重））
        val tradeEdges = tradeRelations.map { case ((src, dst), attr) => (src, (dst, attr)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (src, ((dst, attr), srcindex)) => (dst, (srcindex, attr)) }
            .join(vertices.map(vertex => (vertex._2.nsrsbh, vertex._1)))
            .map { case (dst, ((srcindex, (w_trade, je, se)), dstindex)) =>
                val edgeAttr = EdgeAttr(0.0, 0.0, w_trade)
                edgeAttr.se = se
                edgeAttr.trade_je = je
                ((srcindex, dstindex), edgeAttr)
            }.persist
        // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
        val edges = controlEdges.union(investmentEdges).union(tradeEdges)
            .reduceByKey(EdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2)
            .map(edge => Edge(edge._1._1, edge._1._2, edge._2)).persist
        //annotation of david:获取度大于0的顶点
        // Vertices with no edges are not returned in the resulting RDD.
        val degrees = Graph(vertices, edges).degrees.persist
        // 使用度大于0的顶点和边构建图
        Graph(vertices.join(degrees).map(vertex => (vertex._1, vertex._2._1)), edges).persist()
    }

    // 保存TPIN到HDFS
    def saveAsObjectFile(tpin: Graph[VertexAttr, EdgeAttr], sparkContext: SparkContext): Unit = {
        // 顶点集文件路径
        val verticesFilePath = "/tpin/object/vertices_wwd"
        // 边集文件路径
        val edgesFilePath = "/tpin/object/edges_wwd"
        // 对象方式保存顶点集
        tpin.vertices.repartition(partition).saveAsObjectFile(verticesFilePath)
        // 对象方式保存边集
        tpin.edges.repartition(partition).saveAsObjectFile(edgesFilePath)
    }

    // 从HDFS获取TPIN
    def getFromHDFS_Object(sparkContext: SparkContext): Graph[VertexAttr, EdgeAttr] = {
        // 顶点集文件路径
        val verticesFilePath = "/tpin/object/vertices_wwd"
        // 边集文件路径
        val edgesFilePath = "/tpin/object/edges_wwd"
        // 对象方式获取顶点集
        val vertices = sparkContext.objectFile[(VertexId, VertexAttr)](verticesFilePath)
        // 对象方式获取边集
        val edges = sparkContext.objectFile[Edge[EdgeAttr]](edgesFilePath)
        // 构建图
        Graph(vertices, edges)
    }

    // 从HDFS获取TPIN
    def getFromHdfsObject(sparkContext: SparkContext, verticesFilePath: String, edgesFilePath: String): Graph[VertexAttr, EdgeAttr] = {
        // 对象方式获取顶点集
        val vertices = sparkContext.objectFile[(VertexId, VertexAttr)](verticesFilePath)
        // 对象方式获取边集
        val edges = sparkContext.objectFile[Edge[EdgeAttr]](edgesFilePath)
        // 构建图
        Graph(vertices, edges)
    }

    /**
      * 从hdfs中获取各月的交易数据集合，数据采用的是最初由税友导出的全部交易数据
      **/
    def getMonthsTradeDataFromHive(hiveContext: HiveContext) = {
        val TradeData = hiveContext.sql("SELECT xfnsrsbh,gfnsrsbh,je,se,KPRQ FROM ls_dxj_2015_fp_fwsk_cglmx").rdd.repartition(120)

        import java.text.SimpleDateFormat
        val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

        val TradeDataRDD = TradeData.map(row =>
            (row.getAs[String]("xfnsrsbh"), row.getAs[String]("gfnsrsbh"), row.getAs[String]("je").toDouble, row.getAs[String]("se").toDouble, row.getAs[String]("KPRQ")))

        val data01 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2014/12/31 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/2/1 00:00:00"))))
        val data02 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/1/31 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/3/1 00:00:00"))))
        val data03 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/2/28 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/4/1 00:00:00"))))
        val data04 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/3/31 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/5/1 00:00:00"))))
        val data05 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/4/30 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/6/1 00:00:00"))))
        val data06 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/5/31 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/7/1 00:00:00"))))
        val data07 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/6/30 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/8/1 00:00:00"))))
        val data08 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/7/31 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/9/1 00:00:00"))))
        val data09 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/8/31 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/10/1 00:00:00"))))
        val data10 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/9/30 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/11/1 00:00:00"))))
        val data11 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/10/31 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2015/12/1 00:00:00"))))
        val data12 = TradeDataRDD.filter(x => (dateFormat.parse(x._5).after(dateFormat.parse("2015/11/30 00:00:00")) && dateFormat.parse(x._5).before(dateFormat.parse("2016/1/1 00:00:00"))))

        val dataList = List(data01, data02, data03, data04, data05, data06, data07, data08, data09, data10, data11, data12)

        dataList
    }

}
