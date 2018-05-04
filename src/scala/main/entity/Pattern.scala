package entity

import entity.Pattern.NodePair
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by david on 6/29/16.
  */
class Pattern(val pid: Int, val edges: Seq[NodePair], val vertices: Seq[VertexId]) extends Serializable {
    var communityId: Long = 0
    val src_id: Long = edges.last._1
    val dst_id: Long = edges.last._2
    var src_flag: Int = 0
    var dst_flag: Int = 0
    var src_sf: Double = 0
    var dst_sf: Double = 0
    var src_hyyj: Double = 0
    var dst_hyyj: Double = 0
    var month: Int = 0

    override def toString = s"Pattern(Edges{${edges.mkString(",")}}, Vertex $vertices)"

    def src_isHighSF(delta: Double): Boolean = {
        src_sf > src_hyyj * (2 - delta)
    }

    def dst_isHighSF(delta: Double): Boolean = {
        dst_sf > dst_hyyj * (2 - delta)
    }

    def canEqual(other: Any): Boolean = other.isInstanceOf[Pattern]

    override def equals(other: Any): Boolean = other match {
        case that: Pattern =>
            (that canEqual this) &&
                src_id == that.src_id &&
                dst_id == that.dst_id
        case _ => false
    }

    override def hashCode(): Int = {
        val state = Seq(src_id, dst_id)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
}

object Pattern {
    type NodePair = (VertexId, VertexId)

    def count1InCount(x: Int) = {
        var result: Int = 0
        var xx: Int = x
        var tmp: Int = 0
        while (xx != 0) {
            tmp = xx % 2
            if (tmp == 1)
                result = result + 1
            xx = xx / 2
        }
        result
    }

    // getFromHiveTable(hiveContext,2,3)
    def getFromHiveTable(hiveContext: HiveContext, month: Int, patternType: Int) = {
        //hiveContext.sql("select ychz from tpin_zhibiao_wwd where month = 2 and id = 3559003 ").rdd.map(row=> row.getAs[Int](0)).collect
        //    hiveContext.sql("select ychz from tpin_zhibiao_wwd where month = 2 and id = 1867165 ").rdd.map(row=> row.getAs[Int](0)).collect
        //    hiveContext.sql("select src_flag from tpin_pattern_month_wwd where month = 2 and src_id = 3559003 ").rdd.map(row=> row.getAs[Int](0)).collect
        //hiveContext.sql("select count(*) from tpin_pattern_month_wwd ").rdd.map(row=> row.getAs[Long](0)).collect
        val patternInfo = hiveContext.sql("SELECT src_id,dst_id,src_flag,dst_flag,src_sf,dst_sf,src_hyyj,dst_hyyj FROM tpin_pattern_month_wwd where month = " + month + " and type = " + patternType).rdd

        val patterns = patternInfo.map(row => (row.getAs[Long]("src_id"), row.getAs[Long]("dst_id"), row.getAs[Int]("src_flag"), row.getAs[Int]("dst_flag")
            , row.getAs[Double]("src_sf"), row.getAs[Double]("dst_sf"), row.getAs[Double]("src_hyyj"), row.getAs[Double]("dst_hyyj")))
            .distinct.map { case e =>
            val pattern = Pattern(patternType, Seq((e._1, e._2)), Seq(e._1, e._2))
            pattern.src_flag = e._3
            pattern.dst_flag = e._4
            pattern.src_sf = e._5
            pattern.dst_sf = e._6
            pattern.src_hyyj = e._7
            pattern.dst_hyyj = e._8
            pattern
        }
        patterns
    }

    def CheckIntersect(pattern1: RDD[Pattern], pattern2: RDD[Pattern]) = {

        val distinct_pattern1 = pattern1.distinct()
        println("distinct pattern1 count:" + distinct_pattern1.count())

        val distinct_pattern2 = pattern2.distinct()
        println("distinct pattern2 count:" + distinct_pattern2.count())

        val intersect = distinct_pattern1.map(e => (e.edges.last, 1)).join(distinct_pattern2.map(e => (e.edges.last, 1)))
        println("find pattern1 and pattern2 intersect count:" + intersect.count())

    }

    def apply(pid: Int, edges: Seq[NodePair], vertices: Seq[VertexId]) = {
        new Pattern(pid, edges, vertices)
    }

    // 保存关联交易模式（单向环和双向环）到Hive
    def saveAsHiveTable2(hiveContext: HiveContext, pattern: RDD[Pattern]): Unit = {
        // 构建边集字段（边间分号分隔，边内起点终点逗号分隔）
        def constructEdgesString(edges: Seq[(VertexId, VertexId)]): String = {
            var toReturn = ""
            for ((src, dst) <- edges) {
                toReturn += src + "," + dst + ";"
            }
            toReturn.substring(0, toReturn.length - 1)
        }

        // 构建顶点集字段（逗号分隔）
        def constructVerticesString(vertices: Seq[VertexId]): String = {
            var toRetrun = ""
            for (vid <- vertices) {
                toRetrun += vid + ","
            }
            toRetrun.substring(0, toRetrun.length - 1)
        }

        // 写入模式表
        hiveContext.createDataFrame(pattern.zipWithIndex.
            map(pattern1WithId =>
                Row(pattern1WithId._2, pattern1WithId._1.pid,
                    pattern1WithId._1.src_id, pattern1WithId._1.dst_id,
                    pattern1WithId._1.src_flag, pattern1WithId._1.dst_flag,
                    pattern1WithId._1.src_sf, pattern1WithId._1.dst_sf,
                    pattern1WithId._1.src_hyyj, pattern1WithId._1.dst_hyyj,
                    pattern1WithId._1.month,
                    constructVerticesString(pattern1WithId._1.vertices)
                    , constructEdgesString(pattern1WithId._1.edges),
                    pattern1WithId._1.communityId)),
            StructType(StructField("id", LongType) :: StructField("type", IntegerType)
                :: StructField("src_id", LongType) :: StructField("dst_id", LongType)
                :: StructField("src_flag", IntegerType) :: StructField("dst_flag", IntegerType)
                :: StructField("src_sf", DoubleType) :: StructField("dst_sf", DoubleType)
                :: StructField("src_hyyj", DoubleType) :: StructField("dst_hyyj", DoubleType)
                :: StructField("month", IntegerType)
                :: StructField("vertices", StringType) :: StructField("edges", StringType)
                :: StructField("community_id", LongType) :: Nil))
            .write.insertInto("tpin_pattern_month_wwd")
    }

    /**
      * 重构
      **/
    // 保存关联交易模式（单向环和双向环）到Hive
    def saveAsHiveTable(sqlContext: SQLContext, pattern: RDD[Pattern]): Unit = {
        // 构建边集字段（边间分号分隔，边内起点终点逗号分隔）
        def constructEdgesString(edges: Seq[(VertexId, VertexId)]): String = {
            var toReturn = ""
            for ((src, dst) <- edges) {
                toReturn += src + "," + dst + ";"
            }
            toReturn.substring(0, toReturn.length - 1)
        }

        // 构建顶点集字段（逗号分隔）
        def constructVerticesString(vertices: Seq[VertexId]): String = {
            var toRetrun = ""
            for (vid <- vertices) {
                toRetrun += vid + ","
            }
            toRetrun.substring(0, toRetrun.length - 1)
        }

        // 写入模式表
        sqlContext.createDataFrame(pattern.zipWithIndex.
            map(pattern1WithId => Row(pattern1WithId._2, pattern1WithId._1.pid, constructVerticesString(pattern1WithId._1.vertices), constructEdgesString(pattern1WithId._1.edges), pattern1WithId._1.communityId)),
            StructType(StructField("id", LongType) :: StructField("type", IntegerType) :: StructField("vertices", StringType) :: StructField("edges", StringType) :: StructField("community_id", LongType) :: Nil))
            .write.save("/tpin/table/tpin_pattern_wwd")
    }

}
