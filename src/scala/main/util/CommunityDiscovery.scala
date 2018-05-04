package util

import entity._
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by david on 6/29/16.
  */
object CommunityDiscovery {
    //annotation of david:使用连通图方法，发现社团
    def Use_connectedComponents(tpin: Graph[VertexAttr, EdgeAttr]): Graph[VertexAttr, EdgeAttr] = {
        val newVertices = tpin.vertices.map { case (vid, attr) => (vid, vid) }.repartition(20)

        //annotation of david:仅沿前件网络发现社团，同时简化边携带的数据
        val newEdges = tpin.edges.filter(_.attr.isAntecedent()).map { case edge => Edge(edge.srcId, edge.dstId, "none") }.repartition(20)
        // 不包含交易关系边的TPIN
        val newTpin = Graph(newVertices, newEdges).persist()

        // 构建顶点社团编号
        def constructVertexCommunityId(vertexId: VertexId,
                                       attr: VertexAttr,
                                       communityIdOpt: Option[VertexId]): VertexAttr = {
            communityIdOpt match {
                case Some(communityId) => {
                    attr.community_id = communityId
                    attr
                }
                case None => {
                    attr.community_id = -1L
                    attr
                }
            }
        }

        // 构建边社团编号
        def constructEdgeCommunityId(edge: EdgeTriplet[VertexAttr, EdgeAttr]): EdgeAttr = {
            if (edge.srcAttr.community_id == edge.dstAttr.community_id) {
                edge.attr.community_id = edge.srcAttr.community_id
                edge.attr
            } else {
                edge.attr.community_id = -1L
                edge.attr
            }
        }

        // 连通图划分社团
        val communityIds = newTpin.connectedComponents.vertices
        // 社团编号添加到TPIN
        tpin.outerJoinVertices(communityIds)(constructVertexCommunityId).mapTriplets(constructEdgeCommunityId)
    }

    /**
      * 重构
      **/
    //Annotation of david:总结社团
    def ConcludeCommunityAndOutput(graph: Graph[VertexAttr, EdgeAttr],
                                   pattern1s: RDD[Pattern],
                                   sqlContext: SQLContext) = {
        val vertices = graph.vertices.filter(_._2.community_id != -1)
        val cid2vid = vertices.map[(Long, Long)] { case (vid, vattr) => (vattr.community_id, vid) }

        val communities = cid2vid.groupByKey().map { case (cid, consistNodes) => (cid, Community(cid, ArrayBuffer() ++= consistNodes)) }

        val edges = graph.edges.map { case edge => (edge.attr.community_id, (edge.srcId, edge.dstId, edge.attr)) }
        val completeAttr = edges.groupByKey().join(communities).map {
            case (cid, (list, community)) =>
                val (total_je, total_se) = list.map { case e => (e._3.trade_je, e._3.se) }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
                community.total_je = total_je
                community.total_se = total_se
                community.num_edge = list.size
                val CentralNode = list.map { case (src, dst, attr) => (src, attr.trade_je) }
                    .groupBy(_._1).map { case (src, list) => (src, list.map(_._2).sum) }
                    .toArray.reduce((a, b) => if (a._2 > b._2) a else b)._1
                community.CentralNode = CentralNode
                (cid, community)
        }
        //annotation ofi david:没匹配出模式的社团 没有输出,改为使用leftOuterJoin
        val communitiesRDD = completeAttr.leftOuterJoin(pattern1s.map { case pattern => (pattern.communityId, 1) }.groupBy(_._1))
            .map { case (cid, (community, ite)) =>
                if (!ite.isEmpty) {
                    community.num_pattern = ite.get.size
                }
                community
            }
        sqlContext.createDataFrame(communitiesRDD.
            map(community => Row(community.cid, community.total_je, community.total_se, community.CentralNode,
                community.num_edge, community.num_vertex, community.num_pattern)),
            StructType(StructField("id", LongType) :: StructField("total_je", DoubleType)
                :: StructField("total_se", DoubleType) :: StructField("central_node", LongType)
                :: StructField("num_edge", LongType) :: StructField("num_vertex", LongType)
                :: StructField("num_pattern", LongType) :: Nil))
            .write.save("/tpin/table/tpin_community_wwd")
    }
}
