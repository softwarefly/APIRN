package util

import entity.{EdgeAttr, VertexAttr}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.collection.mutable.LinkedList

/**
 * Created by david on 6/30/16.
 */
object AddNewAttr {
    type Path = Seq[VertexId]
    type Paths = Seq[Seq[VertexId]]
    def findFrequence(d: Seq[(VertexId, Seq[Long])]): Seq[(Long, Long)] = {
        val frequencies = d
        val result =
            for (i <- 0 until frequencies.length) yield
                for (j <- i + 1 until frequencies.length) yield {
                    val (vid1, list1) = frequencies(i)
                    val (vid2, list2) = frequencies(j)
                    if (list1.intersect(list2).size >= 2)
                        Option(Iterable((vid1, vid2), (vid2, vid1)))
                    else
                        Option.empty
                }
        result.flatten.filter(!_.isEmpty).map(_.get).flatten
    }

    def getPath(graph: Graph[Paths, EdgeAttr],weight: Double,maxIteratons:Int=Int.MaxValue,initLength:Int = 1) = {
        // 发送路径
        def sendPaths(edge: EdgeContext[Paths,Int,Paths],
                      length: Int): Unit = {
            // 过滤掉仅含交易权重的边 以及非起点
            val satisfied = edge.srcAttr.filter(_.size==length).filter(!_.contains(edge.dstId))
            if (satisfied.size>0){
                // 向终点发送顶点路径集合
                edge.sendToDst(satisfied.map(_ ++ Seq(edge.dstId)))
            }
        }
        // 路径长度
        val degreesRDD = graph.degrees.cache()
        // 使用度大于0的顶点和边构建图
        var preproccessedGraph = graph
            .subgraph(epred = triplet =>
                triplet.attr.isAntecedent(weight)
            )
            .outerJoinVertices(degreesRDD)((vid,vattr,degreesVar)=>(vattr,degreesVar.getOrElse(0)))
            .subgraph(vpred = {case (vid,(vattr,degreesVar))=>
                degreesVar > 0
                }
            )
            .mapVertices{case (vid,(list,degreesVar))=> list }
            .mapEdges(edge=>1)
            .cache()
        var i = initLength
        var messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_,i), _ ++ _)
        var activeMessages = messages.count()
        var prevG: Graph[Paths, Int] = null
        while (activeMessages>0 && i < maxIteratons) {
            prevG = preproccessedGraph
            preproccessedGraph=preproccessedGraph.joinVertices[Paths](messages)((id, vd, path) => vd++path).cache()
            println("iterator "+i+" finished!")
            i +=1
            val oldMessages = messages
            messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_,i), _ ++ _).cache()
            activeMessages=messages.count()
            oldMessages.unpersist(blocking = false)
            prevG.unpersistVertices(blocking = false)
            prevG.edges.unpersist(blocking = false)
        }
        //         printGraph[Paths,Int](preproccessedGraph)
        preproccessedGraph.vertices
    }

    //annotation of david:添加严格版的互锁，依赖的投资边权重必须大于0.2
    def addStrictIL(graph: Graph[VertexAttr, EdgeAttr],weight:Double): Graph[VertexAttr, EdgeAttr] = {
        //annotation of david:仅从单条路径发送消息，造成了算法的不一致
        // 含义：每个人所控制及间接控制企业的列表
        val initialGraph = graph
            .mapVertices{case (id, vattr) =>
                if(vattr.ishuman) Seq(Seq(id)) else Seq[Seq[VertexId]]()
            }
        val messagesOfCompanys = getPath(initialGraph,weight).flatMap(_._2).filter(_.size>1).groupBy(_.head).mapValues(lists=>lists.map(_.last).toSeq)
        //     println("messagesOfCompanys:")
        //     messagesOfCompanys.collect().foreach(println)
        val messagesOffDirection = messagesOfCompanys
            .flatMap { case (vid, controlList) =>
                controlList.map(controledId =>
                    (controledId, (vid, controlList))
                )
            }.groupByKey().map { case (vid, ite) => (vid, ite.toSeq) }
        //     println("messagesOffDirection:")
        //     messagesOffDirection.collect().foreach(println)
        val newILEdges = messagesOffDirection
            .flatMap { case (dstid, list) => findFrequence(list) }
            .distinct
            .join(graph.vertices)
            .map { case (src, (dst, vertexAttr)) =>
                val edgeAttr = EdgeAttr()
                edgeAttr.is_IL = true
                edgeAttr.community_id = vertexAttr.community_id
                Edge(src, dst, edgeAttr)
            }
        Graph(graph.vertices, graph.edges.union(newILEdges))
    }

    def Add_Loose_IL(context: SparkContext, graph: Graph[VertexAttr, EdgeAttr]): Graph[VertexAttr, EdgeAttr] = {

    //annotation of david:the company need to know all of its board
    val Messages_off_direction = graph.subgraph(epred = e => e.attr.isTZ()&&e.attr.w_tz>0.2).aggregateMessages[LinkedList[Long]](
      ctx => ctx.sendToDst(LinkedList(ctx.srcId)),_ ++ _)
    val NewEdges = Messages_off_direction.flatMap{case (dstid,list)=> list.map(e1=>list.map(e2=>(e1,e2))).flatten.filter(e=> e._1 != e._2)}
      .distinct.join(graph.vertices)
      .map { case (src, (dst,vertexAttr)) =>
      val edgeAttr = EdgeAttr()
      edgeAttr.is_IL = true
      edgeAttr.community_id=vertexAttr.community_id
      Edge(src, dst, edgeAttr)
    }
    Graph(graph.vertices, graph.edges.union(NewEdges))
  }
}
