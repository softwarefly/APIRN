package entity

import scala.collection.mutable.ArrayBuffer

/**
 * Created by david on 3/29/16.
 */
class Community (val cid:Long,var consistNodes:ArrayBuffer[Long]) extends Serializable{

  var total_je:Double=0.0
  var total_se:Double=0.0
  var CentralNode:Long=0L
  var num_edge:Long=0L
  var num_vertex = consistNodes.size.toLong
  var num_pattern:Long=0L

  def canEqual(other: Any): Boolean = other.isInstanceOf[Community]

  override def equals(other: Any): Boolean = other match {
    case that: Community =>
      (that canEqual this) &&
        cid == that.cid
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(cid)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Community{
  def apply(cid:Long,consistNodes:ArrayBuffer[Long]) = {
    new Community(cid,consistNodes)

  }
}

