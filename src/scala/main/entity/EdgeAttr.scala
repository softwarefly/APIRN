package entity

/**
 * Created by Administrator on 2016/4/27.
 */
class EdgeAttr(var w_control:Double,
               var w_tz:Double,
               var w_trade: Double
              ) extends  Serializable {

  def toCsvString:String={
    var result = ""
    if(w_control!=0.0) result="控制"
    if(w_tz!=0.0) result="投资"
    if(w_trade!=0.0) result="交易"
    if(is_IL!=false) result="互锁"
    result
  }
  var community_id: Long = 0L
  var trade_je: Double = 0.0
  var tz_je: Double = 0.0
  var se: Double = 0.0
  var is_IL:Boolean=false
  lazy val w_IL=if(is_IL) 1.0 else 0.0
  var taxrate:Double =0.0
  //  控制关系边、投资关系边和交易关系边，金额，税额

  def isAntecedent(): Boolean = {
    if(this.is_IL)  return false
    val result = (this.w_trade == 0.0 || this.w_control != 0.0 || this.w_tz != 0.0)
    result
  }

  def isAntecedent(weight:Double): Boolean = {
    if(this.is_IL)  return false
//    val result = (this.w_trade == 0.0 && this.w_control >= weight) || (this.w_trade == 0.0 && this.w_tz >= weight)
    //annotation of david:严格的，当weight取值为0.0时，允许等号将导致交易边被 count in
    val result= (this.w_control > weight || this.w_tz > weight)
    result
  }

  def isImportantAntecedent(): Boolean = {
    if(this.w_control!=0.0) return true
    if(this.w_tz>0.2) return true
    return false
  }

  def isTrade(): Boolean = {
    this.w_trade != 0.0
  }

  def isTZ(): Boolean = {
    this.w_tz != 0.0
  }

  override def toString = s"EdgeAttr(控制:$w_control, 投资:$w_tz, 交易:$w_trade, 互锁:$w_IL)"
}

object EdgeAttr {
  def fromString(s: String): EdgeAttr = {
      s match{
        case "交易" => EdgeAttr(w_trade = 1.0);
        case "控制" => EdgeAttr(w_control = 1.0);
        case "投资" => EdgeAttr(w_tz = 1.0);
      }
  }

  def combine(a:EdgeAttr, b:EdgeAttr) = {
    val toReturn = new EdgeAttr(a.w_control + b.w_control, a.w_tz + b.w_tz, a.w_trade + b.w_trade)
    toReturn.trade_je=a.trade_je+b.trade_je
    toReturn.tz_je=a.tz_je+b.tz_je
    toReturn.se=a.se+b.se
    toReturn
  }

  def apply(w_control:Double=0.0, w_tz:Double=0.0, w_trade: Double=0.0) = {
    val  lw_control=if(w_control>1.0) 1.0 else w_control
    val  lw_tz=if(w_tz>1.0) 1.0 else w_tz
    val  lw_trade=if(w_trade>1.0) 1.0 else w_trade
    new EdgeAttr(lw_control, lw_tz,lw_trade)
  }
}