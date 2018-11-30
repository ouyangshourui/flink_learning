package Bean

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import product.TrackRealTimeAcquisitionHackAttack.format

import scala.beans.BeanProperty

case class RealTimeTrackerBean(
                                @BeanProperty servertime: String = "",
                                @BeanProperty fronttime:Long=0L,
                                @BeanProperty ip: String = "",
                                @BeanProperty guid: String = "",
                                @BeanProperty userid: String = "",
                                @BeanProperty platform: String = "",
                                @BeanProperty biztype: String = "",
                                @BeanProperty logtype: String = "",
                                @BeanProperty referurl: String = "",
                                @BeanProperty curpageurl: String = "",
                                @BeanProperty pagelevelid: String = "",
                                @BeanProperty viewid: String = "",
                                @BeanProperty viewparam: String = "",
                                @BeanProperty clickid: String = "",
                                @BeanProperty clickparam: String = "",
                                @BeanProperty os: String = "",
                                @BeanProperty display: String = "",
                                @BeanProperty downchann: String = "",
                                @BeanProperty appversion: String = "",
                                @BeanProperty devicetype: String = "",
                                @BeanProperty nettype: String = "",
                                @BeanProperty coordinate: String = "",
                                @BeanProperty hserecomkey: String = "",
                                @BeanProperty hseextend: String = "",
                                @BeanProperty hseepread: String = "",
                                @BeanProperty searchengine: String = "",
                                @BeanProperty keyword: String = "",
                                @BeanProperty chansource: String = "",
                                @BeanProperty search: String = "",
                                @BeanProperty platformid: String = "",
                                @BeanProperty appid: String = ""
                              ){
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def parse(text: String ): RealTimeTrackerBean ={
    val json = JSON.parseObject(text)
    val appid = json.getString("appid")
    val platformid = json.getString("platformid")
    val search = json.getString("search")
    val coordinate = json.getString("coordinate")
    val devicetype = json.getString("devicetype")
    val platform = json.getString("platform")
    val userid = json.getString("userid")
    val ip = json.getString("ip")
    val servertime = json.getString("servertime")
    val fronttime:Long = try {
      json.getLong("fronttime")
    } catch {
      case _ => 0L
    }
    val guid = json.getString("guid")

    new RealTimeTrackerBean(
      appid=appid,
      platformid=platformid,
      search=search,
      coordinate=coordinate,
      devicetype=devicetype,
      platform=platform,
      userid=userid,
      ip=ip,
      servertime=servertime,
      fronttime=fronttime,
      guid=guid
    )

  }

}

