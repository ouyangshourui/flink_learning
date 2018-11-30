package product
import java.sql.Timestamp

import Bean.RealTimeTrackerBean
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.{SerializationSchema, TypeInformationSerializationSchema}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import product.TrackRealTimeAcquisitionDemo.{init, properties}




object TrackRealTimeAcquisitionHackAttack_proctime {

  import java.text.SimpleDateFormat

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    init()
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val stream = env.addSource(new FlinkKafkaConsumer010[String]("track-realTime-acquisition", new SimpleStringSchema(), properties))

    val structstream = stream.map{recode=>
     val rtt=new  RealTimeTrackerBean()
      rtt.parse(recode)
    }

    tEnv.registerDataStream("trackerTable",structstream)

//    //微商城 1-微商城，2-安卓，3-ios，4-pc，5-wap
//    val weixinshop=tEnv.sqlQuery("select ip,nettype,devicetype,coordinate,platform ,servertime from" +
//      " trackerTable where platform='03' ")
//    //val weixinshop=tEnv.sqlQuery("select ip,nettype,devicetype,coordinate from trackerTable where platform=1 ")
//    weixinshop.toAppendStream[wxs].print()


   //计算一分钟，访问最多的IP
    val wxs1Stream= tEnv.sqlQuery("select ip,nettype,devicetype,coordinate,platform ,servertime from" +
      " trackerTable ").toAppendStream[wxs].map{ wxs=>
      val servertimet= new java.sql.Timestamp(format.parse(wxs.servertime).getTime)

      new wxs1(wxs.ip,wxs.nettype,wxs.devicetype,wxs.coordinate,wxs.platform,servertimet)
    }


    //proctime.proctime ,将proctime将proctime类型

    tEnv.registerDataStream("tablestream_table",wxs1Stream,'ip,'nettype,'devicetype,'coordinate,'platform,'servertime,'proctime.proctime)
    val maxIpStream1min=tEnv.sqlQuery("SELECT ip,count(ip) FROM " +
      "tablestream_table GROUP BY TUMBLE(proctime, INTERVAL '60' SECOND), ip")


    maxIpStream1min.toAppendStream[maxip].print()

   env.execute("TrackRealTimeAcquisitionHackAttack")

  }


  case class maxip(ip:String,cip:Long)
  case class wxs(ip:String,nettype:String,devicetype:String,coordinate:String,platform:String,servertime:String)

  case class wxs1(ip:String,nettype:String,devicetype:String,coordinate:String,platform:String,servertime:Timestamp)

}
