package com.haiziwang.streaming

import com.alibaba.fastjson.JSON
import com.haiziwang.platform.kmem.client.KMEMClient
import com.haiziwang.platform.kmem.client.api.IKMEMCache
import com.haiziwang.streaming.bean._
import com.haiziwang.streaming.util.{OracleConnector, utilTool}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object DeptOnlineUserBatch {

  val checkpointDirectory = "/apps/hive/warehouse/fdm/lkz/checkpoint" //checkpoint目录

  def main(args: Array[String]) {

    // 创建context
    val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

    // 启动流计算
    context.start()
    context.awaitTermination()
  }

  // 通过函数来创建或者从已有的checkpoint里面构建StreamingContext
  def functionToCreateContext(): StreamingContext = {

    val conf = new SparkConf().setAppName("DeptOnlineUserBatch").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"). // 设置序列化器为KryoSerializer
      registerKryoClasses(Array(classOf[TraceRealtime], classOf[UserBasic], classOf[StoreBasic], classOf[IKMEMCache], classOf[kryoRegConsumerRecord], classOf[ConsumerRecord[String, String]])). // 注册要序列化的自定义类型。
      set("spark.locality.wait", "8000"). // 数据本地化等待时长(毫秒)
      set("spark.default.parallelism", "8")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))

    //配置kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.172.210.169:9092,172.172.210.170:9092,172.172.210.171:9092,172.172.210.172:9092,172.172.210.173:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_track-realTime-acquisition_lkz50",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true" //当数据被消费完之后会，如果spark streaming的程序由于某种原因停止之后再启动，下次不会重复消费之前消费过的数据
    )

    val topics = collection.Iterable("track-realTime-acquisition")

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    dStream.checkpoint(Seconds(100)) //设置通过间隔时间，定时持久checkpoint到HDFS上

    KMEMClient.getInstance().registerCache("basicDataCache") //redis客户端注册

    //redis客户端获取
    val cache = KMEMClient.getInstance().getCache("basicDataCache")

    //广播变量
    val broadCast = sc.broadcast(cache)

    //RDD遍历
    dStream.foreachRDD(rdd => {

      rdd.repartition(8)

      //遍历分区内容
      rdd.foreachPartition(rdd_part => {
        try {

          val users: mutable.Map[String, User] = mutable.Map()

          //获取redis客户端
          val userCache = broadCast.value

          //单分区内容处理
          rdd_part.foreach(each => {

            val trackRecord = getMemAndCoordinate(each.value().toString)

            //获取当前流中uid
            val uid = trackRecord.getUserid()

            //如果uid不是会员,进入下次循环,否则开始处理(相当于continue)
            breakable {
              if (uid.length < 10 || "".equals(uid)) break
              else {
                val key = "CURT_" + utilTool.getDate("yyyyMMdd") + "_" + uid

                //获取当前会员的经纬度
                val coordinate = trackRecord.getCoordinate()

                //获取当前会员的在线时间
                val userTime = trackRecord.getFronttime()

                val lastTime = {
                  if (userCache.exists(key)) {
                    userCache.read(key)
                  } else {
                    "9999999999999"
                  }
                }

                //获取时间间隔
                val interval = utilTool.getInterval(lastTime, userTime)

                //判定uid是否需要处理,如果当天的会员已经存在,判定上次时间和本次时间的间隔,小于5分钟，不更新,大于5分钟在更新；如果不存在redis也会更新
                if (!userCache.exists(key) || (userCache.exists(key) && interval > 300000)) { //间隔大于5分钟,更新

                  //println("real new or recent user!")
                  val userBasic = userCache.readObject("userBasic_" + uid, classOf[UserBasic]) //获取会员基本信息
                  val user = new User()
                  user.setCoordinate(coordinate)
                  user.setFronttime(userTime)

                  if (null != userBasic) { //会员基本信息不为空
                    user.setUserid(userBasic.getUsrId)
                    user.setFmanager(userBasic.getFmanager)
                    user.setPhonenumber(userBasic.getPhonenumber)
                    user.setFmemberlevel(userBasic.getFmemberlevel)
                    user.setFuserlevel(userBasic.getFuserlevel)
                    user.setFphoto(userBasic.getFphoto)
                    user.setFsbabyage(userBasic.getFsbabyage)
                    user.setFcostscoreaccountpoints(userBasic.getFcostscoreaccountpoints)
                    user.setStoreid(userBasic.getStoreid)
                    user.setFnickname(userBasic.getFnickname)

                  } else {
                    //会员基本信息为空
                    user.setUserid(uid)
                    user.setStoreid("0000")
                    user.setFsbabyage("-99999")
                  }

                  users.put(uid, user)

                } else {
                  //println("exist user !")
                  //updateUserOnlineTime(key, userTime, userCache)
                  //println("当前时间="+userTime+"|上次时间="+userCache.read(key)+"间隔时间="+utilTool.getInterval(userCache.read(key),userTime))
                }
              }
            }

          })

          //批量更新oracle
          batchWriteUserToOracle(users, userCache)

        } catch {
          case e: Exception => println(e)
            println("分区循环处理异常 ！")
        }
      }
      )
    })

    //设置检查点,在HDFS上的checkpoint目录
    ssc.checkpoint(checkpointDirectory)

    //返回context
    ssc
  }

  /**
    * 批量更新数据库
    *
    * @param users
    * @param userCache
    */
  def batchWriteUserToOracle(users: mutable.Map[String, User], userCache: IKMEMCache): Unit = {

    val conn = OracleConnector.getConnection

    // 关闭自动提交事务
    conn.setAutoCommit(false)

    val insertSql = "INSERT INTO USER_REALTIME_DETAILS" +
      "(USERID, FMANAGER, PHONENUMBER, FMEMBERLEVEL, FPHOTO, FCOSTSCOREACCOUNTPOINTS, STOREID, TIME, ISORNOT, STATE, USERTYPER, FUSERLEVEL, DT, FNICKNAME)" +
      " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    val deleteSql = "DELETE USER_REALTIME_DETAILS WHERE DT = ? AND USERID = ?"

    val dt = utilTool.getDate("yyyy-MM-dd")

    try {
      val delPrep = conn.prepareStatement(deleteSql)

      val insPrep = conn.prepareStatement(insertSql)

      //取这批数据的最终状态
      //先删除
      users.keys.foreach { i =>
        delPrep.setString(1, dt)
        delPrep.setString(2, i)
        delPrep.addBatch()
      }

      delPrep.executeBatch()
      delPrep.close()

      //再插入
      users.values.foreach { i =>

        val key = "CURT_" + utilTool.getDate("yyyyMMdd") + "_" + i.getUserid

        //更新redis中会员的当前时间
        val curTime = updateUserOnlineTime(key, i.getFronttime, userCache)

        //计算会员是店内还是店外
        val isOut = isInOrOut(i, userCache, i.getCoordinate)
        //获取会员的类型
        val userType = getUserType(i.getFsbabyage)

        insPrep.setString(1, i.getUserid)
        insPrep.setString(2, i.getFmanager)
        insPrep.setString(3, i.getPhonenumber)
        insPrep.setString(4, i.getFmemberlevel)
        insPrep.setString(5, i.getFphoto)
        insPrep.setString(6, i.getFcostscoreaccountpoints)
        insPrep.setString(7, i.getStoreid)
        insPrep.setString(8, curTime)
        insPrep.setInt(9, isOut)
        insPrep.setInt(10, 1)
        insPrep.setString(11, userType)
        insPrep.setString(12, i.getFuserlevel)
        insPrep.setString(13, dt)
        insPrep.setString(14, i.getFnickname)

        insPrep.addBatch()
      }

      insPrep.executeBatch()
      insPrep.close()

      conn.commit()
    } catch {
      case e: Exception => println(e)
        println("批量更新数据库异常 ！")
        if (conn != null) {
          conn.rollback()
        }
    }
  }

  /**
    * 根据月龄设定会员类型
    *
    * @param fsbabyage
    * @return
    */
  def getUserType(fsbabyage: String): String = {
    var userType = "4"
    try {
      if (fsbabyage.trim.length <= 0) {
        userType = "4"
      } else {
        val age = fsbabyage.toInt
        if (age >= 0 && age <= 10) userType = "0"
        else if (age >= 11 && age <= 12) userType = "1"
        else if (age >= 13 && age <= 36) userType = "2"
        else if (age >= 36 && age <= 168) userType = "3"
        else userType = "4"
      }
    } catch {
      case e: Exception => userType = "4"
        println(e)
        println("获取会员类型处理异常！")
    }
    userType
  }

  /**
    * 判断会员在店内、店外
    *
    * @param user
    * @param userCache
    * @param userCoordinate
    * @return
    */
  def isInOrOut(user: User, userCache: IKMEMCache, userCoordinate: String): Int = {
    //0:店外 1 店内
    var isOut = 0
    //println("当前会员的经纬度的值userCoordinate="+userCoordinate+"|育儿顾问门店信息="+user.getStoreid)
    try {
      if (null == user.getStoreid || "".equals(user.getStoreid) || user.getStoreid.length == 0) {
        isOut = 0
      } else {
        val key = "storeBasic_" + user.getStoreid //门店key
        val store = userCache.readObject(key, classOf[StoreBasic]) //获取门店基本信息
        if (null != store) {
          val storeCoord = Array(store.getLatitude, store.getLongitude) //门店经纬度
          val userCoord = userCoordinate.split(",") //会员当前经纬度
          isOut = getStateFromLocation(storeCoord, userCoord)
        } else {
          isOut = 0
        }
      }
    } catch {
      case e: Exception => println(e)
        println("判定是否在店内程序异常!")
    }
    isOut
  }

  /**
    * 根据门店经纬度和会员当前位置计算会员是在店内还是店外
    *
    * @param storeCoord
    * @param userCoord
    * @return
    */
  def getStateFromLocation(storeCoord: Array[String], userCoord: Array[String]): Int = {
    var state = 0 //0:店外 1 店内
    try {
      var lat1 = 0d
      var lng1 = 0d
      var lat2 = 0d
      var lng2 = 0d
      if (storeCoord.length == 2) {
        lat1 = utilTool.strCovertDouble(storeCoord(0))
        lng1 = utilTool.strCovertDouble(storeCoord(1))
      }
      if (userCoord.length == 2) {
        lat2 = utilTool.strCovertDouble(userCoord(0))
        lng2 = utilTool.strCovertDouble(userCoord(1))
      }

      val dis = utilTool.GetDistance(lat1, lng1, lat2, lng2)

      if (dis > 0d && dis < 5d) {
        state = 1
      }
    } catch {
      case e: Exception => println(e)
        println("经纬度计算距离失败！")
    } finally {
      state = 0
    }
    state
  }

  /**
    * 更新当前会员的在线时间
    *
    * @param key
    * @param curt_time
    * @param userCache
    * @return
    */
  def updateUserOnlineTime(key: String, curt_time: String, userCache: IKMEMCache): String = {

    var curTime = curt_time
    try {

      val last_time = userCache.read(key) //获取redis保存的时间

      //如果不存在redis表示是新会员或者已经时间过期
      if (null == last_time || "".equals(last_time)) {
        userCache.write(key, curt_time) //保存到redis中
        userCache.expire(key, 86400) //有效期半小时
        curTime = curt_time
      } else {
        //如果存在redis;判定两个时间差值
        val interval = utilTool.getInterval(last_time, curt_time) //两个时间间隔
        //当前时间大于等于上次时间
        if (interval >= 0) {
          userCache.write(key, curt_time) //更新redis中的数据
          userCache.expire(key, 86400) //有效期半小时
          curTime = curt_time
        } else {
          curTime = last_time
        }
      }
    } catch {
      case e: Exception => println(e)
        println("更新会员的当前在线时间异常！")
    }
    curTime
  }

  /**
    * 过滤每一行记录,返回执行的字段值
    *
    * @param row
    * @return
    */
  def getMemAndCoordinate(row: String): TraceRealtime = {

    var trackObj = new TraceRealtime()
    val obj = JSON.parseObject(row, classOf[TraceRealtime])
    val userId = obj.getUserid()
    if (userId.length >= 10) {
      obj.setCoordinate(filterCoordinate(obj.getCoordinate()))
      trackObj = obj
    }
    trackObj
  }

  /**
    * 过滤坐标
    *
    * @param coordinate
    * @return
    */
  def filterCoordinate(coordinate: String): String = {
    var resCoordinate = ""
    try {
      val cdAry: Array[String] = coordinate.split(",")

      val pattern = """\d""".r

      if (cdAry.length == 2 && pattern.findAllIn(cdAry(0)).nonEmpty && pattern.findAllIn(cdAry(1)).nonEmpty) resCoordinate = coordinate

    } catch {
      case ex: Exception => println("经纬度长度有问题：" + coordinate + "==>" + ex)
    }
    resCoordinate
  }

}
