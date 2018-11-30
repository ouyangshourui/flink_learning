package example
import java.io.PrintWriter
import java.net.ServerSocket
import java.sql.{Time, Timestamp}
import java.util.Random

import Bean.RealTimeTrackerBean
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala._
import product.TrackRealTimeAcquisitionDemo.{init, properties}


object IoTSQLTester {

  private def listenAndGenerateNumbers(port: Int) = {
    try {
      val serverSocket = new ServerSocket(port)
      val clientSocket = serverSocket.accept
      System.out.println("Accepted connection")
      val random = new Random
      val out = new PrintWriter(clientSocket.getOutputStream, true)
      val rooms = Array[String]("living room", "kitchen", "outside", "bedroom", "attic")
      var i = 0
      while ( {
        i < 10000
      }) {
        val room = rooms(random.nextInt(rooms.length))
        val temp = random.nextDouble * 30 + 20
        out.println(room + "," + temp)
        Thread.sleep(random.nextInt(10) + 50)

        {
          i += 1; i - 1
        }
      }
      System.out.println("Closing server")
      clientSocket.close()
      serverSocket.close()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  private val mapFunction = new MapFunction[String, Tuple3[String, Double, Long]]() {
    @throws[Exception]
    override def map(s: String): Tuple3[String, Double, Long] = { // data is: <roomname>,<temperature>
      val p = s.split(",")
      val room = p(0)
      val temperature = p(1).toDouble
      val creationDate = System.currentTimeMillis
      new Tuple3[String, Double, Long](room, temperature, creationDate)
    }
  }

  private val extractor = new AscendingTimestampExtractor[Tuple3[String, Double, Long]]() {
    override def extractAscendingTimestamp(element: Tuple3[String, Double, Long]): Long = element._3
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.enableSysoutLogging()

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val port = 3901

    new Thread(new Runnable() {
      override def run(): Unit = {
        listenAndGenerateNumbers(port)
      }
    }).start()


    Thread.sleep(1000) // wait the socket for a little;


    val text = env.socketTextStream("localhost", port)
    val dataset = text.map{s=>
      val p = s.split(",")
      val room = p(0)
      val temperature = p(1).toDouble
      val creationDate = System.currentTimeMillis
      new Tuple3[String, Double, Long](room, temperature, creationDate)
    }.assignTimestampsAndWatermarks(extractor)


    // Register it so we can use it in SQL
    tableEnv.registerDataStream("sensors", dataset, 'room, 'temperature, 'creationDate.rowtime)



    //val query = "SELECT room, creationDate,temperature  FROM sensors "

    val query = "SELECT room, TUMBLE_END(creationDate, INTERVAL '10' SECOND), AVG(temperature) AS avgTemp FROM sensors GROUP BY TUMBLE(creationDate, INTERVAL '10' SECOND), room"
    //Table table = tableEnv.sql(query);
    val table = tableEnv.sqlQuery(query)

    // Just for printing purposes, in reality you would need something other than Row
    table.toAppendStream[tt].print()

    env.execute

  }
case class tt(r:String,t:Timestamp,tp:Double)


}
