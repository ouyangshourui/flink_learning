package product
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010


object TrackRealTimeAcquisitionDemo {

  val properties = new Properties()

  def init():Unit={
    properties.setProperty("bootstrap.servers", "62.62.210.169:9092,62.62.210.170:9092,62.62.210.171:9092,62.62.210.62:9092,62.62.210.173:9092")
    properties.setProperty("group.id", "flink-scala-test")
    properties.setProperty("auto.offset.reset","latest")
  }

  def main(args: Array[String]): Unit = {
    init()
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val  stream = env.addSource(new FlinkKafkaConsumer010[String]("track-realTime-acquisition", new SimpleStringSchema(), properties))


    stream.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map(x=>"1s count")
      .map((_, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .sum(1).print()

    //风控：统计1分钟类访问最高的IP,
    stream.print()

    env.execute("TrackRealTimeAcquisition 1 seconds count")
  }
}
