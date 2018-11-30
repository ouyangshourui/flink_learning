package product

import Bean.RealTimeTrackerBean
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class BoundedGenerator extends AssignerWithPeriodicWatermarks[(Long,String,String,String,String,String,String,String,String,Long,String)] {

  val maxOutOfOrderness = 3500L // 3.5 seconds

  var currentMaxTimestamp: Long = _

  override def extractTimestamp(element:(Long,String,String,String,String,String,String,String,String,Long,String), previousElementTimestamp: Long): Long = {
    val timestamp = element._10
    timestamp;
  }

  override def getCurrentWatermark(): Watermark = {
    // return the watermark as current highest timestamp minus the out-of-orderness bound
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}
