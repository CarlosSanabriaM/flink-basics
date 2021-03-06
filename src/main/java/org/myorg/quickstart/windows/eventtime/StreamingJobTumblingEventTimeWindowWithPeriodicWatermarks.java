package org.myorg.quickstart.windows.eventtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;
import org.myorg.quickstart.windows.eventtime.watermarks.BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory;


/**
 * <b>Windows useful documentation:</b>
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html</li>
 * <li>https://flink.apache.org/news/2015/12/04/Introducing-windows.html</li>
 * <br>
 * <b>Watermarks useful documentation</b>
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html</li>
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamp_extractors.html</li>
 */
public class StreamingJobTumblingEventTimeWindowWithPeriodicWatermarks {

    public static void main(String[] args) throws Exception {
        // IMPORTANT NOTE: The execution of this example IS NOT deterministic,
        //  because watermarks generation depends on processing time (due to the usage of a periodic watermark generator)

        //region Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);
        //endregion

        //region Define the AutoWatermarkInterval
        // See the "Define the WatermarkStrategy" region to see what this line means
        final long autoWatermarkInterval =
                // * With 10 seconds, watermarks will be updated only once,
                //   and the value of the watermark will be Long.MAX_VALUE.
                // * With 1 millisecond, watermarks will probably be updated more than once.
                // Check "current watermark" value in the windows results after executing the program to verify this.
                //
                // Uncomment one of the following 2 lines
//                10000L; // poll watermark every 10 seconds
                1L; // poll watermark every millisecond

        env.getConfig().setAutoWatermarkInterval(autoWatermarkInterval);
        //endregion

        //region Create a DataStream of events
        DataStream<Event> dataStream = env.fromElements(
                // a
                Event.builder().key("a").value(1).timestamp(2L).build(),
                Event.builder().key("a").value(2).timestamp(1L).build(),
                Event.builder().key("a").value(3).timestamp(3L).build(),
                Event.builder().key("a").value(4).timestamp(9L).build(),
                Event.builder().key("a").value(5).timestamp(4L).build(),
                Event.builder().key("a").value(6).timestamp(5L).build(),
                Event.builder().key("a").value(7).timestamp(6L).build(),
                Event.builder().key("a").value(8).timestamp(7L).build(),
                Event.builder().key("a").value(9).timestamp(14L).build(),
                Event.builder().key("a").value(10).timestamp(10L).build(),
                Event.builder().key("a").value(11).timestamp(9L).build()
        );
        //endregion

        //region Define the WatermarkStrategy
        WatermarkStrategy<Event> strategy = BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory.getInstance()
                .getWatermarkStrategy();
        //endregion

        //region Assign Timestamps and Watermarks to the events in the DataStream
        DataStream<Event> dataStreamWithTimestampsAndWatermarks =
                dataStream.assignTimestampsAndWatermarks(strategy);
        //endregion

        //region Define tag for late events
        final OutputTag<Event> lateTag = new OutputTag<Event>("late") {
        };
        //endregion

        //region Group events with the same key into windows of 5 milliseconds
        WindowedStream<Event, String, TimeWindow> windowedDataStream = dataStreamWithTimestampsAndWatermarks
                .keyBy(Event::getKey) // key by the Event POJO key field
                // Tumbling event time window of 5 milliseconds.
                // timeWindow() creates processing or event time windows based on
                // the time characteristic specified in the environment
                // (in this case, EventTime is specified in the environment).
                .timeWindow(Time.milliseconds(5))
                // OPTIONAL: Collect the late events in a side output DataStream
                .sideOutputLateData(lateTag);
        //endregion

        //region Use a process window function to create an String with the info and the values of each window
        // In this case, the ProcessWindowFunction is defined as a normal class
        SingleOutputStreamOperator<String> resultDataStream = windowedDataStream
                .process(new GetWindowMetadataProcessWindowFunction());
        resultDataStream
                .print("process");

        // Obtain and print the DataStream with the late events
        resultDataStream
                .getSideOutput(lateTag)
                .print("lateEvents");
        //endregion

        //region Execute program
        env.execute("Flink Streaming Job (TumblingEventTimeWindowWithPeriodicWatermarks example)");
        //endregion
    }

}
