package org.myorg.quickstart.windows.eventtime;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
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


/**
 * <b>Windows useful documentation:</b>
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html</li>
 * <li>https://flink.apache.org/news/2015/12/04/Introducing-windows.html</li>
 * <br>
 * <b>Watermarks useful documentation</b>
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html</li>
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamp_extractors.html</li>
 */
public class StreamingJobTumblingEventTimeWindowWithPunctuatedWatermarks {

    public static void main(String[] args) throws Exception {
        // IMPORTANT NOTE: The execution of this example IS deterministic,
        //  because watermarks generation doesn't depend on processing time,
        //  but in the timestamps of the events (due to the usage of a punctuated watermark generator)

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        //region Create a DataStream of events
        DataStream<Event> dataStream = env.fromElements(
                // a
                Event.builder().key("a").value(1).timestamp(2L).build(), // window 1
                Event.builder().key("a").value(2).timestamp(1L).build(), // window 1
                Event.builder().key("a").value(3).timestamp(3L).build(), // window 1
                Event.builder().key("a").value(4).timestamp(9L).build(), // window 2
                Event.builder().key("a").value(5).timestamp(4L).build(), // in this example, this event will always be late
                Event.builder().key("a").value(6).timestamp(5L).build(), // window 2
                Event.builder().key("a").value(7).timestamp(6L).build(), // window 2
                Event.builder().key("a").value(8).timestamp(7L).build(), // window 2
                Event.builder().key("a").value(9).timestamp(14L).build(), // window 3
                Event.builder().key("a").value(10).timestamp(8L).build() // in this example, this event will always be late
        );
        //endregion

        //region Define the WatermarkStrategy
        // A watermark for time Wt is an assertion that the stream is (probably) now complete up through time t.
        // Any event following this watermark whose timestamp is ≤ Wt is late.

        // A WatermarkStrategy is formed by a WatermarkGenerator and a TimestampAssigner.
        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamp_extractors.html#fixed-amount-of-lateness
        // https://ci.apache.org/projects/flink/flink-docs-stable/learn-flink/streaming_analytics.html#working-with-watermarks

        // There are two different styles of watermark generation: periodic and punctuated.
        // A punctuated watermark generator will observe the stream of events and emit a watermark
        // whenever it sees a special element that carries watermark information.
        //
        // Note: It is possible to generate a watermark on every single event.
        //  However, because each watermark causes some computation downstream,
        //  an excessive number of watermarks degrades performance.

        // With this strategy, we consider events that arrive out of order for a fixed amount of time.
        // The strategy will generate a watermark on every single event, for testing purposes.
        //
        // IMPORTANT: It will do the same as the forBoundedOutOfOrderness() in the
        // StreamingJobTumblingEventTimeWindowWithPeriodicWatermarks class, but in this case,
        // each event will force the generation of a watermark.
        WatermarkStrategy<Event> strategy = WatermarkStrategy
                // The watermark lags behind the max timestamp seen in the stream by a fixed amount of time.
                // In this case, the max amount of time an element is allowed to be late before being ignored
                // when computing the final result for the given window is 3 milliseconds.
                .forGenerator(context -> new WatermarkGenerator<Event>() { // specify custom WatermarkGenerator
                    // Implementation based on:
                    // https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html#writing-a-periodic-watermarkgenerator
                    // https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html#writing-a-punctuated-watermarkgenerator

                    private static final long MAX_OUT_OF_ORDERNESS = 3; // 3 milliseconds

                    private long currentMaxTimestamp;

                    @Override
                    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                        long lastMaxTimestamp = currentMaxTimestamp;
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);

                        if (currentMaxTimestamp != lastMaxTimestamp)
                            // emit the watermark as current highest timestamp minus the out-of-orderness bound
                            output.emitWatermark(new Watermark(currentMaxTimestamp - MAX_OUT_OF_ORDERNESS - 1));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // don't need to do anything because we emit in reaction to events above
                    }
                })
                // Event timestamp is picked from the Event POJO timestamp field
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()); // specify TimestampAssigner
        //endregion

        // Assign Timestamps and Watermarks to the events in the DataStream
        DataStream<Event> dataStreamWithTimestampsAndWatermarks =
                dataStream.assignTimestampsAndWatermarks(strategy);

        //Apply Flink operators

        // Tag for late events
        final OutputTag<Event> lateTag = new OutputTag<Event>("late") {
        };

        // Events with the same key are grouped into windows of 5 milliseconds
        WindowedStream<Event, String, TimeWindow> windowedDataStream = dataStreamWithTimestampsAndWatermarks
                .keyBy(Event::getKey) // key by the Event POJO key field
                // Tumbling event time window of 5 seconds.
                // timeWindow() creates processing or event time windows based on
                // the time characteristic specified in the environment
                // (in this case, EventTime is specified in the environment).
                .timeWindow(Time.milliseconds(5))
                // OPTIONAL: Collect the late events in a side output DataStream
                .sideOutputLateData(lateTag);

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

        // Execute program
        env.execute("Flink Streaming Job (TumblingEventTimeWindowWithPunctuatedWatermarks example)");
    }

}
