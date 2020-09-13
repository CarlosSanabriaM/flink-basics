package org.myorg.quickstart.windows.eventtime.watermarks;

import org.apache.flink.api.common.eventtime.*;
import org.myorg.quickstart.windows.eventtime.Event;

/**
 * A watermark for time Wt is an assertion that the stream is (probably) now complete up through time t.
 * Any event <b>following</b> this watermark whose timestamp is ≤ Wt is late.
 * <br><br>
 * A {@link WatermarkStrategy} is formed by a {@link WatermarkGenerator} and a {@link TimestampAssigner}.
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamp_extractors.html#fixed-amount-of-lateness</li>
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/learn-flink/streaming_analytics.html#working-with-watermarks</li>
 * <br>
 * There are two different styles of watermark generation: periodic and punctuated.
 * <br>
 * A <b>punctuated</b> watermark generator will observe the stream of events and emit a watermark
 * whenever it sees a special element that carries watermark information.
 * <br><br>
 * <b>Note</b>: It is possible to generate a watermark on every single event.
 * However, because each watermark causes some computation downstream,
 * an excessive number of watermarks degrades performance.
 * <br><br>
 * <b>With this strategy, we consider events that arrive out of order for a fixed amount of time.
 * The strategy will generate a watermark on every single event, for testing purposes.</b>
 * <br><br>
 * <b>IMPORTANT</b>: It will do the same as the forBoundedOutOfOrderness() in
 * {@link BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory#getWatermarkStrategy()},
 * but in this case, each event will force the generation of a watermark.
 */
public class BoundedOutOfOrdernessPunctuatedWatermarkStrategyFactory implements WatermarkStrategyFactory {

    //region Singleton code
    private static BoundedOutOfOrdernessPunctuatedWatermarkStrategyFactory instance;

    private BoundedOutOfOrdernessPunctuatedWatermarkStrategyFactory() {
    }

    public static BoundedOutOfOrdernessPunctuatedWatermarkStrategyFactory getInstance() {
        // Lazy initialization
        if (instance == null)
            instance = new BoundedOutOfOrdernessPunctuatedWatermarkStrategyFactory();

        return instance;
    }
    //endregion

    /**
     * @return {@link WatermarkStrategy} for {@link Event}, for situations where records are <b>out of order</b>,
     * with an <b>upper bound of 3 milliseconds</b> on how far the events are out of order. The watermark generation
     * is <b>punctuated: each event generates a watermark</b> (if it's timestamp is greater than the highest timestamp processed).
     */
    @Override
    public WatermarkStrategy<Event> getWatermarkStrategy() {
        return WatermarkStrategy
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
    }
}