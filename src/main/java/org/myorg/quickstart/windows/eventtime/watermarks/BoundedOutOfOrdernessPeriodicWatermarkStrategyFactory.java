package org.myorg.quickstart.windows.eventtime.watermarks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.myorg.quickstart.windows.eventtime.Event;

import java.time.Duration;

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
 * A <b>periodic</b> generator usually observes to the incoming events via onEvent() and
 * then emits a watermark when the framework calls onPeriodicEmit().
 * <br>
 * So it generates watermarks periodically.
 * <br>
 * The interval (every n milliseconds) in which the watermark will be generated is defined
 * via ExecutionConfig.setAutoWatermarkInterval(...).
 * <br>
 * The generators’ onPeriodicEmit() method will be called each time, and a new watermark
 * will be emitted if the returned watermark is non-null and larger than the previous watermark.
 * <br><br>
 * <b>With this strategy, we consider events that arrive out of order for a fixed amount of time.
 * The strategy will generate a watermark each x processing time milliseconds.</b>
 */
public class BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory implements WatermarkStrategyFactory {

    //region Singleton code
    private static BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory instance;

    private BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory() {
    }

    public static BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory getInstance() {
        // Lazy initialization
        if (instance == null)
            instance = new BoundedOutOfOrdernessPeriodicWatermarkStrategyFactory();

        return instance;
    }
    //endregion

    /**
     * @return {@link WatermarkStrategy} for {@link Event}, for situations where records are <b>out of order</b>,
     * with an <b>upper bound of 3 milliseconds</b> on how far the events are out of order. The watermark generation
     * is <b>periodic: a watermark is generated every n processing time milliseconds (defined via
     * {@link ExecutionConfig#setAutoWatermarkInterval(long)}</b>.
     */
    @Override
    public WatermarkStrategy<Event> getWatermarkStrategy() {
        return WatermarkStrategy
                // The watermark lags behind the max timestamp seen in the stream by a fixed amount of time.
                // In this case, the max amount of time an element is allowed to be late before being ignored
                // when computing the final result for the given window is 3 milliseconds.
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(3)) // specify WatermarkGenerator
                // Event timestamp is picked from the Event POJO timestamp field
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()); // specify TimestampAssigner
    }

}
