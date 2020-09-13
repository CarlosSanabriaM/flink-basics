package org.myorg.quickstart.windows.eventtime.watermarks;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 */
public interface WatermarkStrategyFactory {
    WatermarkStrategy<Event> getWatermarkStrategy();
}
