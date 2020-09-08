package org.myorg.quickstart.windows.eventtime;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GetWindowMetadataProcessWindowFunction extends ProcessWindowFunction<Event, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) {
        // This method gets an Iterable containing all the elements of the window,
        // and a Context object with access to time and state information
        // This comes at the cost of performance and resource consumption,
        // because elements cannot be incrementally aggregated but instead need to be
        // buffered internally until the window is considered ready for processing.

        // The key parameter is the key that is extracted via
        // the KeySelector that was specified for the keyBy() invocation.

        // Transform Iterable<Event> to List<Integer> with the values of the events
        List<Integer> elementValues = StreamSupport
                .stream(elements.spliterator(), false)
                .map(Event::getValue)
                .collect(Collectors.toList());

        // Transform Iterable<Event> to List<Integer> with the timestamps of the events
        List<Long> elementTimestamps = StreamSupport
                .stream(elements.spliterator(), false)
                .map(Event::getTimestamp)
                .collect(Collectors.toList());

        out.collect(String.format("key: %s.  " +
                        "window type: %s.  " +
                        "window start: %d.  " +
                        "window max timestamp: %d.  " +
                        "window end: %d.  " +
                        "current watermark: %d.  " +
                        "elements timestamps in the window: %s.  " +
                        "elements values in the window: %s.",
                key,
                context.window(),
                context.window().getStart(), // first timestamp that belongs to this window
                context.window().maxTimestamp(), // largest timestamp that still belongs to this window
                context.window().getEnd(), // first timestamp that does not belong to this window any more
                context.currentWatermark(),
                elementTimestamps,
                elementValues));
    }

}
