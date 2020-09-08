package org.myorg.quickstart.windows.count;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


/**
 * Windows useful documentation:
 * <li>https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html</li>
 * <li>https://flink.apache.org/news/2015/12/04/Introducing-windows.html</li>
 */
public class StreamingJobTumblingCountWindow {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some tuples
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                // a
                Tuple2.of("a", 1), // begin of window a.1
                Tuple2.of("a", 2),
                Tuple2.of("a", 3), // end of window a.1
                Tuple2.of("a", 4), // begin of window a.2
                Tuple2.of("a", 5),
                Tuple2.of("a", 6), // end of window a.2
                Tuple2.of("a", 7), // begin of window a.3 (this window is not triggered)
                // b
                Tuple2.of("b", 8), // begin of window b.1
                Tuple2.of("b", 9),
                Tuple2.of("b", 10), // end of window b.1
                Tuple2.of("b", 11), // begin of window b.2 (this window is not triggered)
                // c
                Tuple2.of("c", 12) // With this key, none window is triggered, because there is only 1 event whose key is c
        );

        // Apply Flink operators
        // Events with the same key are grouped into windows of 3 elements
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowedDataStream = dataStream
                .keyBy(tuple -> tuple.f0)
                // tumbling count window of 3 elements size
                .countWindow(3);

        //region Sum the values of each window
        windowedDataStream
                .sum(1)
                .print("sum");
        //endregion

        //region Use a reduce function tu multiply the values of each window
        // A ReduceFunction specifies how two elements from the input are combined to produce an output element of the same type.
        // Flink uses a ReduceFunction to incrementally aggregate the elements of a window.

        // In this case, the ReduceFunction is defined as a lambda function
        windowedDataStream
                // reduce: input values, accumulator and output values are all of the same type
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 * value2.f1))
                .print("reduce");
        //endregion

        //region Use an aggregate function tu calculate the average of the values of each window
        // An AggregateFunction is a generalized version of a ReduceFunction that has three types:
        // an input type (IN), accumulator type (ACC), and an output type (OUT).
        // Same as with ReduceFunction, Flink will incrementally aggregate input elements of a window as they arrive.

        // In this case, the AggregateFunction is defined as an anonymous class
        windowedDataStream
                // aggregate: input values, accumulator and output values can differ in type
                // In this case, the flink AverageAccumulator class is used to simplify the implementation
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, AverageAccumulator, Double>() {
                    @Override
                    public AverageAccumulator createAccumulator() {
                        // This method creates the accumulator
                        return new AverageAccumulator();
                    }

                    @Override
                    public AverageAccumulator add(Tuple2<String, Integer> value, AverageAccumulator accumulator) {
                        // This method receives the current event, updates the accumulator and returns the accumulator
                        accumulator.add(value.f1);
                        return accumulator;
                    }

                    @Override
                    public Double getResult(AverageAccumulator accumulator) {
                        // This method gets the result of the aggregation from the accumulator
                        return accumulator.getLocalValue();
                    }

                    @Override
                    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
                        // This method merges two accumulators, returning an accumulator with the merged state.
                        // The assumption is that the given accumulators will not be used any more after having been passed to this function.
                        System.out.println("2 accumulators are going to be merged -> " +
                                "accumulator a: " + a + " accumulator b: " + b);

                        a.merge(b);
                        return a;
                    }
                })
                .print("aggregate");
        //endregion

        //region Use a process window function to create an String with the info and the values of each window
        // In this case, the ProcessWindowFunction is defined as an anonymous class
        windowedDataStream
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        // This method gets an Iterable containing all the elements of the window,
                        // and a Context object with access to time and state information
                        // This comes at the cost of performance and resource consumption,
                        // because elements cannot be incrementally aggregated but instead need to be
                        // buffered internally until the window is considered ready for processing.

                        //region Access the per-key global state
                        // This state is shared between all the windows created for each key
                        ValueState<String> perKeyGlobalState = context
                                .globalState()
                                .getState(new ValueStateDescriptor<>("globalValueState", String.class));
                        String perKeyGlobalStateValue = perKeyGlobalState.value();

                        // Value states values are null the first time they are accessed
                        if (perKeyGlobalStateValue == null)
                            perKeyGlobalStateValue = "";

                        for (Tuple2<String, Integer> element : elements)
                            // String concatenation in loop shouldn't be used
                            // https://help.semmle.com/wiki/display/JAVA/String+concatenation+in+loop
                            perKeyGlobalStateValue += element.f1;

                        perKeyGlobalState.update(perKeyGlobalStateValue);
                        //endregion

                        // The key parameter is the key that is extracted via
                        // the KeySelector that was specified for the keyBy() invocation.

                        out.collect(String.format("key: %s.  " +
                                        "window type: %s.  " +
                                        "elements in the window: %s.  " +
                                        "current processing time: %s.  " +
                                        "per-key global state value: %s",
                                key,
                                context.window(),
                                elements,
                                context.currentProcessingTime(),
                                perKeyGlobalStateValue));
                    }
                })
                .print("process");
        //endregion

        //region Use a process window function with incremental aggregation to return the smallest event in a window along with window info
        // Using ProcessWindowFunction for simple aggregates such as count is quite inefficient.
        // This section shows how a ReduceFunction or AggregateFunction can be combined with a ProcessWindowFunction
        // to get both incremental aggregation and the added information of a ProcessWindowFunction.

        // In this case, the ReduceFunction is defined as a lambda function,
        // and the ProcessWindowFunction is defined as an anonymous class,
        windowedDataStream
                .reduce(
                        // ReduceFunction
                        // Incrementally aggregate elements as they arrive in the window.
                        // This function returns the smallest event in the window
                        (value1, value2) -> value1.f1 < value2.f1 ? value1 : value2,
                        // ProcessWindowFunction
                        // When the window is closed, the ProcessWindowFunction will be provided with the aggregated result.
                        // This allows it to incrementally compute windows while having access to the
                        // additional window meta information of the ProcessWindowFunction.
                        new ProcessWindowFunction<Tuple2<String, Integer>, String, String, GlobalWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) {
                                // This method gets an Iterable containing only one element:
                                // the aggregated value returned by the ReduceFunction when the window is closed
                                Tuple2<String, Integer> minEvent = elements.iterator().next();

                                // The key parameter is the key that is extracted via
                                // the KeySelector that was specified for the keyBy() invocation.

                                out.collect(String.format("key: %s.  " +
                                                "window type: %s.  " +
                                                "min event in the window: %s.  " +
                                                "current processing time: %s",
                                        key,
                                        context.window(),
                                        minEvent,
                                        context.currentProcessingTime()));
                            }
                        }
                )
                .print("reduceAndProcess");
        //endregion

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (TumblingCountWindow example)");
    }

}
