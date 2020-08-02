package org.myorg.quickstart.map;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


public class StreamingJobRichCoFlatMap {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create 2 DataStreams with tuples
        DataStream<Tuple2<String, Integer>> dataStream1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("b", 2),
                        Tuple2.of("c", 3)
                );
        DataStream<Tuple2<String, String>> dataStream2 = env
                .fromElements(
                        Tuple2.of("b", "y"),
                        Tuple2.of("c", "z"),
                        Tuple2.of("a", "x")
                );

        // KeyBy both streams by the attribute that is going to be used to join them
        KeyedStream<Tuple2<String, Integer>, String> keyedDataStream1 = dataStream1
                .keyBy(event -> event.f0);
        KeyedStream<Tuple2<String, String>, String> keyedDataStream2 = dataStream2
                .keyBy(event -> event.f0);

        // Apply Flink operators
        DataStream<Tuple2<String, String>> resultDataStream = keyedDataStream1
                .connect(keyedDataStream2)
                .flatMap(new JoinEventsRichCoFlatMap());

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (RichCoFlatMap example)");
    }

    private static class JoinEventsRichCoFlatMap extends RichCoFlatMapFunction<
            Tuple2<String, Integer>, // IN type on the left side (map1)
            Tuple2<String, String>, // IN type on the right side (map2)
            Tuple2<String, String> // OUT type
            > {
        // keyed, managed state
        private ValueState<Tuple2<String, Integer>> leftState;
        private ValueState<Tuple2<String, String>> rightState;

        @Override
        public void open(Configuration config) {
            // Create / initialize the flink states
            leftState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>(
                            "left state", // state name
                            Types.TUPLE(Types.STRING, Types.STRING) // state type information
                    ));
            rightState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>(
                            "right state", // state name
                            Types.TUPLE(Types.STRING, Types.STRING) // state type information
                    ));
        }

        @Override
        public void flatMap1(Tuple2<String, Integer> leftValue, Collector<Tuple2<String, String>> out) throws Exception {
            Tuple2<String, String> rightValue = rightState.value(); // Obtain value stored in state for the other event
            if (rightValue != null) {
                // If there is value in state for the other event, clear that state and return a tuple with info of both
                rightState.clear();
                out.collect(Tuple2.of(leftValue.f0, leftValue.f1 + rightValue.f1));
            } else {
                // If not, don't return anything and store the current event in state
                leftState.update(leftValue);
            }
        }

        @Override
        public void flatMap2(Tuple2<String, String> rightValue, Collector<Tuple2<String, String>> out) throws Exception {
            Tuple2<String, Integer> leftValue = leftState.value(); // Obtain value stored in state for the other event
            if (leftValue != null) {
                // If there is value in state for the other event, clear that state and return a tuple with info of both
                leftState.clear();
                out.collect(Tuple2.of(leftValue.f0, leftValue.f1 + rightValue.f1));
            } else {
                // If not, don't return anything and store the current event in state
                rightState.update(rightValue);
            }
        }

    }

}
