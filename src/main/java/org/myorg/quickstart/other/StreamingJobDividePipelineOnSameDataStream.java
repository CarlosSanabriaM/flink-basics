package org.myorg.quickstart.other;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


/**
 * This example shows how the same DataStream can be used in different pipelines
 */
public class StreamingJobDividePipelineOnSameDataStream {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some numbers
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Apply a first operation to the DataStream to check that this operation is executed once
        dataStream = dataStream.filter(ev -> {
            System.out.println("Processed event: " + ev);
            return ev % 2 == 0;
        });

        // Apply a map that multiplies by 2 (on the dataStream object)
        DataStream<Integer> resultDataStreamReference1 = dataStream
                .map(ev -> ev * 2);
        // Apply a map that multiplies by 100 (on the dataStream object)
        DataStream<Integer> resultDataStreamReference2 = dataStream
                .map(ev -> ev * 100);

        // Print the DataStreams
        // The string in the print is a prefix to identify to which DataSteam corresponds the printed event
        resultDataStreamReference1.print("resultDataStreamReference1");
        resultDataStreamReference2.print("resultDataStreamReference2");

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (DividePipelineOnSameDataStream example)");
    }

}
