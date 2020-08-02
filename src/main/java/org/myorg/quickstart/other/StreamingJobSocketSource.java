package org.myorg.quickstart.other;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


public class StreamingJobSocketSource {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        // In this case, the local environment with web ui is used,
        // because this job receives an unbounded stream input, unlike the other examples, where the stream was finite.
        // For that reason, the web ui can be used, while the job is in execution, in http://localhost:8081.
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.LOCAL_WITH_WEB_UI);

        // Create a DataStream from a socket
        // In the CLI, create the socket with: nc -lk 9999
        DataStream<String> dataStream = env
                .socketTextStream("localhost", 9999);

        // Print the DataStream
        dataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (SocketSource example)");
    }

}
