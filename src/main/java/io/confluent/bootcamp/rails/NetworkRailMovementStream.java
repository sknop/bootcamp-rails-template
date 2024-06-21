package io.confluent.bootcamp.rails;

import io.confluent.bootcamp.rails.common.SerdeGenerator;
import io.confluent.bootcamp.rails.schema.NetworkRailMovement;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.Properties;

@CommandLine.Command(name = "NetworkRailMovementStream",
        version = "NetworkRailMovementStream 1.0",
        description = "Reads NetworkRailMovementStream objects in Avro format from a stream.")
public class NetworkRailMovementStream extends AbstractStream {
    protected static Logger logger = LoggerFactory.getLogger(NetworkRailMovementStream.class.getName());
    private final String DEFAULT_TOPIC = "NETWORKRAIL_TRAIN_MVT";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})",
            defaultValue = DEFAULT_TOPIC)
    private String topic;

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<String, NetworkRailMovement> stream =
                builder.stream(topic, Consumed.with(Serdes.String(), SerdeGenerator.getSerde(properties)));

        if (verbose) {
            stream.foreach((key, value) -> System.out.println(key + " => " + value));
        }
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        // Serdes are defined explicitly
    }

    @Override
    protected String getApplicationName() {
        return "network-rail-movements-stream";
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new NetworkRailMovementStream()).execute(args);
        } catch (Exception e) {
            logger.error("Something went wrong", e);
            System.exit(1);
        }
    }
}
