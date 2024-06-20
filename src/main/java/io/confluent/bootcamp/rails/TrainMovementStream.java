package io.confluent.bootcamp.rails;

import io.confluent.bootcamp.rails.common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import io.confluent.bootcamp.rails.schema.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.Properties;

@CommandLine.Command(name = "TrainMovementStream",
        version = "TrainMovementStream 1.0",
        description = "Reads Train_Movements objects in Avro format from a stream.")
public class TrainMovementStream extends AbstractStream {
    protected static Logger logger = LoggerFactory.getLogger(TrainMovementStream.class.getName());
    private final String TRAIN_MOVEMENTS_TOPIC = "TRAIN_MOVEMENTS";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})",
            defaultValue = TRAIN_MOVEMENTS_TOPIC)
    private String trainMovementsTopic;

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<String, TrainMovements> trainMovements =
                builder.stream(trainMovementsTopic, Consumed.with(Serdes.String(), SerdeGenerator.getSerde(properties)));

        if (verbose) {
            trainMovements.foreach((key, value) -> System.out.println(key + " => " + value));
        }
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        // Serdes are defined explicitly
    }

    @Override
    protected String getApplicationName() {
        return "train-movements-stream";
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new TrainMovementStream()).execute(args);
        } catch (Exception e) {
            logger.error("Something went wrong", e);
            System.exit(1);
        }
    }
}
