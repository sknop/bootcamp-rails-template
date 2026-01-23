package io.confluent.bootcamp.rails;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.confluent.bootcamp.rails.common.SerdeGenerator;
import io.confluent.bootcamp.rails.networkrail.Header;
import io.confluent.bootcamp.rails.schema.NetworkRailMovement;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.Optional;
import java.util.Properties;

@CommandLine.Command(name = "NetworkRailTrainMVTStream",
        version = "NetworkRailTrainMVTStream 1.0",
        description = "Reads Network Rail MVT objects in Avro format from a stream.")
public class NetworkRailTrainMVTStream extends AbstractStream {
    protected static Logger logger = LoggerFactory.getLogger(NetworkRailTrainMVTStream.class.getName());
    private final String TRAIN_MOVEMENTS_TOPIC = "NETWORKRAIL_TRAIN_MVT";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})",
            defaultValue = TRAIN_MOVEMENTS_TOPIC)
    private String trainMovementsTopic;

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<String, NetworkRailMovement> trainMovements =
                builder.stream(trainMovementsTopic, Consumed.with(Serdes.String(), SerdeGenerator.getSerde(properties)));

        if (verbose) {
            ObjectMapper mapper = new ObjectMapper();
            trainMovements.foreach((key, value) -> {
                var text = value.getText();

                try {
                    JsonNode node = mapper.readTree(text);
                    var nodeType = node.getNodeType();
                    if (JsonNodeType.ARRAY == nodeType) {
                        for (var entry : node) {
                            var header = entry.get("header");
                            var body = entry.get("body");

                            Header h = Header.createHeader(
                                    getValue(header,"msg_type"),
                                    getValue(header,"msg_queue_timestamp"),
                                    getValue(header,"source_system_id"),
                                    getValue(header,"original_data_source")); // this can be "<NULL>"
                            String prefix = switch (h.msg_type()) {
                                case "0001" -> "ACTIVATION         :";
                                case "0002" -> "CANCELLATION       :";
                                case "0003" -> "MOVEMENT           :";
                                case "0004" -> "NOT USED           :";
                                case "0005" -> "REINSTATEMENT      :";
                                case "0006" -> "CHANGE OF ORIGIN   :";
                                case "0007" -> "CHANGE OF IDENTITY :";
                                case "0008" -> "CHANGE OF LOCATION :";
                                default ->  String.format("UNKNOWN %4s   :", h.msg_type());
                            };
                            System.out.printf("%s header = %s  body = %s\n", prefix, h, body);
                        }
                    } else {
                        throw new RuntimeException("Unexpected node type: " + nodeType);
                    }
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

            });
        }
    }

    private String getValue(JsonNode node, String keyword) {
        JsonNode subNode = node.get(keyword);
        return subNode != null ? subNode.textValue() : "<NULL>";
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        // Serdes are defined explicitly
    }

    @Override
    protected String getApplicationName() {
        return "networkrail-train-mvt-stream";
    }

    static void main(String[] args) {
        try {
            new CommandLine(new NetworkRailTrainMVTStream()).execute(args);
        } catch (Exception e) {
            logger.error("Something went wrong", e);
            System.exit(1);
        }
    }
}
