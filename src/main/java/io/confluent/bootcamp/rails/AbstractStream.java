package io.confluent.bootcamp.rails;

import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import com.googlecode.lanterna.terminal.ansi.UnixLikeTerminal;
import io.confluent.bootcamp.rails.common.AbstractBase;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Callable;

public abstract class AbstractStream extends AbstractBase implements Callable<Integer> {
    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    protected boolean verbose = false;

    @CommandLine.Option(names = {"--scale"},
            description = "If greater than 1, Stream app will increase threads to the number provided")
    protected int scale = 1;

    @CommandLine.Option(names = {"--state-dir"},
            description = "If set, use as the state.dir parameter for this stream")
    protected String stateDir;

    protected Terminal terminal;
    protected Screen screen;
    protected TextGraphics tg;

    public AbstractStream() {
    }

    @Override
    protected void createProperties() {
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        readConfigFile(properties);

        if (bootstrapServers != null) {
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        if (schemaRegistryURL != null) {
            properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        }

        if (stateDir != null) {
            properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        }

        // Specify default (de)serializers for record keys and for record values.
        // properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        // properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        // properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        addConsumerProperties(properties);
    }

    @Override
    public Integer call() throws IOException {
        DefaultTerminalFactory factory = new DefaultTerminalFactory();
        factory.setUnixTerminalCtrlCBehaviour(UnixLikeTerminal.CtrlCBehaviour.CTRL_C_KILLS_APPLICATION);

        terminal = factory.createHeadlessTerminal();
        screen = new TerminalScreen(terminal);
        screen.startScreen();
        tg = screen.newTextGraphics();

        StreamsBuilder builder = new StreamsBuilder();

        createProperties();

        createTopology(builder);

        //noinspection resource
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.setStateListener((newState, oldState) -> {
            System.out.println("*** Changed state from " + oldState + " to " + newState);
            if (newState == KafkaStreams.State.NOT_RUNNING &&
                    oldState == KafkaStreams.State.PENDING_SHUTDOWN) {
                // Streams fully stopped (not just rebalancing)
                System.out.println("Kafka Streams stopped. Reporting results...");

                reportResults();
            }
        });
        streams.start();

        if (scale > 1) {
            for (var threads = 1; threads < scale; threads++) {
                logger.info("Increased thread count to {}", threads);
                streams.addStreamThread();
            }
        }

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("🛑 Shutdown hook running...");
            streams.close(Duration.ofSeconds(10));
            System.out.println("✅ Clean shutdown complete");
        }));


        return 0;
    }

    abstract protected void createTopology(StreamsBuilder builder) throws IOException;

    protected abstract void addConsumerProperties(Properties properties);

    protected abstract String getApplicationName();

    protected void reportResults() { }
}
