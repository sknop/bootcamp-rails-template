package io.confluent.bootcamp.rails.common;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import picocli.CommandLine;

import java.io.*;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

@CommandLine.Command(
        scope = CommandLine.ScopeType.INHERIT,
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false)
abstract public class AbstractBase {
    protected static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    protected static final String DEFAULT_SCHEMA_REGISTRY =  "http://localhost:8081";
    protected Logger logger = LoggerFactory.getLogger(AbstractBase.class.getName());

    protected Properties properties = new Properties();

    @CommandLine.Option(names = {"-c", "--config-file"},
            description = "If provided, content will be added to the properties")
    protected String configFile = null;
    @CommandLine.Option(names = {"--bootstrap-servers"})
    protected String bootstrapServers;
    @CommandLine.Option(names = {"--schema-registry"})
    protected String schemaRegistryURL;

    public void readConfigFile(Properties properties) {
        if (configFile != null) {
            logger.info("Reading config file {}", configFile);

            try (InputStream inputStream = new FileInputStream(configFile)) {
                Reader reader = new InputStreamReader(inputStream);

                properties.load(reader);
                logger.info(properties.entrySet()
                            .stream()
                            .map(e -> e.getKey() + " : " + e.getValue())
                            .collect(Collectors.joining(", ")));
            } catch (FileNotFoundException e) {
                logger.error("Input file {} not found", configFile, e);
                System.exit(1);
            } catch (IOException e) {
                logger.error("Error reading config file {}", configFile, e);
            }
        }
        else {
            logger.warn("No config file specified");
        }
    }

    protected void createProperties() {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY);

        addProperties(properties);

        readConfigFile(properties);

        if (bootstrapServers != null) {
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        if (schemaRegistryURL != null) {
            properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        }
    }

    protected void addProperties(Properties ignoredProperties) {
        // empty
    }
}
