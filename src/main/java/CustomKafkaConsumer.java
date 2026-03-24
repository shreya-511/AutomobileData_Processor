import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomKafkaConsumer {


    public static final Logger logger = LogManager.getLogger(CustomKafkaConsumer.class);
    private static List<String> globalBuffer = new ArrayList<>();
    private static Instant batchStart = Instant.now();
    public static KafkaProducer<String, String> producer;


    public static void main(String[] args) throws Exception {
        KafkaConfig.init();

        String bootstrap     = System.getenv().getOrDefault("BOOTSTRAP_SERVER",  "localhost:9092");
        String consumerTopic = System.getenv().getOrDefault("CONSUMER_TOPIC",    "automobile_raw_diagnostics");
        String errorTopic    = System.getenv().getOrDefault("ERROR_TOPIC",       "automobile_failures");
        String consumerGroup = System.getenv().getOrDefault("CONSUMER_GROUP",    "Consumer_Automobile");
        String postgresUri   = System.getenv().getOrDefault("POSTGRES_URI",      "jdbc:postgresql://localhost:5432/automobile_diagnostics_data?reWriteBatchedInserts=true");
        String pgUser        = System.getenv().getOrDefault("POSTGRES_USERNAME", "postgres");
        String pgPass        = System.getenv().getOrDefault("POSTGRES_PASSWORD", "postgres123");
        String pgTable       = System.getenv().getOrDefault("DB_TABLE",          "automobile_diagnostic_new");

        Properties cProps = new Properties();
        cProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrap);
        cProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,                 consumerGroup);
        cProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        cProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        cProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest");

        Properties pProps = new Properties();
        pProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrap);
        pProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        pProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(pProps);


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);

             Connection connection = DriverManager.getConnection(postgresUri, pgUser, pgPass)) {


            consumer.subscribe(Collections.singletonList(consumerTopic));


            logger.info("Service Started. Waiting for records...");

            PostgresImplementation processor = new PostgresImplementation(connection,errorTopic, pgTable);


            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                loadDataFromKafka(records);

                if (globalBuffer.size() >= KafkaConfig.BATCH_SIZE) {
                    logger.info("Based On Batch Inserting to Database");
                    List<String> secondBuffer = new ArrayList<>(globalBuffer);
                    globalBuffer.removeAll(secondBuffer);
                    batchStart = Instant.now();
                    processor.process(secondBuffer);
                    secondBuffer.clear();
                }

                else if (!globalBuffer.isEmpty() && Duration.between(batchStart, Instant.now()).toMillis() >= KafkaConfig.LINGER_MS) {
                    logger.info("Based On LingerMs Inserting to Database");
                    List<String> secondBuffer = new ArrayList<>(globalBuffer);
                    globalBuffer.removeAll(secondBuffer);
                    batchStart = Instant.now();
                    processor.process(secondBuffer);
                    secondBuffer.clear();
                }
            }
        }
    }


    public static void loadDataFromKafka(ConsumerRecords<String, String> records) {
        if (records.isEmpty()) return;

        for (ConsumerRecord<String, String> record : records) {

            globalBuffer.add(record.value());
        }
        logger.info("[STAGING] Raw records loaded. Current size: {}", globalBuffer.size());
    }}