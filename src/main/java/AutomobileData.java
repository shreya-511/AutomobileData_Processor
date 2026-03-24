/* import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.time.Duration;
import java.util.*;

public class AutomobileData {
    public static ObjectMapper mapper = new ObjectMapper();
    public static Logger logger = LogManager.getLogger(AutomobileData.class);

    public static void main(String[] args) throws Exception {

        String bootstrap        = System.getenv().getOrDefault("BOOTSTRAP_SERVER",  "localhost:9092");
        String consumerTopic    = System.getenv().getOrDefault("CONSUMER_TOPIC",    "automobile_raw_diagnostics");
        String producerTopic    = System.getenv().getOrDefault("PRODUCER_TOPIC",    "automobile_processed_diagnostics");
        String consumerGroup    = System.getenv().getOrDefault("CONSUMER_GROUP",    "consumer_Automobile");
        String postgresUri      = System.getenv().getOrDefault("POSTGRES_URI",      "jdbc:postgresql://localhost:5432/");
        String postgresDbName   = System.getenv().getOrDefault("DB_NAME",           "automobile_diagnostics_data");
        String postgresUsername = System.getenv().getOrDefault("POSTGRES_USERNAME", "postgres");
        String postgresTable    = System.getenv().getOrDefault("DB_TABLE",          "automobile_diagnostic_new");
        String postgrepassword  = System.getenv().getOrDefault("POSTGRES_PASSWORD", "postgres123");

        Properties cProps = new Properties();
        cProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        cProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Properties pProps = new Properties();
        pProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Properties dbProps = new Properties();
        dbProps.setProperty("user", postgresUsername);
        dbProps.setProperty("password", postgrepassword);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(pProps);
             Connection connection = DriverManager.getConnection(postgresUri + postgresDbName, dbProps)) {

            connection.setAutoCommit(false);
            consumer.subscribe(Collections.singletonList(consumerTopic));
            logger.info("Consumer Active on {}", consumerTopic);

            while (true) {
                ConsumerRecords<String, String> records = null;
                try {
                    records = consumer.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()) {
                        logger.info("[POLL] Got {} records", records.count());

                        for (ConsumerRecord<String, String> record : records) {
                            String sanitized = record.value().replaceAll("[\\r\\n]+", "").trim();
                            logger.info("[KAFKA-RECEIVED] -> {}", sanitized);

                            JsonNode inputData = mapper.readTree(sanitized);
                            String imei = inputData.get("imei") != null ? inputData.get("imei").asText() : "Not Available";

                            try {

                                Iterator<Map.Entry<String, JsonNode>> fields = inputData.fields();
                                StringBuilder columns = new StringBuilder();
                                StringBuilder placeholders = new StringBuilder();
                                List<JsonNode> valueList = new ArrayList<>();

                                while (fields.hasNext()) {
                                    Map.Entry<String, JsonNode> field = fields.next();
                                    columns.append(field.getKey()).append(",");
                                    placeholders.append("?").append(",");
                                    valueList.add(field.getValue());
                                }

                                String sql = "INSERT INTO " + postgresTable +
                                        " (" + columns.toString().replaceAll(",$", "") + ")" +
                                        " VALUES (" + placeholders.toString().replaceAll(",$", "") + ")";

                                PreparedStatement pst = connection.prepareStatement(sql);

                                for (int i = 0; i < valueList.size(); i++) {
                                    Object jsonValue = mapper.treeToValue(valueList.get(i), Object.class);
                                    pst.setObject(i + 1, jsonValue);
                                }

                                pst.executeUpdate();
                                connection.commit();
                                logger.info("[POSTGRES-SAVED] imei: {}", imei);

                                producer.send(new ProducerRecord<>(producerTopic, imei, sanitized),
                                        (metadata, exception) -> {
                                            if (exception != null) {
                                                logger.error("[PRODUCER-ERROR] {}", exception.getMessage());
                                            } else {
                                                logger.info("[PRODUCER-SENT] imei: {} partition: {} offset: {}",
                                                        imei, metadata.partition(), metadata.offset());
                                            }
                                        });

                            } catch (Exception e) {
                                logger.error("[RECORD-ERROR] Skipping record: {}", e.getMessage());
                                connection.rollback();
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.error("DATA ERROR: {}", e.getMessage());
                    connection.rollback();
                    e.printStackTrace();
                }
            }
        }
    }
}
*/