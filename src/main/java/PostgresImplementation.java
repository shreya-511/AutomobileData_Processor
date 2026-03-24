import org.apache.kafka.clients.producer.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.*;
import java.util.*;

public class PostgresImplementation {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LogManager.getLogger(PostgresImplementation.class);

    private final Connection connection;
    private final String errorTopic;
    private final String pgTable;

    public PostgresImplementation(Connection connection, String errorTopic, String pgTable) {
        this.connection = connection;
        this.errorTopic = errorTopic;
        this.pgTable = pgTable;
    }


    public void process(List<String> secondBuffer) {
        if (secondBuffer == null || secondBuffer.isEmpty()) return;

        try {
            insertData(secondBuffer);
            logger.info("[SUCCESS] Batch of {} records inserted to DB.", secondBuffer.size());
        } catch (Exception e) {
            logger.error("[FAILURE] DB Insert failed: {}. Sending to error topic.", e.getMessage());
            sendToErrorTopic(secondBuffer);
        }
    }

    private void insertData(List<String> secondBuffer) throws Exception {
        Map<String, Object> schemaMap = mapper.readValue(secondBuffer.get(0), Map.class);
        List<String> columns = new ArrayList<>(schemaMap.keySet());

        String columnNames = String.join(",", columns);
        String placeholders = String.join(",", Collections.nCopies(columns.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", pgTable, columnNames, placeholders);

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (String rawJson : secondBuffer) {
                Map<String, Object> data = mapper.readValue(rawJson, Map.class);

                for (int i = 0; i < columns.size(); i++) {
                    pstmt.setObject(i + 1, data.get(columns.get(i)));
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }





    private void sendToErrorTopic(List<String> secondBuffer) {
        for (String data : secondBuffer) {
            try {
                String jsonPayload = mapper.writeValueAsString(data);
                ProducerRecord<String, String> record = new ProducerRecord<>(this.errorTopic, null, jsonPayload);
                CustomKafkaConsumer.producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Sent to Error Topic! Topic: {} | Partition: {} | Offset: {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        logger.error("kafka producer failed: {}", exception.getMessage());
                    }
                });

            } catch (Exception e) {
                logger.error("JSON Mapping failed for a record: {}", e.getMessage());
            }
        }
    }}