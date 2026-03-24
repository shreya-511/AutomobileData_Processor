/*import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaToPostgres {
    private static final Logger logger = LogManager.getLogger(KafkaToPostgres.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Set<String> columnCache = new HashSet<>();

    public static void main(String[] args) throws Exception {

        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVER", "localhost:9092");
        String topic = System.getenv().getOrDefault("CONSUMER_TOPIC", "automobile_raw_diagnostics");
        String pgUri = System.getenv().getOrDefault("POSTGRES_URI", "jdbc:postgresql://localhost:5432/automobile_diagnostics_data");
        String pgUser = System.getenv().getOrDefault("POSTGRES_USERNAME", "postgres");
        String pgPass = System.getenv().getOrDefault("POSTGRES_PASSWORD", "postgres123");
        String table = System.getenv().getOrDefault("DB_TABLE", "automobile_diagnostic_new");

        Properties cProps = new Properties();
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_Automobile_Dynamic");
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);
             Connection conn = DriverManager.getConnection(pgUri, pgUser, pgPass)) {

            consumer.subscribe(Collections.singletonList(topic));
            refreshColumnCache(conn, table);
            logger.info("Service Started. Listening on {}", topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Map<String, Object> data = mapper.readValue(record.value(), Map.class);

                        ensureColumnsExist(conn, table, data);

                        List<String> keys = new ArrayList<>(data.keySet());
                        String columns = String.join(", ", keys);
                        String placeholders = keys.stream().map(k -> "?").collect(Collectors.joining(", "));
                        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders);

                        try (PreparedStatement pst = conn.prepareStatement(sql)) {
                            for (int i = 0; i < keys.size(); i++) {
                                Object val = data.get(keys.get(i));

                                if (val instanceof Number) {
                                    pst.setDouble(i + 1, ((Number) val).doubleValue());
                                } else {

                                    pst.setObject(i + 1, val);
                                }
                            }
                            pst.executeUpdate();
                            logger.info("[DB-SAVED] imei: {}", data.getOrDefault("imei", "N/A"));
                        }
                    } catch (Exception e) {
                        logger.error("[ERROR] Record failed: {}", e.getMessage());
                    }
                }
            }
        }
    }

    private static void ensureColumnsExist(Connection conn, String table, Map<String, Object> data) throws SQLException {
        for (String key : data.keySet()) {
            String colName = key.toLowerCase();
            if (!columnCache.contains(colName)) {
                String type = getPostgresType(data.get(key));
                String sql = String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", table, colName, type);
                try (Statement st = conn.createStatement()) {
                    st.execute(sql);
                    columnCache.add(colName);
                    logger.info("[SCHEMA] Added column: {} ({})", colName, type);
                }
            }
        }
    }

    private static String getPostgresType(Object val) {
        if (val instanceof Number) return "DOUBLE PRECISION";
        if (val instanceof Boolean) return "BOOLEAN";
        return "TEXT";
    }

    private static void refreshColumnCache(Connection conn, String table) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getColumns(null, null, table.toLowerCase(), null)) {
            while (rs.next()) {
                columnCache.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
        }
    }
}

*/

