/*import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProducerService {
    private static final Logger LOGGER = LogManager.getLogger(ProducerService.class);
    private final String errorTopic;

    public ProducerService(String errorTopic) {
        this.errorTopic = errorTopic;
    }


    public void sendToKafka(String data, String topic, String key) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, data);
            AutoMobile_Data.producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Kafka publish failed to topic {}: {}", topic, exception.getMessage());
                } else {
                    // This is the metadata logic you were looking for!
                    LOGGER.info("Successfully routed to Topic: {}! Partition: {}, Offset: {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            LOGGER.error("Producer send error: {}", e.getMessage());
        }
    }
}*/