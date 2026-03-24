import java.time.Instant;

public class KafkaConfig {

    public static int BATCH_SIZE;
    public static long LINGER_MS;
    public static Instant batchStart;

    public static void init() {
        BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "500"));
        LINGER_MS  = Long.parseLong(System.getenv().getOrDefault("LINGER_MS", "20000"));
        batchStart = Instant.now();
    }
}
