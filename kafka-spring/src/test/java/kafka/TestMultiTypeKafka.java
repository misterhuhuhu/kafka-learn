package kafka;

import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(classes = KafkaApplicationMultiListener.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TestMultiTypeKafka {

    @Resource
    private KafkaApplicationMultiListener.MessageProducer producer;
    
    @Test
    void name() throws InterruptedException {
        producer.sendMessages();
        Thread.sleep(5000);
    }
}
