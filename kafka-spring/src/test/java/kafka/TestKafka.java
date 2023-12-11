package kafka;

import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = KafkaApplication.class)
@DirtiesContext
@EmbeddedKafka(partitions = 6, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TestKafka {
    @Resource
    KafkaApplication.MessageProducer producer;
    @Resource
    KafkaApplication.MessageListener listener;
    
    @Test
    public void Runtest() throws Exception {
        
        
        /*
         * Sending a Hello World message to topic 'baeldung'.
         * Must be received by both listeners with group foo
         * and bar with containerFactory fooKafkaListenerContainerFactory
         * and barKafkaListenerContainerFactory respectively.
         * It will also be received by the listener with
         * headersKafkaListenerContainerFactory as container factory.
         */
        producer.sendBaeldungMessage("Hello, World!");
        listener.getLatch().await(2, TimeUnit.SECONDS);
        
        /*
         * Sending message to a topic with 5 partitions,
         * each message to a different partition. But as per
         * listener configuration, only the messages from
         * partition 0 and 3 will be consumed.
         */
        for (int i = 0; i < 5; i++) {
            producer.sendMessageToPartition("Hello To Partitioned Topic!", i);
        }
        listener.getPartitionLatch().await(2, TimeUnit.SECONDS);
        
        /*
         * Sending message to 'filtered' topic. As per listener
         * configuration,  all messages with char sequence
         * 'World' will be discarded.
         */
        producer.sendMessageToFiltered("Hello Baeldung!");
        producer.sendMessageToFiltered("Hello World!");
        listener.getFilterLatch().await(2, TimeUnit.SECONDS);
        
        /*
         * Sending message to 'greeting' topic. This will send
         * and received a java object with the help of
         * greetingKafkaListenerContainerFactory.
         */
        producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
        listener.getGreetingLatch().await(2, TimeUnit.SECONDS);
    }
}
