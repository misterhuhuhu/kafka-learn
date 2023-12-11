package kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);

        MessageProducer producer = context.getBean(MessageProducer.class);
        MessageListener listener = context.getBean(MessageListener.class);
        /*
         * Sending a Hello World message to topic 'baeldung'. 
         * Must be received by both listeners with group foo
         * and bar with containerFactory fooKafkaListenerContainerFactory
         * and barKafkaListenerContainerFactory respectively.
         * It will also be received by the listener with
         * headersKafkaListenerContainerFactory as container factory.
         */
        producer.sendBaeldungMessage("Hello, World!");
        listener.latch.await(10, TimeUnit.SECONDS);

        /*
         * Sending message to a topic with 5 partitions,
         * each message to a different partition. But as per
         * listener configuration, only the messages from
         * partition 0 and 3 will be consumed.
         */
        for (int i = 0; i < 5; i++) {
            producer.sendMessageToPartition("Hello To Partitioned Topic!", i);
        }
        listener.partitionLatch.await(10, TimeUnit.SECONDS);

        /*
         * Sending message to 'filtered' topic. As per listener
         * configuration,  all messages with char sequence
         * 'World' will be discarded.
         */
        producer.sendMessageToFiltered("Hello Baeldung!");
        producer.sendMessageToFiltered("Hello World!");
        listener.filterLatch.await(10, TimeUnit.SECONDS);

        /*
         * Sending message to 'greeting' topic. This will send
         * and received a java object with the help of
         * greetingKafkaListenerContainerFactory.
         */
        producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
        listener.greetingLatch.await(10, TimeUnit.SECONDS);

        context.close();
    }

    @Bean
    public MessageProducer messageProducer() {
        return new MessageProducer();
    }

    @Bean
    public MessageListener messageListener() {
        return new MessageListener();
    }

    public static class MessageProducer {
        public KafkaTemplate<String, String> getKafkaTemplate() {
            return kafkaTemplate;
        }
        
        public MessageProducer setKafkaTemplate(KafkaTemplate<String, String> kafkaTemplate) {
            this.kafkaTemplate = kafkaTemplate;
            return this;
        }
        
        public KafkaTemplate<String, Greeting> getGreetingKafkaTemplate() {
            return greetingKafkaTemplate;
        }
        
        public MessageProducer setGreetingKafkaTemplate(KafkaTemplate<String, Greeting> greetingKafkaTemplate) {
            this.greetingKafkaTemplate = greetingKafkaTemplate;
            return this;
        }
        
        public String getTopicName() {
            return topicName;
        }
        
        public MessageProducer setTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }
        
        public String getPartitionedTopicName() {
            return partitionedTopicName;
        }
        
        public MessageProducer setPartitionedTopicName(String partitionedTopicName) {
            this.partitionedTopicName = partitionedTopicName;
            return this;
        }
        
        public String getFilteredTopicName() {
            return filteredTopicName;
        }
        
        public MessageProducer setFilteredTopicName(String filteredTopicName) {
            this.filteredTopicName = filteredTopicName;
            return this;
        }
        
        public String getGreetingTopicName() {
            return greetingTopicName;
        }
        
        public MessageProducer setGreetingTopicName(String greetingTopicName) {
            this.greetingTopicName = greetingTopicName;
            return this;
        }
        
        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        @Autowired
        private KafkaTemplate<String, Greeting> greetingKafkaTemplate;

        @Value(value = "${message.topic.name}")
        private String topicName;

        @Value(value = "${partitioned.topic.name}")
        private String partitionedTopicName;

        @Value(value = "${filtered.topic.name}")
        private String filteredTopicName;

        @Value(value = "${greeting.topic.name}")
        private String greetingTopicName;

        public void sendBaeldungMessage(String message) {

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
            future.whenComplete((result, ex) -> {

                if (ex == null) {
                    System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata()
                        .offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            });
        }

        public void sendMessageToPartition(String message, int partition) {
            kafkaTemplate.send(partitionedTopicName, partition, null, message);
        }

        public void sendMessageToFiltered(String message) {
            kafkaTemplate.send(filteredTopicName, message);
        }

        public void sendGreetingMessage(Greeting greeting) {
            greetingKafkaTemplate.send(greetingTopicName, greeting);
        }
    }

    public static class MessageListener {
        public CountDownLatch getLatch() {
            return latch;
        }
        
        public MessageListener setLatch(CountDownLatch latch) {
            this.latch = latch;
            return this;
        }
        
        public CountDownLatch getPartitionLatch() {
            return partitionLatch;
        }
        
        public MessageListener setPartitionLatch(CountDownLatch partitionLatch) {
            this.partitionLatch = partitionLatch;
            return this;
        }
        
        public CountDownLatch getFilterLatch() {
            return filterLatch;
        }
        
        public MessageListener setFilterLatch(CountDownLatch filterLatch) {
            this.filterLatch = filterLatch;
            return this;
        }
        
        public CountDownLatch getGreetingLatch() {
            return greetingLatch;
        }
        
        public MessageListener setGreetingLatch(CountDownLatch greetingLatch) {
            this.greetingLatch = greetingLatch;
            return this;
        }
        
        private CountDownLatch latch = new CountDownLatch(3);

        private CountDownLatch partitionLatch = new CountDownLatch(2);

        private CountDownLatch filterLatch = new CountDownLatch(2);

        private CountDownLatch greetingLatch = new CountDownLatch(1);

        @KafkaListener(topics = "${message.topic.name}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
        public void listenGroupFoo(String message) {
            System.out.println("Received Message in group 'foo': " + message);
            latch.countDown();
        }

        @KafkaListener(topics = "${message.topic.name}", groupId = "bar", containerFactory = "barKafkaListenerContainerFactory")
        public void listenGroupBar(String message) {
            System.out.println("Received Message in group 'bar': " + message);
            latch.countDown();
        }

        @KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
        public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
            System.out.println("listenWithHeaders:Received Message: " + message + " from partition: " + partition);
            latch.countDown();
        }

        @KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = { "0", "3" }), containerFactory = "partitionsKafkaListenerContainerFactory")
        public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
            System.out.println("listenToPartition:Received Message: " + message + " from partition: " + partition);
            this.partitionLatch.countDown();
        }

        @KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
        public void listenWithFilter(String message) {
            System.out.println("listenWithFilter:Received Message in filtered listener: " + message);
            this.filterLatch.countDown();
        }

        @KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
        public void greetingListener(Greeting greeting) {
            System.out.println("greetingListener:Received greeting message: " + greeting);
            this.greetingLatch.countDown();
        }

    }

}
