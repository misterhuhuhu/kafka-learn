package streams;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;
import java.util.Properties;

@Slf4j
@DirtiesContext
@EmbeddedKafka(partitions = 6, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@SpringBootTest(classes = KafkaStreamsApplication.class)
class KafkaStreamsApplicationLiveTest {

//    private final BlockingQueue<String> output = new LinkedBlockingQueue<>();
    
    private WordCountProcessor wordCountProcessor;
    
    @Resource
    private StreamsBuilderFactoryBean factoryBean;
    @Resource
    private KafkaProducer kafkaProducer;
    
    
    @BeforeEach
    public void setUp() {
//        output.clear();
        wordCountProcessor = new WordCountProcessor();
    }
    
    //    @DynamicPropertySource
//    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
//        registry.add("spring.kafka.streams.state.dir", tempDir::getAbsolutePath);
//    }
    @Test
    void givenInputMessages_whenPostToEndpoint_thenWordCountsReceivedOnOutput() throws Exception {
        addMessage("test message");
        addMessage("another test message");
        Thread.sleep(2000);
        log.info("test {}",getWordCount("test"));
        log.info("message {}",getWordCount("message"));
        log.info("another {}",getWordCount("another"));
        
    }
    
    @Test
    void 测试wordCountProcessor() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        wordCountProcessor.buildPipeline(streamsBuilder);
        Topology topology = streamsBuilder.build();
        
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            
            TestInputTopic<String, String> inputTopic = topologyTestDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, Long> outputTopic = topologyTestDriver.createOutputTopic("output-topic", new StringDeserializer(), new LongDeserializer());
            inputTopic.pipeInput("key", "hello world");
            inputTopic.pipeInput("key2", "hello");
            List<KeyValue<String, Long>> keyValues = outputTopic.readKeyValuesToList();
            keyValues.forEach(k -> log.info("key = {},value = {}", k.key, k.value));
        }
    }
    
    public long getWordCount(String word) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
        return counts.get(word);
    }
    
    @Test
    public void addMessage(String message) {
        kafkaProducer.sendMessage(message);
    }
    
}
