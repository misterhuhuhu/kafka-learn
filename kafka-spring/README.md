## 使用 Kafka 发送大型消息

需要配置Kafka Producer、Kafka Broker、Topic 和 Kafka Consumer

所有这些都需要配置更新才能将大型消息从一端发送到另一端
- Kafka Producer 配置

ProducerConfig.MAX_REQUEST_SIZE_CONFIG
``` 
public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "20971520");

    return new DefaultKafkaProducerFactory<>(configProps);
}
```
- Topic配置
> ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic longMessage --partitions 1 \
--replication-factor 1 --config max.message.bytes=20971520 

或者
```
public NewTopic topic() {
    NewTopic newTopic = new NewTopic(longMsgTopicName, 1, (short) 1);
    Map<String, String> configs = new HashMap<>();
    configs.put("max.message.bytes", "20971520");
    newTopic.configs(configs);
    return newTopic;
}
``` 
- Kafka Broker
> message.max.bytes=20971520
- Consumer

  max.partition.fetch.bytes：此属性限制使用者可以从 Topic 的分区中获取的字节数

  fetch.max.bytes：此属性限制使用者可以从 Kafka 服务器本身获取的字节数
```
public ConsumerFactory<String, String> consumerFactory(String groupId) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
    return new DefaultKafkaConsumerFactory<>(props);
}
```

1. Kafka producer 提供了压缩消息的功能。此外，它还支持不同的压缩类型，我们可以使用 compression.type 属性进行配置。 
2. 我们可以将大消息存储在共享存储位置的文件中，并通过 Kafka 消息发送该位置。这可能是一个更快的选项，并且具有最小的处理开销。 
3. 另一种选择是在生产者端将大消息拆分为大小为 1KB 的小消息。之后，我们可以使用分区键将所有这些消息发送到单个分区，以确保正确的顺序。因此，稍后，在消费者端，我们可以从较小的消息中重建大消息。