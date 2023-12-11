package kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(id = "multiGroup", topics = "multitype")
public class MultiTypeKafkaListener {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiTypeKafkaListener.class);
    @KafkaHandler
    public void handleGreeting(Greeting greeting) {
        
        logger.info("Greeting received: " + greeting);
    }

    @KafkaHandler
    public void handleFarewell(Farewell farewell) {
        logger.info("Farewell received: " + farewell);
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object object) {
        logger.info("Unkown type received: " + object);
    }

}
