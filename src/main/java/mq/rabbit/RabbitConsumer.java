package mq.rabbit;

import avro.AvroJsonDecoder;
import com.rabbitmq.client.*;
import mq.MQConsumer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by user on 28/09/2017.
 */
public class RabbitConsumer implements MQConsumer {

    private static final Logger logger = LogManager.getLogger();

    private final AvroJsonDecoder decoder = new AvroJsonDecoder();
    private final Connection connection;
    private final Channel channel;
    private Consumer consumer;

    public RabbitConsumer(String host) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public void subscribe(String queue, java.util.function.Consumer<SpecificRecord> handler) throws IOException {

        channel.queueDeclare(queue, false, false, false, null);

        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {

                SpecificRecord record = decoder.deserialize(body);
                handler.accept(record);
            }
        };
        channel.basicConsume(queue, true, consumer);
    }

    @Override
    public void close() throws IOException {
        try {
            if (channel.isOpen()) channel.close();
        } catch (TimeoutException e) {
            logger.error(e);
        }
        if (connection.isOpen()) connection.close();
    }
}
