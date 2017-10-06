package mq;

import org.apache.avro.specific.SpecificRecord;

import java.io.Closeable;

/**
 * enable MQ implementation DI test wise
 */
public interface MQProducer extends Closeable {

    <T extends SpecificRecord> void send(T message) throws InterruptedException;
}
