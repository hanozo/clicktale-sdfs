package mq;

import org.apache.avro.specific.SpecificRecord;

import java.io.Closeable;
import java.io.IOException;

/**
 * enable MQ implementation DI test wise
 */
public interface MQConsumer extends Closeable {
    void subscribe(String queue, java.util.function.Consumer<SpecificRecord> handler) throws IOException;
}
