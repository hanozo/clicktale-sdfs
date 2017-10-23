package mq;

import org.apache.avro.specific.SpecificRecord;

import java.io.Closeable;

public interface MQRPCServer extends Closeable {

    void process(java.util.function.Function<SpecificRecord, SpecificRecord> handler);
}
