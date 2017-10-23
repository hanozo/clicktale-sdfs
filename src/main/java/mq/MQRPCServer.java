package mq;

import avro.commands.BareResponse;
import org.apache.avro.specific.SpecificRecord;

import java.io.Closeable;

public interface MQRPCServer extends Closeable {

    void process(java.util.function.Function<SpecificRecord, BareResponse> handler);
}
