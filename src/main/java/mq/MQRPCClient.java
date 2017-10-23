package mq;

import avro.commands.BareResponse;
import org.apache.avro.specific.SpecificRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface MQRPCClient extends Closeable {

    <T extends SpecificRecord> CompletableFuture<BareResponse> call(T request) throws IOException, InterruptedException;
}
