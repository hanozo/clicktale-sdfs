package avro;

import mq.avro.Commands;
import mq.avro.MessageEnvelope;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Each message T is wrapped by MessageEnvelope to enforce homogeneous delivery through MQ.
 */
public class AvroJsonEncoder {

    private static final Logger logger = LogManager.getLogger();

    /**
     * @param record The specific message.
     * @return Envelope message payload.
     * @throws IOException
     */
    public <T extends SpecificRecord> byte[] serialize(T record) throws IOException {

        byte[] payload = encode(record);

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setInnerSchema(Commands.valueOf(record.getSchema().getName()))
                .setPayload(ByteBuffer.wrap(payload))
                .build();

        return this.encode(envelope);

    }

    public  <T extends SpecificRecord> byte[] encode(T record) throws IOException {

        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {

            Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), stream);

            DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());

            writer.write(record, encoder);

            encoder.flush();

            return stream.toByteArray();

        } catch (IOException e) {
            logger.error(e);
            throw e;
        }
    }
}
