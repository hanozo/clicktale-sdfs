package avro;

import avro.commands.*;
import mq.avro.MessageEnvelope;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Each MQ message is wrapped by MessageEnvelope to enforce homogeneous delivery through MQ.
 */
public class AvroJsonDecoder {

    private static final Logger logger = LogManager.getLogger();
    private final SpecificDatumReader<MessageEnvelope> envelopeReader = new SpecificDatumReader<>(MessageEnvelope.class);

    /**
     *
     * @param payload Only MessageEnvelope are consumed through MQ
     * @return The specific message
     * @throws IOException
     */
    public SpecificRecord deserialize(byte[] payload) throws IOException {

        try (ByteArrayInputStream stream = new ByteArrayInputStream(payload)) {

            MessageEnvelope envelope = this.envelopeReader.read(null, DecoderFactory.get().jsonDecoder(MessageEnvelope.getClassSchema(), stream));

            switch (envelope.getInnerSchema()) {
                case CreateFileCommand:
                    return deserialize(CreateFileCommand.class, envelope.getPayload().array());
                case RemoveFileCommand:
                    return deserialize(RemoveFileCommand.class, envelope.getPayload().array());
                case UpdateFileCommand:
                    return deserialize(UpdateFileCommand.class, envelope.getPayload().array());
                case MakeDirCommand:
                    return deserialize(MakeDirCommand.class, envelope.getPayload().array());
                case RemoveDirCommand:
                    return deserialize(RemoveDirCommand.class, envelope.getPayload().array());
                case RenameDirCommand:
                    return deserialize(RenameDirCommand.class, envelope.getPayload().array());
                default:
                    logger.error("{} message is not recognized.", envelope.getInnerSchema());

            }

        } catch (IOException e) {
            logger.error(e);
            throw e;
        }

        return null;
    }

    public  <T extends SpecificRecord> T deserialize(Class<T> clazz, byte[] payload) throws IOException {

        try (ByteArrayInputStream stream = new ByteArrayInputStream(payload)) {

            SpecificDatumReader<T> reader = new SpecificDatumReader<>(clazz);

            return reader.read(null, DecoderFactory.get().jsonDecoder(reader.getSchema(), stream));

        } catch (IOException e) {
            logger.error(e);
            throw e;
        }
    }
}
