package kafka.examples.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import kafka.examples.DatabusMessage;


public class AvroDeserializer extends AbstractAvroSerDeser implements Deserializer<DatabusMessage>
{
    private boolean isKey;

    private final DecoderFactory decoderFactory = DecoderFactory.get();

    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        }
        return buffer;
    }

    private DatumReader getDatumReader(Schema writerSchema, Schema readerSchema) {

        if (readerSchema == null) {
            return new GenericDatumReader(writerSchema);
        }

        return new GenericDatumReader(writerSchema, readerSchema);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        this.isKey = isKey;
    }

    @Override
    public DatabusMessage deserialize(String s, byte[] value)
    {
        if (value == null) {
            return null;
        }

        int id = -1;
        Schema readerSchema = null;

        try {
            ByteBuffer buffer = getByteBuffer(value);
            id = buffer.getInt();
            String subject = null;
            // Schema schema = schemaRegistry.getBySubjectAndID(subject, id);
            Schema schema = getSchema();
            int length = buffer.limit() - 1 - idSize;

            int start = buffer.position() + buffer.arrayOffset();
            DatumReader<GenericRecord> reader = getDatumReader(schema, readerSchema);
            GenericRecord object = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));
            Map<String, String> headers = (Map<String, String>) object.get("headers");
            ByteBuffer bbf =  (ByteBuffer) object.get("payload");
            byte[] payload = new byte[bbf.remaining()];
            bbf.get(payload);
            DatabusMessage message = new DatabusMessage(headers, payload);
            return message;
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error deserializing Avro message for id " + id, e);
        }

    }

    @Override
    public void close()
    {

    }
}
