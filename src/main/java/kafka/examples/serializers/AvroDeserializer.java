package kafka.examples.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class AvroDeserializer implements Deserializer<Object>
{
    private boolean isKey;

    // Move to base class
    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private static final Schema schema;

    static
    {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = "{\"type\" : \"bytes\"}";
        schema = parser.parse(schemaString);
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        }
        return buffer;
    }

    protected Schema getSchema()
    {
        return schema;
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
    public Object deserialize(String s, byte[] payload)
    {
        if (payload == null) {
            return null;
        }

        int id = -1;
        Schema readerSchema = null;

        try {
            ByteBuffer buffer = getByteBuffer(payload);
            id = buffer.getInt();
            String subject = null;
            // Schema schema = schemaRegistry.getBySubjectAndID(subject, id);
            Schema schema = getSchema();
            int length = buffer.limit() - 1 - idSize;
            final Object result;

            int start = buffer.position() + buffer.arrayOffset();
            DatumReader reader = getDatumReader(schema, readerSchema);
            Object object = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));
            return object;
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error deserializing Avro message for id " + id, e);
        }

    }

    @Override
    public void close()
    {

    }
}
