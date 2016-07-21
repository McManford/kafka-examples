package kafka.examples.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


public class AvroSerializer implements Serializer<Object>
{
    private boolean isKey;

    // Move to base class
    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;

    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private static final Schema schema;

    static
    {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = "{\"type\" : \"bytes\"}";
        schema = parser.parse(schemaString);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, Object record)
    {
        Schema schema = null;
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema registration and return a null value in Kafka, instead
        // of an Avro encoded null.
        if (record == null) {
            return null;
        }

        try {
            schema = getSchema();
            // int id = schemaRegistry.register(subject, schema);
            int id = 0;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(idSize).putInt(id).array());

            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            DatumWriter<Object> writer;

            writer = new GenericDatumWriter<>(schema);

            writer.write(record, encoder);
            encoder.flush();

            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException | RuntimeException e) {
            // Avro serialization can throw AvroRuntimeException, NullPointerException,
            // ClassCastException, etc
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    @Override
    public void close()
    {

    }

    protected Schema getSchema()
    {
        return schema;
    }
}
