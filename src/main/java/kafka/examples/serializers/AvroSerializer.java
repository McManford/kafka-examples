package kafka.examples.serializers;

import kafka.examples.DatabusMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


public class AvroSerializer extends AbstractAvroSerDeser implements Serializer<DatabusMessage>
{
    private boolean isKey;

    private final EncoderFactory encoderFactory = EncoderFactory.get();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, DatabusMessage message)
    {
        Schema schema = null;
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass recordSchema registration and return a null value in Kafka, instead
        // of an Avro encoded null.
        if (message == null) {
            return null;
        }

        try {
            schema = getSchema();
            GenericRecord databusValue = new GenericData.Record(schema);

            databusValue.put("headers", message.getHeaders());
            databusValue.put("payload", ByteBuffer.wrap(message.getPayload()));

            // int id = schemaRegistry.register(subject, recordSchema);
            int id = 0;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(idSize).putInt(id).array());

            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            writer.write(databusValue, encoder);
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
}
