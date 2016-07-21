package kafka.examples.serializers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import kafka.examples.DatabusMessage;


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
        if (message == null) {
            return null;
        }

        try {
            Schema schema = getSchema();
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
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    @Override
    public void close()
    {

    }
}
