package kafka.examples.serializers;

import org.apache.avro.Schema;


public class AbstractAvroSerDeser {

    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;
    protected static final String HEADERS_FIELD_NAME = "headers";
    protected static final String PAYLOAD_FIELD_NAME = "payload";

    private static final Schema schema;

    static
    {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = "{" +
                "    \"namespace\":\"com.intel.databus.client\"," +
                "    \"type\": \"record\"," +
                "    \"name\": \"DatabusMessage\"," +
                "    \"fields\": [" +
                "        {" +
                "            \"name\": \""+HEADERS_FIELD_NAME+"\"," +
                "            \"type\": {" +
                "                \"type\": \"map\"," +
                "                \"values\": \"string\"" +
                "            }" +
                "        }," +
                "        {" +
                "            \"name\":\""+PAYLOAD_FIELD_NAME+"\"," +
                "            \"type\":\"bytes\"" +
                "        }" +
                "    ]" +
                "}";
        schema = parser.parse(schemaString);
    }

    public static Schema getSchema() {
        return schema;
    }
}
