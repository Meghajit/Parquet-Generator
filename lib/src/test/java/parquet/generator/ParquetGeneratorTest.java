package parquet.generator;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.Types.buildMessage;

public class ParquetGeneratorTest {

    @Test
    public void parquetWriterTest() throws IOException {
        MessageType schema = buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
                .required(PrimitiveType.PrimitiveTypeName.FLOAT).named("weight")
                .named("TestMessage");

        SimpleGroup simpleGroup = new SimpleGroup(schema);
        simpleGroup.add("name", "Chintu");
        simpleGroup.add("weight", 23.6F);

        ParquetGenerator parquetGenerator = new ParquetGenerator("local_file", schema);

        parquetGenerator.writeToFile(simpleGroup);

        parquetGenerator.closeWriter();
    }
}
