package parquet.generator;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;
import java.time.DayOfWeek;

import static org.apache.parquet.schema.Types.*;

/***
 * This test will generate a Parquet File having a schema like this:
 *
 * ---------------------------------------------------------------------------
 * message TestMessage {
 *   required group primitive_types {
 *     required double double;
 *     repeated double repeated_double;
 *     required float float;
 *     repeated float repeated_float;
 *     required int32 int32;
 *     repeated int32 repeated_int32;
 *     required int64 long;
 *     repeated int64 repeated_long;
 *     required boolean boolean;
 *     repeated boolean repeated_boolean;
 *   }
 *   required group logical_types {
 *     required binary enum (ENUM);
 *     repeated binary repeated_enum (ENUM);
 *     required binary string (STRING);
 *     repeated binary repeated_string (STRING);
 *   }
 *   repeated group continent {
 *     required binary name (STRING);
 *   }
 * }
 * ---------------------------------------------------------------------------
 */

public class ParquetGeneratorTest {

    @Test
    public void parquetWriterTest() throws IOException {
        GroupType nestedSchema1 = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("double")
                .repeated(PrimitiveType.PrimitiveTypeName.DOUBLE).named("repeated_double")
                .required(PrimitiveType.PrimitiveTypeName.FLOAT).named("float")
                .repeated(PrimitiveType.PrimitiveTypeName.FLOAT).named("repeated_float")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("int32")
                .repeated(PrimitiveType.PrimitiveTypeName.INT32).named("repeated_int32")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("long")
                .repeated(PrimitiveType.PrimitiveTypeName.INT64).named("repeated_long")
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("boolean")
                .repeated(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("repeated_boolean")
                .named("primitive_types");

        SimpleGroup nestedSimpleGroup1 = new SimpleGroup(nestedSchema1);
        nestedSimpleGroup1.add("double", 45.45D);
        nestedSimpleGroup1.add("repeated_double", 56.4D);
        nestedSimpleGroup1.add("repeated_double", -93.0D);
        nestedSimpleGroup1.add("float", 23.6F);
        nestedSimpleGroup1.add("repeated_float", 12.6F);
        nestedSimpleGroup1.add("repeated_float", -12.99F);
        nestedSimpleGroup1.add("int32", 12);
        nestedSimpleGroup1.add("repeated_int32", 1992);
        nestedSimpleGroup1.add("repeated_int32", -292);
        nestedSimpleGroup1.add("long", 23L);
        nestedSimpleGroup1.add("repeated_long", 124L);
        nestedSimpleGroup1.add("repeated_long", -9292L);
        nestedSimpleGroup1.add("boolean", true);
        nestedSimpleGroup1.add("repeated_boolean", false);
        nestedSimpleGroup1.add("repeated_boolean", false);


        GroupType nestedSchema2 = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.enumType()).named("enum")
                .repeated(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.enumType()).named("repeated_enum")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("string")
                .repeated(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("repeated_string")
                .named("logical_types");

        SimpleGroup nestedSimpleGroup2 = new SimpleGroup(nestedSchema2);
        nestedSimpleGroup2.add("enum", Binary.fromConstantByteArray(DayOfWeek.SUNDAY.toString().getBytes()));
        nestedSimpleGroup2.add("repeated_enum", Binary.fromConstantByteArray(DayOfWeek.WEDNESDAY.toString().getBytes()));
        nestedSimpleGroup2.add("repeated_enum", Binary.fromConstantByteArray(DayOfWeek.FRIDAY.toString().getBytes()));
        nestedSimpleGroup2.add("string", "hello world");
        nestedSimpleGroup2.add("repeated_string", "Holla !");
        nestedSimpleGroup2.add("repeated_string", "Adios !");


        GroupType nestedSchema3 = repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
                .named("continent");

        SimpleGroup value1 = new SimpleGroup(nestedSchema3);
        value1.add("name", "ASIA");
        SimpleGroup value2 = new SimpleGroup(nestedSchema3);
        value2.add("name", "AFRICA");


        MessageType schema = buildMessage()
                .addField(nestedSchema1)
                .addField(nestedSchema2)
                .addField(nestedSchema3)
                .named("TestMessage");

        SimpleGroup simpleGroup = new SimpleGroup(schema);
        simpleGroup.add("primitive_types", nestedSimpleGroup1);
        simpleGroup.add("logical_types", nestedSimpleGroup2);
        simpleGroup.add("continent", value1);
        simpleGroup.add("continent", value2);

        ParquetGenerator parquetGenerator = new ParquetGenerator("local_file", schema);

        parquetGenerator.writeToFile(simpleGroup);

        parquetGenerator.closeWriter();
    }
}
