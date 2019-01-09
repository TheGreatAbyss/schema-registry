package io.confluent.connect.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BinaryNode;
import org.junit.Test;

public class DecimalTest {

  @Test
  public void integrationTestDecimalType() {
    Integer scale = 5;
    Schema decimalLogicalType = LogicalTypes.decimal(12, scale)
        .addToSchema(Schema.create(Type.BYTES));

    BigInteger defaultBigInteger = new BigInteger("310000000");
    byte[] defaultBigIntegerByteArray = defaultBigInteger.toByteArray();
    String b64String = DatatypeConverter.printBase64Binary(defaultBigIntegerByteArray);

    Schema schema = SchemaBuilder
        .record("myrecord").namespace("org.example").aliases("DecimalLogicalTypeDefaultByteBuffer")
        .fields()
        .name("number").type(decimalLogicalType).withDefault(b64String)
        .endRecord();

    BigInteger recordBigInteger = new BigInteger("1234567890");
    BigDecimal expectedBigDecimal = new BigDecimal(recordBigInteger, scale);

    GenericRecord avroRecord = new GenericRecordBuilder(schema)
        .set("number", recordBigInteger.toByteArray())
        .build();

    AvroData avroData = new AvroData(1000);
    SchemaAndValue schemaAndValue = avroData.toConnectData(schema, avroRecord);

    // test value contains expected decimal
    Struct connectValueStruct = (Struct) schemaAndValue.value();
    BigDecimal actualBigDecimal = (BigDecimal) connectValueStruct.get("number");
    assertEquals(expectedBigDecimal, actualBigDecimal);

    // test schema contains expected defaultValue in bytes
    org.apache.kafka.connect.data.Schema connectSchema = schemaAndValue.schema();
    org.apache.kafka.connect.data.Field connectField = connectSchema.field("number");
    byte[] connectBinaryValue = (byte[]) connectField.schema().defaultValue();
    BigInteger ConnectSchemaBigInt = new BigInteger(connectBinaryValue);
    assertEquals(defaultBigInteger, ConnectSchemaBigInt);

    // test fromConnect Schema
    Schema avroSchema = avroData.fromConnectSchema(schemaAndValue.schema());
    Field avroSchemaField = avroSchema.getField("number");
    try {
      byte[] avroSchemaBinaryValue = avroSchemaField.defaultValue().getBinaryValue();
      BigInteger avroSchemaBigInt = new BigInteger(avroSchemaBinaryValue);
      assertEquals(defaultBigInteger, avroSchemaBigInt);
    } catch (java.io.IOException e) {
      fail(e.toString());
    }

    GenericRecord connectRecord= (GenericRecord) avroData.fromConnectData(schemaAndValue.schema(), schemaAndValue.value());
    ByteBuffer connectByteBuffer= (ByteBuffer) connectRecord.get("number");
    assertEquals(recordBigInteger, new BigInteger(connectByteBuffer.array()));
  }

}
