package com.github.sroigmas.avro.generic;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericRecordExamples {

  public static void main(String[] args) {
    // Step 0: Define schema
    Schema.Parser parser = new Parser();
    Schema schema = parser.parse("{\n"
        + "     \"type\": \"record\",\n"
        + "     \"namespace\": \"com.example\",\n"
        + "     \"name\": \"Customer\",\n"
        + "     \"doc\": \"Avro Schema for our Customer\",\n"
        + "     \"fields\": [\n"
        + "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n"
        + "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n"
        + "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n"
        + "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n"
        + "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n"
        + "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n"
        + "     ]\n"
        + "}");

    // Step 1: Create a generic record
    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 25);
    customerBuilder.set("height", 170f);
    customerBuilder.set("weight", 80.5f);
    //customerBuilder.set("automated_email", false);
    GenericData.Record customer = customerBuilder.build();
    System.out.println(customer);

    // Step 2: Write to a file
    final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
      dataFileWriter.append(customer);
      System.out.println("Written customer-generic.avro");
    } catch (IOException e) {
      System.out.println("Couldn't write file");
      e.printStackTrace();
    }

    // Step 3: Read from a file
    final File file = new File("customer-generic.avro");
    final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
      GenericRecord customerRead = dataFileReader.next();
      //dataFileReader.hasNext();
      System.out.println("Successfully read Avro file");
      System.out.println(customerRead);

      // get data from generic record
      System.out.println("First name: " + customerRead.get("first_name"));

      // read a non-existing field
      System.out.println("Non-existing field: " + customerRead.get("not_here"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
