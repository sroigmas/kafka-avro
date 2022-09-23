package com.github.sroigmas.avro.evolution;

import com.github.sroigmas.avro.schema.CustomerV1;
import com.github.sroigmas.avro.schema.CustomerV2;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SchemaEvolutionBackwardExamples {

  public static void main(String[] args) {
    CustomerV1 customerV1 = CustomerV1.newBuilder()
        .setAge(34)
        .setAutomatedEmail(false)
        .setFirstName("John")
        .setLastName("Doe")
        .setHeight(178f)
        .setWeight(75f)
        .build();
    System.out.println("Customer V1 = " + customerV1);

    final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
    try (DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(customerV1.getSchema(), new File("customer-v1.avro"));
      dataFileWriter.append(customerV1);
      System.out.println("Successfully wrote customer-v1.avro");
    } catch (IOException e) {
      e.printStackTrace();
    }

    final File file = new File("customer-v1.avro");
    final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
    try (final DataFileReader<CustomerV2> dataFileReader = new DataFileReader<>(file,
        datumReader)) {
      System.out.println("Reading our customer-v1.avro with v2 schema");
      while (dataFileReader.hasNext()) {
        CustomerV2 readCustomer = dataFileReader.next();
        System.out.println("Customer V2 = " + readCustomer);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
