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

public class SchemaEvolutionForwardExamples {

  public static void main(String[] args) {
    CustomerV2 customerV2 = CustomerV2.newBuilder()
        .setAge(25)
        .setFirstName("Mark")
        .setLastName("Simpson")
        .setHeight(178f)
        .setWeight(75f)
        .setEmail("mark.simpson@gmail.com")
        .setPhoneNumber("123-456-7890")
        .build();
    System.out.println("Customer V2 = " + customerV2);

    final DatumWriter<CustomerV2> datumWriter = new SpecificDatumWriter<>(CustomerV2.class);
    try (DataFileWriter<CustomerV2> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(customerV2.getSchema(), new File("customer-v2.avro"));
      dataFileWriter.append(customerV2);
      System.out.println("Successfully wrote customer-v2.avro");
    } catch (IOException e) {
      e.printStackTrace();
    }

    final File file = new File("customer-v2.avro");
    final DatumReader<CustomerV1> datumReader = new SpecificDatumReader<>(CustomerV1.class);
    try (final DataFileReader<CustomerV1> dataFileReader = new DataFileReader<>(file,
        datumReader)) {
      System.out.println("Reading our customer-v2.avro with v1 schema");
      while (dataFileReader.hasNext()) {
        CustomerV1 readCustomer = dataFileReader.next();
        System.out.println("Customer V1 = " + readCustomer);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
