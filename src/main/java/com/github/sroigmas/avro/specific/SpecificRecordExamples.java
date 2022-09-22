package com.github.sroigmas.avro.specific;

import com.github.sroigmas.avro.schema.Customer;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SpecificRecordExamples {

  public static void main(String[] args) {
    Customer.Builder customerBuilder = Customer.newBuilder();
    customerBuilder.setAge(25);
    customerBuilder.setFirstName("John");
    customerBuilder.setLastName("Doe");
    customerBuilder.setHeight(175.5f);
    customerBuilder.setWeight(80.5f);
    //customerBuilder.setAutomatedEmail(false);
    Customer customer = customerBuilder.build();
    System.out.println(customer);

    final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
    try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
      dataFileWriter.append(customer);
      dataFileWriter.append(customer);
      System.out.println("Successfully wrote customer-specific.avro");
    } catch (IOException e) {
      e.printStackTrace();
    }

    final File file = new File("customer-specific.avro");
    final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
    try (final DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
      System.out.println("Reading our specific record");
      while (dataFileReader.hasNext()) {
        Customer readCustomer = dataFileReader.next();
        System.out.println(readCustomer);
        System.out.println("First name: " + readCustomer.getFirstName());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
