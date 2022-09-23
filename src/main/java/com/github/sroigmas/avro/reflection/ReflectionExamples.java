package com.github.sroigmas.avro.reflection;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class ReflectionExamples {

  public static void main(String[] args) {
    Schema schema = ReflectData.get().getSchema(ReflectedCustomer.class);
    System.out.println("Schema = " + schema.toString(true));

    try {
      System.out.println("Writing customer-reflected.avro");
      File file = new File("customer-reflected.avro");
      DatumWriter<ReflectedCustomer> writer = new ReflectDatumWriter<>(ReflectedCustomer.class);
      try (DataFileWriter<ReflectedCustomer> out = new DataFileWriter<>(writer)
          .setCodec(CodecFactory.deflateCodec(9))
          .create(schema, file)) {
        out.append(new ReflectedCustomer("Bill", "Clark", "The Rocket"));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      System.out.println("Reading customer-reflected.avro");
      File file = new File("customer-reflected.avro");
      DatumReader<ReflectedCustomer> reader = new ReflectDatumReader<>(ReflectedCustomer.class);
      try (DataFileReader<ReflectedCustomer> in = new DataFileReader<>(file, reader)) {
        for (ReflectedCustomer reflectedCustomer : in) {
          System.out.println(reflectedCustomer.fullName());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
