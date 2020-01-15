package io.confluent.ps.streams.referenceapp.finance.money;

import io.confluent.ps.streams.referenceapp.finance.currency.Currencies;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.money.CurrencyUnit;
import java.io.File;

public class MoneyAvroTest {

  @SneakyThrows
  @Test
  @Disabled
  void serialise() {

    CurrencyUnit nzdCur = Currencies.nzdCur;

    String fileName = "test";
    File file = new File(fileName);
    DatumWriter<CurrencyUnit> writer = new ReflectDatumWriter<>(CurrencyUnit.class);
    DataFileWriter<CurrencyUnit> dataFileWriter = new DataFileWriter<>(writer);
    Schema schema = ReflectData.get().getSchema(CurrencyUnit.class);
    dataFileWriter.create(schema, file);

    dataFileWriter.append(nzdCur);

    dataFileWriter.close();
  }

  @SneakyThrows
  @Test
  @Disabled
  void Deserialize() {
    {
      File file = new File("avroFilePath");
      DatumReader<CurrencyUnit> datumReader = new ReflectDatumReader<>(CurrencyUnit.class);
      DataFileReader<CurrencyUnit> dataFileReader = new DataFileReader<>(file, datumReader);
      CurrencyUnit record = null;
      while (dataFileReader.hasNext()) {
        record = dataFileReader.next(record);
        // process record
      }
    }
  }
}
