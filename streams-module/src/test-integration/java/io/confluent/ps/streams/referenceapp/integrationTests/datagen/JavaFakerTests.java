package io.confluent.ps.streams.referenceapp.integrationTests.datagen;

import com.github.javafaker.Faker;
import com.google.common.io.Files;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class JavaFakerTests {

  /**
   * Example output:
   * Biggest: java-faker/src/main/resources/ca-CAT.yml.ca-CAT.faker.addresscity at size: 62
   * Biggest: java-faker/src/main/resources/ca-CAT.yml.ca-CAT.faker.addresscountry at size: 195
   * Biggest: java-faker/src/main/resources/uk.yml.uk.faker.namemale_last_name at size: 239
   * Biggest: java-faker/src/main/resources/en-ZA.yml.en-ZA.faker.namelast_name at size: 1000
   * Biggest: java-faker/src/main/resources/pt-BR.yml.pt-BR.faker.addresscity at size: 5287
   * Biggest: java-faker/src/main/resources/nl.yml.nl.faker.namefirst_name at size: 10020
   * Biggest: java-faker/src/main/resources/nl.yml.nl.faker.namelast_name at size: 22021
   */
  @Disabled("Needs to be changed to look inside the dep jar, not an extracted file")
  @Test
  public void countFromYaml() {
    Yaml yaml = new Yaml();
    Files.fileTraverser().breadthFirst(new File("java-faker/src/main/resources")).forEach(x -> {
      if (x.isDirectory()) return; //skip
      else {
        String absolutePath = x.getAbsolutePath();
        String read = null;
        try {
          read = Files.asCharSource(x, Charset.defaultCharset()).read();
        } catch (IOException e) {
          e.printStackTrace();
        }
        try {
          final Map valuesMap = yaml.loadAs(read, Map.class);
          deepFindListSize(valuesMap, absolutePath);
        } catch (Exception e) {
//          e.printStackTrace();
        }
      }
    });
  }

  int numberOfLists = 0;
  int maxListSize = 0;

  private void deepFindListSize(Map localeBased, String key) {
    Objects.requireNonNull(localeBased);

    Set<Map.Entry> values = localeBased.entrySet();
    values.forEach(x -> {
      Object value = x.getValue();
      if (Map.class.isAssignableFrom(value.getClass())) {
        Map y = (Map) value;
        deepFindListSize(y, key + "." + x.getKey().toString());
      } else if (List.class.isAssignableFrom(value.getClass())) {
        numberOfLists++;
        List y = (List) value;
        int size = y.size();
        if (size > maxListSize) {
          maxListSize = size;
          System.out.println("Biggest: " + key + x.getKey() + " at size: " + size);
        }
      }
    });
  }

  @Test
  void biggestList() {
    Locale nl = Locale.forLanguageTag("nl");
    String s = Faker.instance(nl).name().lastName();
  }

}
