package io.confluent.ps.connect.etod;

import avro.shaded.com.google.common.collect.Lists;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EtodConversionTest {

  int size = 128 / 32;

  @Test
  void conversion() {
    val input = Lists.newArrayList(
            "00D77493B5DC87BB2C000000060C0005",
            "00D77493B94112285400000006000006",
            "00D774941087167E0400000006180006",
            "00D774A01942DBB33A000000063C0006");
    input.stream().map((x) -> {
      assertThat(x).hasSize(size);
      return x;
    });
  }
}
