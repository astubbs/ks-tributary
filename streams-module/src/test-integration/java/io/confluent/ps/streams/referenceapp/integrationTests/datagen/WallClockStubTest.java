package io.confluent.ps.streams.referenceapp.integrationTests.datagen;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class WallClockStubTest {

  @Test
  void test() {
    WallClockStub c = new WallClockStub(GenUtils.randomSeedInstant);
    assertThat(c.getNow()).isEqualTo(GenUtils.randomSeedInstant);
    Duration time = ofMillis(1);
    Instant nowAndAdvance = c.advanceAndGet(time);
    assertThat(nowAndAdvance).isEqualTo(GenUtils.randomSeedInstant.plus(time));
  }
}
