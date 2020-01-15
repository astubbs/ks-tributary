package io.confluent.ps.streams.referenceapp.integrationTests.datagen;

import io.confluent.ps.streams.referenceapp.utils.AdvancingWallClockProvider;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;

@RequiredArgsConstructor
public class WallClockStub implements AdvancingWallClockProvider {

  @Nonnull
  private Instant baseTime;

  @Override
  public Instant getNow() {
    return baseTime;
  }

  @Override
  public Instant advanceAndGet(Duration time) {
    baseTime = baseTime.plus(time);
    return baseTime;
  }
}
