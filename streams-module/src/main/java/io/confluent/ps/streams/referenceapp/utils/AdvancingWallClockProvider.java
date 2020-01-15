package io.confluent.ps.streams.referenceapp.utils;

import java.time.Duration;
import java.time.Instant;

public interface AdvancingWallClockProvider extends WallClockProvider {

  Instant advanceAndGet(Duration time);

}
