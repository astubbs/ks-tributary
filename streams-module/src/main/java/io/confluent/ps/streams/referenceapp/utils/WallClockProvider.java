package io.confluent.ps.streams.referenceapp.utils;

import java.time.Instant;

public interface WallClockProvider {

  Instant getNow();

}
