package io.confluent.ps.streams.referenceapp.finance.topologies;

import lombok.Getter;
import org.apache.kafka.streams.kstream.TimeWindows;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;

import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;

@Getter
@Singleton
public class SnapshotTopologySettings {

  final Duration windowSize = ofHours(24);
  final Duration advanceBy = ofMinutes(30);
  final Duration windowGrace = ofMinutes(5);
  final Duration retentionPeriod = windowSize.multipliedBy(3); // Retention must be longer than window time

  final TimeWindows timeWindow = TimeWindows.of(windowSize).advanceBy(advanceBy).grace(windowGrace);

  @Inject
  public SnapshotTopologySettings() {
  }
}
