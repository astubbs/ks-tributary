package io.confluent.ps.streams.referenceapp.finance;

import avro.shaded.com.google.common.collect.ImmutableList;
import io.confluent.ps.streams.referenceapp.finance.channels.MockChannels;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.InstrumentId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetId;
import io.confluent.ps.streams.referenceapp.finance.model.avro.idlmodel.SnapshotSetKeys;
import io.confluent.ps.streams.referenceapp.finance.topologies.SnapshotTopologySettings;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.time.Duration.ofDays;

@Singleton // Test Data channels hold state, so must ensure only one version exists for a test run
public class TestData {

  @Inject
  public TestData(SnapshotTopologySettings settings) {
    baseTimeMinusWindowSize = baseTime.minus(settings.getWindowSize()); // i.e. the first window that will have closed given our base time
  }

  public static final String SYNTHETIC_KEY = "SYNTHETIC";

  // time
  protected ZoneId zone = ZoneOffset.UTC;

  public Instant baseTime = Instant.parse("2019-02-01T07:00:00Z");
  protected Instant baseTimeMinusWindowSize;
  public Instant snapTime = baseTime.plus(ofDays(1)); // default snap time being requested

  protected DateTimeFormatter formatter =
          DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
                  .withLocale(Locale.UK)
                  .withZone(ZoneId.systemDefault());

  // test data
  protected String appleInstrumentOne = "AAPL";
  protected InstrumentId appleInstrumentOneId = new InstrumentId(appleInstrumentOne);
  protected String firstTick20 = "20";
  protected String secondTick50 = "50";
  protected String thirdTick10 = "10";
  protected String fourthTick12 = "12";
  protected String dummyTickValue = "88";
  protected Instant firstTickTimeStamp = baseTime;

  public String googleInstrumentTwo = "GOOGL";
  public InstrumentId googleInstrumentThreeId = new InstrumentId(googleInstrumentTwo);

  String starbucksInstrumentThree = "STARBUCKS";
  protected InstrumentId starbucksInstrumentThreeId = new InstrumentId(starbucksInstrumentThree);
  protected String instrumentThreeTickOne490 = "490";
  protected String instrumentThreeTickTwo400 = "400";

  // snap set config data
  public final SnapshotSetKeys snapSetConfigOne = new SnapshotSetKeys(new SnapshotSetId("1"), ImmutableList.of(appleInstrumentOneId));
  public final SnapshotSetKeys snapSetConfigTwo = new SnapshotSetKeys(new SnapshotSetId("2"), ImmutableList.of(appleInstrumentOneId, starbucksInstrumentThreeId));

  // channels set 1 simple
  public MockChannels channelOneSimple = new MockChannels("1", "Simple");
  public MockChannels channelTwoSimple = new MockChannels("2", "Simple");
  public List<MockChannels> channelsListSimple = ImmutableList.of(channelOneSimple, channelTwoSimple);

  // channels set 2 sharded
  public MockChannels channelOneSharded = new MockChannels("1", "Sharded");
  public MockChannels channelTwoSharded = new MockChannels("2", "Sharded");
  public List<MockChannels> channelsListSharded = ImmutableList.of(channelOneSharded, channelTwoSharded);

  // channels set 3 latest
  public MockChannels channelThreeIndividualKey = new MockChannels("3", "Individual");

  public MockChannels channelFourLateSub = new MockChannels("4", "LateSub").toBuilder().snapEndTimeSubscribedTo(Optional.of(snapTime)).build();

  public MockChannels channelFiveDataArrivesAfterWindowClose = new MockChannels("5", "AfterClose").toBuilder()
          .snapEndTimeSubscribedTo(Optional.of(snapTime))
          .isNew(false) // fake it to not send the aggregate - for testing. Prod we will always send the bootstrap aggregate, even if late arriving data triggers it
          .build();


}
