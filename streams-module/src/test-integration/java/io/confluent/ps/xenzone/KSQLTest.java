package io.confluent.ps.xenzone;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static io.confluent.ksql.test.tools.KsqlTestingTool.main;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class KSQLTest {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  public void setUpStreams() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }


  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);

    assertThat(errContent.toString()).isBlank();
  }

  @SneakyThrows
  void run(String rootbase, String in, String out, String sql) {
    String root = "src/test-integration/resources/ksql/" + rootbase + "/";
    main(new String[]{"--input-file", root + in, "--sql-file", root + sql, "--output-file", root + out});
  }

  @SneakyThrows
  @Test
  void ksql() {
    run("kpi", "input.json", "output.json", "kpi.sql");
  }

  @SneakyThrows
  @Test
  void example() {
    run("example", "input.json", "output.json", "query.sql");
  }

}
