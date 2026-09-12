package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertEquals;

import io.prometheus.client.CollectorRegistry;
import org.junit.Test;
import org.tron.common.parameter.CommonParameter;

public class PathStateAttributionMetricsTest {
  @Test
  public void gatesExportAndPreservesZeroSamplesAndParticipantUnits() {
    boolean previousMetrics = CommonParameter.getInstance().isMetricsPrometheusEnable();
    String previousAttribution = System.getProperty("tron.pathstate.attribution");
    String prefix = "tron_pathstate_prepared_operation_";
    String[] labels = {"store", "operation"};
    String[] values = {"4", "decode"};
    try {
      System.setProperty("tron.pathstate.attribution", "true");
      CommonParameter.getInstance().setMetricsPrometheusEnable(false);
      double before = sample(prefix + "calls_total", labels, values);
      PathStateAttributionMetrics.operation(4, "decode", 5, 1, 500);
      assertEquals(before, sample(prefix + "calls_total", labels, values), 0);
      CommonParameter.getInstance().setMetricsPrometheusEnable(true);
      System.setProperty("tron.pathstate.attribution", "false");
      PathStateAttributionMetrics.operation(4, "decode", 5, 1, 500);
      assertEquals(before, sample(prefix + "calls_total", labels, values), 0);
      System.setProperty("tron.pathstate.attribution", "true");
      double samples = sample(prefix + "samples_total", labels, values);
      PathStateAttributionMetrics.operation(4, "decode", 5, 0, 0);
      assertEquals(before + 5, sample(prefix + "calls_total", labels, values), 0);
      assertEquals(samples, sample(prefix + "samples_total", labels, values), 0);
      for (int storeId : new int[]{5, 22}) {
        String[] other = {Integer.toString(storeId), "decode"};
        double previous = sample(prefix + "calls_total", labels, other);
        PathStateAttributionMetrics.operation(storeId, "decode", storeId, 1, 1000);
        assertEquals(previous + storeId, sample(prefix + "calls_total", labels, other), 0);
        assertEquals(before + 5, sample(prefix + "calls_total", labels, values), 0);
      }
      String[] participantLabels = {"store", "stage"};
      String[] participantValues = {"4", "work"};
      String histogram = "tron_pathstate_prepared_participant_seconds";
      double count = sample(histogram + "_count", participantLabels, participantValues);
      double seconds = sample(histogram + "_sum", participantLabels, participantValues);
      PathStateAttributionMetrics.participant(4, 2000000, 1200000000);
      assertEquals(count + 1,
          sample(histogram + "_count", participantLabels, participantValues), 0);
      assertEquals(seconds + 1.2,
          sample(histogram + "_sum", participantLabels, participantValues), 1e-9);
    } finally {
      CommonParameter.getInstance().setMetricsPrometheusEnable(previousMetrics);
      if (previousAttribution == null) {
        System.clearProperty("tron.pathstate.attribution");
      } else {
        System.setProperty("tron.pathstate.attribution", previousAttribution);
      }
    }
  }

  private static double sample(String name, String[] labels, String[] values) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(name, labels, values);
    return value == null ? 0 : value;
  }
}
