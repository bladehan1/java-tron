package org.tron.core.db2.checkpoint.common;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.tron.core.db2.checkpoint.common.materialize.CheckpointWriteBatches;

public class CheckpointWriteBatchesTest {

  @Test
  public void splitsAtBoundariesAndKeepsOversizedSingleMutation() {
    List<Integer> input = Arrays.asList(2, 3, 5, 8);
    List<List<Integer>> batches = CheckpointWriteBatches.split(input, value -> value, 5);
    assertEquals(Arrays.asList(Arrays.asList(2, 3), Collections.singletonList(5),
        Collections.singletonList(8)), batches);
    assertEquals(input, batches.stream().flatMap(List::stream).collect(Collectors.toList()));
  }

  @Test
  public void zeroLimitAndEmptyInputKeepSimplePaths() {
    assertEquals(Collections.singletonList(Arrays.asList(1, 2, 3)),
        CheckpointWriteBatches.split(Arrays.asList(1, 2, 3), value -> value, 0));
    assertEquals(Collections.emptyList(),
        CheckpointWriteBatches.split(Collections.emptyList(), value -> 0, 1));
  }
}
