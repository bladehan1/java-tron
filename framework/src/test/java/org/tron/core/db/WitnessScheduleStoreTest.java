package org.tron.core.db;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tron.common.BaseTest;
import org.tron.common.TestConstants;
import org.tron.common.utils.ByteArray;
import org.tron.core.config.args.Args;
import org.tron.core.db2.ISession;
import org.tron.core.db2.core.Chainbase.Cursor;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.store.WitnessScheduleStore;

public class WitnessScheduleStoreTest extends BaseTest {

  @Resource
  private WitnessScheduleStore witnessScheduleStore;
  private static final String KEY1 = "41f08012b4881c320eb40b80f1228731898824e09d";
  private static final String KEY2 = "41df309fef25b311e7895562bd9e11aab2a58816d2";
  private static final String KEY3 = "41F8C7ACC4C08CF36CA08FC2A61B1F5A7C8DEA7BEC";

  private static final String CURRENT_KEY1 = "411D7ABA13EA199A63D1647E58E39C16A9BB9DA689";
  private static final String CURRENT_KEY2 = "410694981B116304ED21E05896FB16A6BC2E91C92C";
  private static final String CURRENT_KEY3 = "411155D10415FAC16A8F4CB2F382CE0E0F0A7E64CC";

  private static List<ByteString> witnessAddresses;
  private static List<ByteString> currentShuffledWitnesses;


  static {
    Args.setParam(new String[]{"-d", dbPath()}, TestConstants.TEST_CONF);
  }

  @Before
  public void init() {
    witnessAddresses = Arrays.asList(
            getByteString(KEY1),
            getByteString(KEY2),
            getByteString(KEY3)
    );

    currentShuffledWitnesses = Arrays.asList(
            getByteString(CURRENT_KEY1),
            getByteString(CURRENT_KEY2),
            getByteString(CURRENT_KEY3)
    );
  }

  private ByteString getByteString(String address) {
    return ByteString.copyFrom(
            ByteArray.fromHexString(address));
  }

  @Test
  public void tetSaveActiveWitnesses() {
    witnessScheduleStore.saveActiveWitnesses(witnessAddresses);
    List<ByteString> activeWitnesses = witnessScheduleStore.getActiveWitnesses();
    Assert.assertNotNull(activeWitnesses);
    Assert.assertEquals(3, activeWitnesses.size());
    ByteString firstWitness = activeWitnesses.get(0);
    Assert.assertEquals(getByteString(KEY1), firstWitness);
  }

  @Test
  public void testGetActiveWitnesses() {
    witnessScheduleStore.saveActiveWitnesses(witnessAddresses);
    List<ByteString> activeWitnesses = witnessScheduleStore.getActiveWitnesses();
    Assert.assertNotNull(activeWitnesses);
    Assert.assertEquals(3, activeWitnesses.size());
    ByteString firstWitness = activeWitnesses.get(0);
    ByteString secondWitness = activeWitnesses.get(1);
    ByteString thirdWitness = activeWitnesses.get(2);
    Assert.assertEquals(getByteString(KEY1), firstWitness);
    Assert.assertEquals(getByteString(KEY2), secondWitness);
    Assert.assertEquals(getByteString(KEY3), thirdWitness);
  }

  @Test
  public void testSaveCurrentShuffledWitnesses() {
    witnessScheduleStore.saveCurrentShuffledWitnesses(currentShuffledWitnesses);
    List<ByteString> currentWitnesses = witnessScheduleStore.getCurrentShuffledWitnesses();
    Assert.assertNotNull(currentWitnesses);
    Assert.assertEquals(3, currentWitnesses.size());
    ByteString firstWitness = currentWitnesses.get(0);
    Assert.assertEquals(getByteString(CURRENT_KEY1), firstWitness);
  }

  @Test
  public void GetCurrentShuffledWitnesses() {
    witnessScheduleStore.saveCurrentShuffledWitnesses(currentShuffledWitnesses);
    List<ByteString> currentWitnesses = witnessScheduleStore.getCurrentShuffledWitnesses();
    Assert.assertNotNull(currentWitnesses);
    Assert.assertEquals(3, currentWitnesses.size());
    ByteString firstWitness = currentWitnesses.get(0);
    ByteString secondWitness = currentWitnesses.get(1);
    ByteString thirdWitness = currentWitnesses.get(2);
    Assert.assertEquals(getByteString(CURRENT_KEY1), firstWitness);
    Assert.assertEquals(getByteString(CURRENT_KEY2), secondWitness);
    Assert.assertEquals(getByteString(CURRENT_KEY3), thirdWitness);
  }

  @Test
  public void testReadWitnessScheduleFromSolidity() {
    DynamicPropertiesStore properties = chainBaseManager.getDynamicPropertiesStore();
    long originalNext = properties.getNextMaintenanceTime();
    long originalInterval = properties.getMaintenanceTimeInterval();
    Cursor cursor = witnessScheduleStore.getRevokingDB().getCursor();
    long boundary = 216_000_000;
    long interval = 21_600_000;
    witnessScheduleStore.saveActiveWitnesses(witnessAddresses);
    properties.saveNextMaintenanceTime(boundary);
    properties.saveMaintenanceTimeInterval(interval);
    try (ISession session = dbManager.getRevokingStore().buildSession()) {
      witnessScheduleStore.saveActiveWitnesses(
          Collections.singletonList(getByteString(CURRENT_KEY1)));
      properties.saveNextMaintenanceTime(boundary + interval);
      properties.saveMaintenanceTimeInterval(interval * 2);
      Assert.assertEquals(1, witnessScheduleStore.getActiveWitnesses().size());
      Assert.assertEquals(witnessAddresses, witnessScheduleStore.getActiveWitnessesFromSolidity());
      Assert.assertEquals(boundary, properties.getNextMaintenanceTimeFromSolidity());
      Assert.assertEquals(interval, properties.getMaintenanceTimeIntervalFromSolidity());
      Assert.assertEquals(cursor, witnessScheduleStore.getRevokingDB().getCursor());
    } finally {
      properties.saveNextMaintenanceTime(originalNext);
      properties.saveMaintenanceTimeInterval(originalInterval);
    }
    Assert.assertEquals(witnessAddresses, witnessScheduleStore.getActiveWitnesses());
  }

}
