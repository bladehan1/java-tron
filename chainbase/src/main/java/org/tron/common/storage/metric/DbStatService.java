package org.tron.common.storage.metric;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.Metrics;
import org.tron.core.db.common.DbSourceInter;
import org.tron.core.db2.common.DB;

@Slf4j(topic = "metrics")
@Component
public class DbStatService {
  private final String esName = "db-stats";
  private final ScheduledExecutorService statExecutor  =
      ExecutorServiceManager.newSingleThreadScheduledExecutor(esName);

  public  void register(DB<byte[], byte[]> db) {
    if (Metrics.databaseEnabled()) {
      statExecutor.scheduleWithFixedDelay(db::stat, 0, statIntervalSeconds(), TimeUnit.SECONDS);
    }
  }

  public  void register(DbSourceInter<byte[]> db) {
    if (Metrics.databaseEnabled()) {
      statExecutor.scheduleWithFixedDelay(db::stat, 0, statIntervalSeconds(), TimeUnit.SECONDS);
    }
  }

  public void shutdown() {
    if (Metrics.databaseEnabled()) {
      ExecutorServiceManager.shutdownAndAwaitTermination(statExecutor, esName);
    }
  }

  private int statIntervalSeconds() {
    return CommonParameter.getInstance().getMetricsPrometheusDatabaseStatIntervalSeconds();
  }
}
