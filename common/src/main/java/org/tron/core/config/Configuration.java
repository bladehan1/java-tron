/*
 * Copyright (c) [2016] [ <ether.camp> ]
 * This file is part of the ethereumJ library.
 *
 * The ethereumJ library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ethereumJ library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the ethereumJ library. If not, see <http://www.gnu.org/licenses/>.
 */

package org.tron.core.config;

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import lombok.extern.slf4j.Slf4j;

@Slf4j(topic = "app")
public class Configuration {

  private static com.typesafe.config.Config config;

  /**
   * Get configuration by a given path.
   *
   * @param confFileName path to configuration file
   * @return loaded configuration
   */
  public static com.typesafe.config.Config getByFileName(
      final String confFileName) {
    if (isBlank(confFileName)) {
      throw new IllegalArgumentException(
          "Configuration path is required!");
    }
    File confFile = new File(confFileName);
    resolveConfigFile(confFileName, confFile);
    return config;
  }

  /**
   * Load the node config and apply a restricted RocksDB benchmark profile.
   *
   * <p>The profile may only provide {@code rocksdb-profile.settings}. Those values override
   * {@code storage.dbSettings}; all other node settings continue to come from the base config.
   * This keeps benchmark profiles reusable without allowing an experiment file to accidentally
   * change network, witness, or output-directory settings.</p>
   *
   * @param confFileName base node config
   * @param rocksDbProfileFile optional RocksDB profile file
   * @return merged config
   */
  public static com.typesafe.config.Config getByFileName(
      final String confFileName, final String rocksDbProfileFile) {
    com.typesafe.config.Config base = getByFileName(confFileName);
    if (isBlank(rocksDbProfileFile)) {
      return base;
    }

    File profileFile = new File(rocksDbProfileFile);
    if (!profileFile.isFile()) {
      throw new IllegalArgumentException(
          "RocksDB profile path is required! No Such file " + rocksDbProfileFile);
    }
    com.typesafe.config.Config profile = ConfigFactory.parseFile(profileFile).resolve();
    if (!profile.hasPath("rocksdb-profile.name")
        || !profile.hasPath("rocksdb-profile.mode")
        || !profile.hasPath("rocksdb-profile.settings")) {
      throw new IllegalArgumentException(
          "RocksDB profile must define rocksdb-profile.name, mode, and settings");
    }

    String profileName = profile.getString("rocksdb-profile.name");
    String profileMode = profile.getString("rocksdb-profile.mode");
    if (!profileName.matches("[A-Za-z0-9._-]+")) {
      throw new IllegalArgumentException(
          "RocksDB profile name may only contain letters, digits, dot, underscore, or dash");
    }
    if (!"E1".equals(profileMode) && !"E2".equals(profileMode)) {
      throw new IllegalArgumentException("RocksDB profile mode must be E1 or E2");
    }

    com.typesafe.config.Config profileSettings = profile.getConfig("rocksdb-profile.settings");
    if (!profileSettings.hasPath("useLegacyOptions")) {
      profileSettings = profileSettings.withValue("useLegacyOptions",
          ConfigValueFactory.fromAnyRef(false));
    }
    com.typesafe.config.Config settings = profileSettings
        .withValue("benchmarkProfile",
            ConfigValueFactory.fromAnyRef(profileName))
        .withValue("benchmarkMode",
            ConfigValueFactory.fromAnyRef(profileMode))
        .withFallback(base.getConfig("storage.dbSettings"));
    config = base.withValue("storage.dbSettings", settings.root()).resolve();
    return config;
  }

  private static void resolveConfigFile(String fileName, File confFile) {
    if (confFile.exists()) {
      config = ConfigFactory.parseFile(confFile)
          .withFallback(ConfigFactory.defaultReference());
    } else if (Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)
        != null) {
      config = ConfigFactory.load(fileName);
    } else {
      throw new IllegalArgumentException(
          "Configuration path is required! No Such file " + fileName);
    }
  }
}
