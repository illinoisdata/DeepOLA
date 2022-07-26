package de.tuda.progressive.db.standalone;

import de.tuda.progressive.db.ProgressiveDbServer;
import java.io.File;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    final File configurationFile = new File(args.length > 0 ? args[0] : "progressive-db.conf");
    if (!configurationFile.exists()) {
      throw new IllegalArgumentException("configuration does not exist: " + configurationFile);
    }

    final Configurations configs = new Configurations();
    final Configuration config = configs.properties(configurationFile);

    final ProgressiveDbServer server = new ProgressiveDbServer.Builder()
        .source(
            config.getString("source.url"),
            config.getString("source.user", null),
            config.getString("source.password", null)
        )
        .meta(
            config.getString("meta.url"),
            config.getString("meta.user", null),
            config.getString("meta.password", null)
        )
        .tmp(
            config.getString("tmp.url"),
            config.getString("tmp.user", null),
            config.getString("tmp.password", null)
        )
        .port(config.getInt("port", 9000))
        .chunkSize(config.getInt("chunkSize", -1))
        .build();

    server.start();
  }
}
