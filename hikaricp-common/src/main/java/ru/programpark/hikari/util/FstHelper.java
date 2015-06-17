package ru.programpark.hikari.util;

import org.nustaq.serialization.FSTConfiguration;

import java.sql.Timestamp;

public class FSTHelper
{
   public static final FSTConfiguration FST;

   static {
      FST = FSTConfiguration.createDefaultConfiguration();

      FST.registerSerializer(CacheByteSource.class, new AsymmetricCacheByteSourceFSTSerializer(), false);
      FST.registerSerializer(CacheCharSource.class, new AsymmetricCacheCharSourceFSTSerializer(), false);

      FST.registerClass(Timestamp.class, CacheByteSource.class, CacheCharSource.class);
   }
}
