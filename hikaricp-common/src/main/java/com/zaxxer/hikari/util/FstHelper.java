package com.zaxxer.hikari.util;

import org.nustaq.serialization.FSTConfiguration;

import java.sql.Timestamp;

public class FstHelper
{
   public static final FSTConfiguration fst;

   static {
      fst = FSTConfiguration.createDefaultConfiguration();
      fst.registerClass(Timestamp.class, CacheByteSource.class, CacheCharSource.class);
   }

}
