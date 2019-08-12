package com.zaxxer.hikari.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Objects;
import com.google.common.collect.ObjectArrays;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Arrays;


public class Marshaller {
   // todo use pool
   private static final ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
      Kryo kryo = new Kryo();
      kryo.register(CacheByteSource.class, new AsymmetricCacheByteSourceSerializer());
      kryo.register(CacheCharSource.class, new AsymmetricCacheCharSourceSerializer());

      kryo.register(Timestamp.class);
      kryo.register(InputStream.class);

      return kryo;
   });

   public static Object[] emptyObjectArray = new Object[]{};

   public static byte[] toBytes(Object object) {
      Output output = new Output(256, -1);
      kryos.get().writeObject(output, object);
      byte[] result = output.toBytes();
      output.close();
      return result;
   }

   public static Class[] sigFromBytes(byte[] bytes) {
      Input input = new Input(bytes);
      Class[] result = kryos.get().readObject(input, Class[].class);
      input.close();
      return result;
   }

   public static Object[] fromBytes(byte[] bytes) {
      if(bytes == null) return emptyObjectArray;
      Input input = new Input(bytes);
      Object[] result = kryos.get().readObject(input, Object[].class);
      input.close();
      return result;
   }
}
