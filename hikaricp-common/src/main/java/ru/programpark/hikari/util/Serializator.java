package ru.programpark.hikari.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.sql.Timestamp;

public class Serializator
{
   public static final Kryo kryo;

   static {
      kryo = new Kryo();

      kryo.register(CacheByteSource.class, new AsymmetricCacheByteSourceSerializer());
      kryo.register(CacheCharSource.class, new AsymmetricCacheCharSourceSerializer());

      kryo.register(Timestamp.class);
   }

   public static byte[] toBytes(Object[] objects) {
      Output output = new Output(256, -1);
      kryo.writeObject(output, objects);
      byte[] result = output.toBytes();
      output.close();
      return result;
   }

   public static Object[] fromBytes(byte[] bytes) {
      Input input = new Input(bytes);
      Object[] result = kryo.readObject(input, Object[].class);
      input.close();
      return result;
   }
}
