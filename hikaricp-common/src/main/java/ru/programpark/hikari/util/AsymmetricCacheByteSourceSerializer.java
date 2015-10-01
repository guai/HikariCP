package ru.programpark.hikari.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import lombok.SneakyThrows;

class AsymmetricCacheByteSourceSerializer extends Serializer
{
   @Override
   @SneakyThrows
   public void write(Kryo kryo, Output output, Object o)
   {
      byte[] bytea = ByteStreams.toByteArray(((CacheByteSource) o).openStream());
      output.writeInt(bytea.length);
      output.write(bytea);
   }

   @Override
   @SneakyThrows
   public Object read(Kryo kryo, Input input, Class aClass)
   {
      int len = input.readInt();
      byte[] bytea = new byte[len];
      input.read(bytea);
      ByteSource byteSource = ByteSource.wrap(bytea);
      return byteSource.openStream();

   }
}
