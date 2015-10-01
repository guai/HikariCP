package ru.programpark.hikari.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.CharSource;
import lombok.SneakyThrows;

class AsymmetricCacheCharSourceSerializer extends Serializer
{
   @Override
   @SneakyThrows
   public void write(Kryo kryo, Output output, Object o)
   {
      output.writeString(((CacheCharSource) o).read());
   }

   @Override
   @SneakyThrows
   public Object read(Kryo kryo, Input input, Class aClass)
   {
      return CharSource.wrap(input.readString()).openStream();
   }
}
