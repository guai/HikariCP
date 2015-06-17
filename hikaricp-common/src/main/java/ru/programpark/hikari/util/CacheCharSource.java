package ru.programpark.hikari.util;

import com.google.common.io.CharSource;
import com.google.common.io.CharStreams;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.Reader;

public class CacheCharSource extends CharSource
{
   private CharSource charSource;

   @SneakyThrows
   public CacheCharSource(final Reader reader, int size)
   {
      String string = CharStreams.toString(reader);
      if (string.length() > size)
         string = string.substring(0, size);
      charSource = CharSource.wrap(string);
   }

   @SneakyThrows
   public CacheCharSource(final Reader reader)
   {
      charSource = CharSource.wrap(CharStreams.toString(reader));
   }

   @Override
   public Reader openStream() throws IOException
   {
      return charSource.openStream();
   }
}
