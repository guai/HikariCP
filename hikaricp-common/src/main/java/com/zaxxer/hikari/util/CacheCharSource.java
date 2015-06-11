package com.zaxxer.hikari.util;

import com.google.common.io.CharSource;
import com.google.common.io.CharStreams;
import lombok.SneakyThrows;

import java.io.*;
import java.nio.charset.Charset;

public class CacheCharSource extends CharSource implements Externalizable
{

   static final Charset utf8 = Charset.forName("UTF-8");
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

   @Deprecated
   public CacheCharSource()
   {
   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException
   {
      byte[] bytea = CharStreams.toString(charSource.openStream()).getBytes(utf8);
      out.writeInt(bytea.length);
      out.write(bytea);
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      int len = in.readInt();
      byte[] bytea = new byte[len];
      in.read(bytea);
      charSource = CharSource.wrap(new String(bytea, utf8));
   }
}
