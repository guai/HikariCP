package com.zaxxer.hikari.util;

import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import lombok.SneakyThrows;

import java.io.*;

public class CacheByteSource extends ByteSource implements Externalizable
{

   private ByteSource byteSource;

   @SneakyThrows
   public CacheByteSource(final InputStream inputStream, long size)
   {
      byteSource = ByteSource.wrap(ByteStreams.toByteArray(ByteStreams.limit(inputStream, size)));
   }

   @SneakyThrows
   public CacheByteSource(final InputStream inputStream)
   {
      byteSource = ByteSource.wrap(ByteStreams.toByteArray(inputStream));
   }

   @Override
   public InputStream openStream() throws IOException
   {
      return byteSource.openStream();
   }

   @Deprecated
   public CacheByteSource()
   {
   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException
   {
      byte[] bytea = ByteStreams.toByteArray(byteSource.openStream());
      out.writeInt(bytea.length);
      out.write(bytea);
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      int len = in.readInt();
      byte[] bytea = new byte[len];
      in.read(bytea);
      byteSource = ByteSource.wrap(bytea);
   }
}
