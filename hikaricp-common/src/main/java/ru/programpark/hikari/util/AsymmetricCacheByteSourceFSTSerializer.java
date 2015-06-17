package ru.programpark.hikari.util;

import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.FSTObjectSerializer;

import java.io.IOException;

class AsymmetricCacheByteSourceFSTSerializer implements FSTObjectSerializer
{
   @Override
   public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition)
         throws IOException
   {
      byte[] bytea = ByteStreams.toByteArray(((CacheByteSource) toWrite).openStream());
      out.writeInt(bytea.length);
      out.write(bytea);
   }

   @Override
   public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy)
         throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException
   {
   }

   @Override
   public boolean willHandleClass(Class cl)
   {
      return true;
   }

   @Override
   public boolean alwaysCopy()
   {
      return true;
   }

   @Override
   public Object instantiate(Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee,
                                       int streamPositioin) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
   {
      int len = in.readInt();
      byte[] bytea = new byte[len];
      in.read(bytea);
      ByteSource byteSource = ByteSource.wrap(bytea);
      return byteSource.openStream();
   }
}
