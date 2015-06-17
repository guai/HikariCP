package ru.programpark.hikari.util;

import com.google.common.io.CharSource;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.FSTObjectSerializer;

import java.io.IOException;

class AsymmetricCacheCharSourceFSTSerializer implements FSTObjectSerializer
{
   @Override
   public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition)
         throws IOException
   {
      out.writeUTF(((CacheCharSource) toWrite).read());
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
      return CharSource.wrap(in.readUTF()).openStream();
   }
}
