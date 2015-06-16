package ru.programpark.hikari.proxy;

import java.io.Serializable;
import java.util.Arrays;

public class Record implements Serializable
{

   static final long serialVersionUID = 1L;

   final int conncetionId;
   int statementId;
   final Class clazz;
   final String methodName;
   final Object[] args;

   public Record(final int conncetionId, final int statementId, final Class clazz, final String methodName, final Object[] args)
   {
      this.conncetionId = conncetionId;
      this.statementId = statementId;
      this.clazz = clazz;
      this.methodName = methodName;
      this.args = args;
   }

   @Override
   public String toString()
   {
      return "Record{" +
              "conncetionId=" + conncetionId +
              ", statementId=" + statementId +
              ", clazz=" + clazz +
              ", methodName='" + methodName + '\'' +
              ", args=" + Arrays.toString(args) +
              '}';
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Record record = (Record) o;

      if (conncetionId != record.conncetionId) return false;
      if (statementId != record.statementId) return false;
      if (!clazz.equals(record.clazz)) return false;
      if (!methodName.equals(record.methodName)) return false;
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      return Arrays.equals(args, record.args);

   }

   @Override
   public int hashCode()
   {
      int result = conncetionId;
      result = 31 * result + statementId;
      result = 31 * result + clazz.hashCode();
      result = 31 * result + methodName.hashCode();
      result = 31 * result + Arrays.hashCode(args);
      return result;
   }

}
