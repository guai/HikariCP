package com.zaxxer.hikari.pool;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

class Record implements Serializable {

   static final long serialVersionUID = 1L;

   final int connectionId;
   int statementId;
   final char classId;
   final String method;
   final Object[] args;

   Record(final int connectionId, final int statementId, final char classId, final String method, final Object[] args) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.classId = classId;
      this.method = method;
      this.args = args;
   }

   @Override
   public String toString() {
      return "Record{" +
         "connectionId=" + connectionId +
         ", statementId=" + statementId +
         ", classId=" + classId +
         ", method=" + method +
         ", args=" + Arrays.toString(args) +
         '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Record record = (Record) o;
      return connectionId == record.connectionId &&
         statementId == record.statementId &&
         classId == record.classId &&
         method.equals(record.method) &&
         Arrays.equals(args, record.args);
   }

   @Override
   public int hashCode() {
      int result = Objects.hash(connectionId, statementId, classId, method);
      result = 31 * result + Arrays.hashCode(args);
      return result;
   }
}
