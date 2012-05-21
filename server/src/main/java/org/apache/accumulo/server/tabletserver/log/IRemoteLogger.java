/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.tabletserver.log;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.LogCopyInfo;
import org.apache.accumulo.core.tabletserver.thrift.LoggerClosedException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchLogIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.server.tabletserver.log.RemoteLogger.LoggerOperation;
import org.apache.thrift.TException;

/**
 * 
 */
public interface IRemoteLogger {
  
  public abstract boolean equals(Object obj);
  
  public abstract int hashCode();
  
  public abstract String toString();
  
  public abstract String getLogger();
  
  public abstract String getFileName();
  
  public abstract void close() throws NoSuchLogIDException, LoggerClosedException, TException;
  
  public abstract void defineTablet(int seq, int tid, KeyExtent tablet) throws NoSuchLogIDException, LoggerClosedException, TException;
  
  public abstract LoggerOperation log(int seq, int tid, Mutation mutation) throws NoSuchLogIDException, LoggerClosedException, TException;
  
  public abstract LoggerOperation logManyTablets(List<TabletMutations> mutations) throws NoSuchLogIDException, LoggerClosedException, TException;
  
  public abstract void minorCompactionFinished(int seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException;
  
  public abstract void minorCompactionStarted(int seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException;
  
  public abstract LogCopyInfo startCopy(String name, String fullyQualifiedFileName) throws ThriftSecurityException, TException;
  
  public abstract List<String> getClosedLogs() throws ThriftSecurityException, TException;
  
  public abstract void removeFile(List<String> files) throws ThriftSecurityException, TException;
  
  public abstract void open() throws IOException;

}