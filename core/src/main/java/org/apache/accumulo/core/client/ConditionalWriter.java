/*
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

package org.apache.accumulo.core.client;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.ConditionalMutation;

/**
 * @since 1.6.0
 */
public interface ConditionalWriter {
  public static class Result {
    
    private Status status;
    private ConditionalMutation mutation;
    private String server;
    private Exception exception;
    
    public Result(Status s, ConditionalMutation m, String server) {
      this.status = s;
      this.mutation = m;
      this.server = server;
    }
    
    public Result(Exception e, ConditionalMutation cm, String server) {
      this.exception = e;
      this.mutation = cm;
      this.server = server;
    }

    public Status getStatus() throws AccumuloException, AccumuloSecurityException {
      if (status == null) {
        if (exception instanceof AccumuloException)
          throw (AccumuloException) exception;
        if (exception instanceof AccumuloSecurityException)
          throw (AccumuloSecurityException) exception;
        if (exception instanceof RuntimeException)
          throw (RuntimeException) exception;
        else
          throw new AccumuloException(exception);
      }

      return status;
    }
    
    public ConditionalMutation getMutation() {
      return mutation;
    }
    
    /**
     * 
     * @return The server this mutation was sent to. Returns null if was not sent to a server.
     */
    public String getTabletServer() {
      return server;
    }
  }
  
  public static enum Status {
    /**
     * conditions were met and mutation was written
     */
    ACCEPTED,
    /**
     * conditions were not met and mutation was not written
     */
    REJECTED,
    /**
     * mutation violated a constraint and was not written
     */
    VIOLATED,
    /**
     * error occurred after mutation was sent to server, its unknown if the mutation was written
     */
    UNKNOWN,
    /**
     * A condition contained a column visibility that could never be seen
     */
    INVISIBLE_VISIBILITY,
    /**
     * nothing was done with this mutation, this is caused by previous mutations failing in some way like timing out
     */
    IGNORED
  }

  public abstract Iterator<Result> write(Iterator<ConditionalMutation> mutations);
  
  public abstract Result write(ConditionalMutation mutation);
  
  /**
   * This setting determines how long a scanner will automatically retry when a failure occurs. By default a scanner will retry forever.
   * 
   * Setting to zero or Long.MAX_VALUE and TimeUnit.MILLISECONDS means to retry forever.
   * 
   * @param timeOut
   * @param timeUnit
   *          determines how timeout is interpreted
   */
  public void setTimeout(long timeOut, TimeUnit timeUnit);
  
  /**
   * Returns the setting for how long a scanner will automatically retry when a failure occurs.
   * 
   * @return the timeout configured for this scanner
   */
  public long getTimeout(TimeUnit timeUnit);

  public void close();
}
