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
    
    public Result(Status s, ConditionalMutation m) {
      this.status = s;
      this.mutation = m;
    }
    
    public Status getStatus() {
      return status;
    }
    
    public ConditionalMutation getMutation() {
      return mutation;
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

}
