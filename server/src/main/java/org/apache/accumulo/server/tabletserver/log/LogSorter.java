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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.log4j.Logger;

/**
 * 
 */
public class LogSorter {
  
  private static final Logger log = Logger.getLogger(LogSorter.class);
  FileSystem fs;
  AccumuloConfiguration conf;
  
  class Work implements Runnable {
    final String name;
    FSDataInputStream input;
    final String destPath;
    long bytesCopied = -1;
    
    synchronized long getBytesCopied() throws IOException {
      return input == null ? bytesCopied : input.getPos();
    }
    
    Work(String name, FSDataInputStream input, String destPath) {
      this.name = name;
      this.input = input;
      this.destPath = destPath;
    }
    synchronized boolean finished() {
      return input == null;
    }
    public void run() {
      String formerThreadName = Thread.currentThread().getName();
      try {
        final long bufferSize = conf.getMemoryInBytes(Property.TSERV_SORT_BUFFER_SIZE);
        Thread.currentThread().setName("Sorting " + name + " for recovery");
        int part = 0;
        while (true) {
          final ArrayList<Pair<LogFileKey, LogFileValue>> buffer = new ArrayList<Pair<LogFileKey, LogFileValue>>();
          try {
            long start = input.getPos();
            while (input.getPos() - start < bufferSize) {
              LogFileKey key = new LogFileKey();
              LogFileValue value = new LogFileValue();
              key.readFields(input);
              value.readFields(input);
              buffer.add(new Pair<LogFileKey, LogFileValue>(key, value));
            }
            writeBuffer(buffer, part++);
            buffer.clear();
          } catch (EOFException ex) {
            writeBuffer(buffer, part++);
            break;
          }
        }
        fs.create(new Path(destPath, "finished")).close();
        log.info("Log copy/sort of " + name + " complete");
      } catch (Throwable t) {
        try {
          fs.create(new Path(destPath, "failed")).close();
        } catch (IOException e) {
          log.error("Error creating failed flag file " + name, e);
        }
        log.error(t, t);
      } finally {
        Thread.currentThread().setName(formerThreadName);
        try {
          close();
        } catch (IOException e) {
          log.error("Error during cleanup sort/copy " + name, e);
        }
      }
    }
    
    private void writeBuffer(ArrayList<Pair<LogFileKey,LogFileValue>> buffer, int part) throws IOException {
      String path = destPath + String.format("/part-r-%05d", part++);
      MapFile.Writer output = new MapFile.Writer(fs.getConf(), fs, path, LogFileKey.class, LogFileValue.class);
      try {
        Collections.sort(buffer, new Comparator<Pair<LogFileKey,LogFileValue>>() {
          @Override
          public int compare(Pair<LogFileKey,LogFileValue> o1, Pair<LogFileKey,LogFileValue> o2) {
            return o1.getFirst().compareTo(o2.getFirst());
          }
        });
        for (Pair<LogFileKey,LogFileValue> entry : buffer) {
          output.append(entry.getFirst(), entry.getSecond());
        }
      } finally {
        output.close();
      }
    }
    
    synchronized void close() throws IOException {
      bytesCopied = input.getPos();
      input.close();
      input = null;
    }
  };
  
  final ExecutorService threadPool;
  Map<String,Work> sorts = new ConcurrentHashMap<String,Work>();
  
  public LogSorter(FileSystem fs, AccumuloConfiguration conf) {
    this.fs = fs;
    this.conf = conf;
    int threadPoolSize = conf.getCount(Property.TSERV_RECOVERY_MAX_CONCURRENT);
    this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
  }
  
  public double sort(String src, String dest) throws IOException {
    synchronized (this) {
      Work work = sorts.get(src);
      if (work == null) {
        work = startSort(src, dest);
        sorts.put(src, work);
      } else {
        if (work.finished())
          sorts.remove(src);
      }
      long bytesCopied = work.getBytesCopied();
      long estimate = conf.getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE);
      return bytesCopied / ((double) estimate);
    }
  }
  
  private Work startSort(String src, String dest) throws IOException {
    log.info("Copying " + src + " to " + dest);
    Path srcPath = new Path(src);
    while (true) {
      try {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem dfs = (DistributedFileSystem) fs;
          dfs.recoverLease(srcPath);
          log.debug("recovered lease on " + srcPath);
        } else {
          fs.append(srcPath).close();
          log.debug("successfully appended to " + srcPath);
        }
        break;
      } catch (IOException e) {
        log.debug("error recovering lease on " + srcPath, e);
        UtilWaitThread.sleep(1000);
        log.debug("retrying lease recovery on " + srcPath);
      }
    }
    Work work = new Work(srcPath.getName(), fs.open(srcPath), dest);
    threadPool.execute(work);
    return work;
  }
}
