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
package org.apache.accumulo.server.logger;

import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.server.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.server.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.server.logger.LogEvents.MUTATION;
import static org.apache.accumulo.server.logger.LogEvents.OPEN;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.cloudtrace.instrument.Span;
import org.apache.accumulo.cloudtrace.instrument.Trace;
import org.apache.accumulo.cloudtrace.thrift.TInfo;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.LogCopyInfo;
import org.apache.accumulo.core.tabletserver.thrift.LogFile;
import org.apache.accumulo.core.tabletserver.thrift.MutationLogger;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchLogIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.logger.metrics.LogWriterMetrics;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.WritableName;
import org.apache.thrift.TException;


/**
 * Write log operations to open {@link org.apache.hadoop.io.SequenceFile}s referenced by log id's.
 */
class LogWriter implements MutationLogger.Iface {
  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(LogWriter.class);
  
  static {
    WritableName.setName(LogFileKey.class, Constants.OLD_PACKAGE_NAME + ".server.logger.LogFileKey");
    WritableName.setName(LogFileValue.class, Constants.OLD_PACKAGE_NAME + ".server.logger.LogFileValue");
  }
  
  static class LogWriteException extends RuntimeException {
    /**
         * 
         */
    private static final long serialVersionUID = 1L;
    
    public LogWriteException(Throwable why) {
      super(why);
    }
  }
  
  /**
   * Generate log ids.
   */
  private static final SecureRandom random = new SecureRandom();
  
  private final FileSystem fs;
  private final ExecutorService copyThreadPool;
  private final static Mutation empty[] = new Mutation[0];
  private boolean closed = false;
  
  private static class Logger {
    SequenceFile.Writer seq;
    FSDataOutputStream out;
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    
    Logger(Configuration conf, String path) throws IOException {
      FileSystem local = TraceFileSystem.wrap(FileSystem.getLocal(conf).getRaw());
      out = local.create(new Path(path));
      seq = SequenceFile.createWriter(conf, out, LogFileKey.class, LogFileValue.class, CompressionType.NONE, null);
    }
    
    void logEntry() throws IOException {
      try {
        long t1 = System.currentTimeMillis();
        seq.append(key, value);
        long t2 = System.currentTimeMillis();
        if (metrics.isEnabled())
          metrics.add(LogWriterMetrics.logAppend, (t2 - t1));
        out.flush();
        long t3 = System.currentTimeMillis();
        if (metrics.isEnabled())
          metrics.add(LogWriterMetrics.logFlush, (t3 - t2));
      } catch (IOException ioe) {
        if (metrics.isEnabled())
          metrics.add(LogWriterMetrics.logException, 0);
        throw ioe;
      }
    }
    
    void close() throws IOException {
      seq.close();
      out.close();
    }
  }
  
  /**
   * The current loggers in use.
   */
  private Map<Long,Logger> logs = new ConcurrentHashMap<Long,Logger>();
  
  /**
   * Map from filename to open log id.
   */
  private Map<String,Long> file2id = new ConcurrentHashMap<String,Long>();
  
  /**
   * Local directory where logs are created.
   */
  private final List<String> roots;
  private int nextRoot = 0;
  
  private final String instanceId;
  
  private LogArchiver logArchiver;
  
  // Metrics MBean
  private static LogWriterMetrics metrics = new LogWriterMetrics();
  
  private final AccumuloConfiguration acuConf;
  
  /**
   * 
   * @param fs
   *          The HDFS instance shared by master/tservers.
   * @param logDirectory
   *          The local directory to write the recovery logs.
   * @param instanceId
   *          The accumulo instance for which we are logging.
   */
  public LogWriter(AccumuloConfiguration acuConf, FileSystem fs, Collection<String> logDirectories, String instanceId, int threadPoolSize, boolean archive) {
    this.acuConf = acuConf;
    this.fs = fs;
    this.roots = new ArrayList<String>(logDirectories);
    this.instanceId = instanceId;
    this.copyThreadPool = Executors.newFixedThreadPool(threadPoolSize);
    try {
      this.logArchiver = new LogArchiver(acuConf, TraceFileSystem.wrap(FileSystem.getLocal(fs.getConf())), fs, new ArrayList<String>(logDirectories), archive);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    
    // Register the metrics MBean
    try {
      metrics.register();
    } catch (Exception e) {
      log.error("Exception registering MBean with MBean Server", e);
    }
  }
  
  @Override
  synchronized public void close(TInfo info, long id) throws NoSuchLogIDException {
    long t1 = System.currentTimeMillis();
    synchronized (logs) {
      Logger out = logs.remove(id);
      if (out == null) {
        throw new NoSuchLogIDException();
      }
      try {
        out.close();
      } catch (IOException ex) {
        log.error("IOException occurred closing file", ex);
      }
      // Iterative search: this is ok if the number of open files is small
      for (Entry<String,Long> entry : file2id.entrySet()) {
        if (entry.getValue().equals(id)) {
          file2id.remove(entry.getKey());
          long t2 = System.currentTimeMillis();
          if (metrics.isEnabled())
            metrics.add(LogWriterMetrics.close, (t2 - t1));
          return;
        }
      }
    }
    throw new RuntimeException("Unexpected failure to find a filename matching an id");
  }
  
  private final String findLocalFilename(String localLog) throws FileNotFoundException {
    for (String root : roots) {
      String path = root + "/" + localLog;
      if (new File(path).exists())
        return path;
    }
    throw new FileNotFoundException("No file " + localLog + " found in " + roots);
  }
  
  interface LogEntryReader {
    
    Pair<LogFileKey,LogFileValue> next() throws IOException;
    
    void close() throws IOException;

    long getPosition() throws IOException;
  }
  
  static abstract class BaseFileReader implements LogEntryReader {
    
    public Pair<LogFileKey,LogFileValue> next() throws IOException {
      LogFileKey key = new LogFileKey();
      LogFileValue value = new LogFileValue();
      fetchOne(key, value);
      return new Pair<LogFileKey,LogFileValue>(key, value);
    }
    
    abstract void fetchOne(LogFileKey key, LogFileValue value) throws IOException;

  }
  
  static class SequenceFileReader extends BaseFileReader {
    
    SequenceFile.Reader file;
    
    SequenceFileReader(SequenceFile.Reader file) {
      this.file = file;
    }
    
    @Override
    public long getPosition() throws IOException {
      return file.getPosition();
    }
    
    @Override
    public void close() throws IOException {
      file.close();
    }
    
    @Override
    void fetchOne(LogFileKey key, LogFileValue value) throws IOException {
      if (file.next(key, value))
        throw new EOFException("EOF");
    }
  }
  
  static class FileReader extends BaseFileReader {
    
    FSDataInputStream file;
    
    FileReader(FSDataInputStream file) {
      this.file = file;
    }
    
    public void close() throws IOException {
      file.close();
    }

    @Override
    public long getPosition() {
      try {
        return file.getPos();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    
    @Override
    void fetchOne(LogFileKey key, LogFileValue value) throws IOException {
      key.readFields(file);
      value.readFields(file);
    }
  }

  @Override
  public LogCopyInfo startCopy(TInfo info, AuthInfo credentials, final String localLog, final String fullyQualifiedFileName, final boolean sort) {
    log.info("Copying " + localLog + " to " + fullyQualifiedFileName);
    final long t1 = System.currentTimeMillis();
    try {
      Long id = file2id.get(localLog);
      if (id != null)
        close(info, id);
    } catch (NoSuchLogIDException e) {
      log.error("Unexpected error thrown", e);
      throw new RuntimeException(e);
    }
    LogEntryReader reader;
    File file;
    long result;
    try {
      try {
        file = new File(findLocalFilename(localLog));
        log.info(file.getAbsoluteFile().toString());
        result = file.length();
        FileSystem local = FileSystem.getLocal(fs.getConf()).getRaw();
        log.info("opened " + file + " of length " + result);
        reader = new SequenceFileReader(new SequenceFile.Reader(local, new Path(file.getAbsolutePath()), local.getConf()));
      } catch (FileNotFoundException ex) {
        Path p = new Path(Constants.getWalDirectory(acuConf), localLog);
        log.info("Looking for " + p);
        if (!fs.exists(p))
          throw new RuntimeException(ex);
        while (fs instanceof DistributedFileSystem) {
          DistributedFileSystem dfs = (DistributedFileSystem) fs;
          try {
            dfs.recoverLease(p);
            log.debug("recovered lease on " + p);
            break;
          } catch (IOException e) {
            try {
              log.debug("error recovering lease on " + p, e);
              dfs.append(p).close();
              log.debug("lease recovered using append on " + p);
              break;
            } catch (IOException ie) {
              UtilWaitThread.sleep(1000);
              log.debug("retrying lease recovery on " + p);
              continue;
            }
          }
        }
        reader = new FileReader(fs.open(p));
        log.info("opened " + p + " of length " + fs.getFileStatus(p).getLen());
        result = fs.getFileStatus(p).getLen();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    final LogEntryReader finalReader = reader;
    
    copyThreadPool.execute(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("Copying " + localLog + " to shared file system");
        for (int i = 0; i < 3; i++) {
          try {
            copySortLog(finalReader, fullyQualifiedFileName);
            return;
          } catch (IOException e) {
            log.error("error during copy", e);
          }
          UtilWaitThread.sleep(1000);
        }
        log.error("Unable to copy file to DFS, too many retries " + localLog);
        try {
          fs.create(new Path(fullyQualifiedFileName + ".failed")).close();
        } catch (IOException ex) {
          log.error("Unable to create failure flag file", ex);
        }
        long t2 = System.currentTimeMillis();
        if (metrics.isEnabled())
          metrics.add(LogWriterMetrics.copy, (t2 - t1));
      }
      
      private void copySortLog(LogEntryReader reader, String fullyQualifiedFileName) throws IOException {
        final long SORT_BUFFER_SIZE = acuConf.getMemoryInBytes(Property.TSERV_SORT_BUFFER_SIZE);
        
        Path dest = new Path(fullyQualifiedFileName + ".recovered");
        log.debug("Sorting log file to DSF " + dest);
        fs.mkdirs(dest);
        int part = 0;
        
        try {
          final ArrayList<Pair<LogFileKey,LogFileValue>> kv = new ArrayList<Pair<LogFileKey,LogFileValue>>();
          long start = reader.getPosition();
          while (true) {
            try {
              kv.add(reader.next());
            } catch (EOFException e) {
              break;
            }
            long memorySize = reader.getPosition() - start;
            if (memorySize > SORT_BUFFER_SIZE) {
              writeSortedEntries(dest, part++, kv);
              kv.clear();
              start = reader.getPosition();
            }
          }

          if (!kv.isEmpty())
            writeSortedEntries(dest, part++, kv);
          log.debug("Input file for " + dest + " was " + reader.getPosition() + " bytes long");
          fs.create(new Path(dest, "finished")).close();
        } finally {
          reader.close();
        }
      }
      
      private void writeSortedEntries(Path dest, int part, final List<Pair<LogFileKey,LogFileValue>> kv) throws IOException {
        String path = dest + String.format("/part-r-%05d", part);
        log.debug("Writing partial log file to DSF " + path);
        log.debug("Sorting");
        Span span = Trace.start("Logger sort");
        span.data("logfile", dest.getName());
        Collections.sort(kv, new Comparator<Pair<LogFileKey,LogFileValue>>() {
          @Override
          public int compare(Pair<LogFileKey,LogFileValue> o1, Pair<LogFileKey,LogFileValue> o2) {
            return o1.getFirst().compareTo(o2.getFirst());
          }
        });
        span.stop();
        span = Trace.start("Logger write");
        span.data("logfile", dest.getName());
        MapFile.Writer writer = new MapFile.Writer(fs.getConf(), fs, path, LogFileKey.class, LogFileValue.class);
        short replication = (short) acuConf.getCount(Property.LOGGER_RECOVERY_FILE_REPLICATION);
        fs.setReplication(new Path(path + "/" + MapFile.DATA_FILE_NAME), replication);
        fs.setReplication(new Path(path + "/" + MapFile.INDEX_FILE_NAME), replication);
        try {
          for (Pair<LogFileKey,LogFileValue> entry : kv)
            writer.append(entry.getFirst(), entry.getSecond());
        } finally {
          writer.close();
          span.stop();
        }
      }
      
    });
    return new LogCopyInfo(result, null);
  }
  
  @Override
  public LogFile create(TInfo info, AuthInfo credentials, String tserverSession) throws ThriftSecurityException {
    if (closed)
      throw new RuntimeException("Logger is closed");
    long t1 = System.currentTimeMillis();
    LogFile result = new LogFile();
    result.id = random.nextLong();
    while (logs.get(result.id) != null)
      result.id = random.nextLong();
    result.name = UUID.randomUUID().toString();
    Logger out = null;
    try {
      out = new Logger(fs.getConf(), roots.get(nextRoot++ % roots.size()) + "/" + result.name);
      out.key.event = OPEN;
      out.key.tserverSession = tserverSession;
      out.key.filename = instanceId;
      out.value.mutations = empty;
      out.logEntry();
      logs.put(result.id, out);
      file2id.put(result.name, result.id);
    } catch (Throwable e) {
      if (out != null) {
        try {
          out.close();
        } catch (Throwable t) {
          log.error("Error closing file", t);
        }
      }
      log.error("Unable to create log " + result.name);
      throw new RuntimeException(e);
    }
    long t2 = System.currentTimeMillis();
    if (metrics.isEnabled())
      metrics.add(LogWriterMetrics.create, (t2 - t1));
    log.info("Created log " + result.name);
    return result;
  }
  
  @Override
  public void log(TInfo info, long id, final long seq, final int tid, final TMutation mutation) throws NoSuchLogIDException {
    Logger out = logs.get(id);
    if (out == null)
      throw new NoSuchLogIDException();
    
    out.key.event = MUTATION;
    out.key.seq = seq;
    out.key.tid = tid;
    out.value.mutations = new Mutation[1];
    out.value.mutations[0] = new Mutation(mutation);
    try {
      out.logEntry();
    } catch (Throwable e) {
      log.error("log failure, closing log", e);
      try {
        close(info, id);
      } catch (Throwable t) {
        log.error("failure closing log", t);
      }
      throw new LogWriteException(e);
    }
  }
  
  private void logMany(TInfo info, long id, final long seq, final int tid, Mutation muations[]) throws NoSuchLogIDException {
    Logger out = logs.get(id);
    if (out == null)
      throw new NoSuchLogIDException();
    out.key.event = MANY_MUTATIONS;
    out.key.seq = seq;
    out.key.tid = tid;
    out.value.mutations = muations;
    try {
      out.logEntry();
    } catch (Throwable e) {
      log.error("log failure, closing log", e);
      try {
        close(info, id);
      } catch (Throwable t) {
        log.error("failure closing log", t);
      }
      throw new LogWriteException(e);
    }
  }
  
  @Override
  public void minorCompactionFinished(TInfo info, long id, final long seq, final int tid, final String fqfn) throws NoSuchLogIDException {
    Logger out = logs.get(id);
    if (out == null)
      throw new NoSuchLogIDException();
    out.key.event = COMPACTION_FINISH;
    out.key.seq = seq;
    out.key.tid = tid;
    out.value.mutations = empty;
    // this lock should not be necessary since there should only be one writer
    try {
      out.logEntry();
    } catch (Throwable e) {
      log.error("log failure, closing log", e);
      try {
        close(info, id);
      } catch (Throwable t) {
        log.error("failure closing log", t);
      }
      throw new LogWriteException(e);
    }
  }
  
  @Override
  public void minorCompactionStarted(TInfo info, long id, final long seq, final int tid, final String fqfn) throws NoSuchLogIDException {
    Logger out = logs.get(id);
    if (out == null)
      throw new NoSuchLogIDException();
    out.key.event = COMPACTION_START;
    out.key.seq = seq;
    out.key.tid = tid;
    out.key.filename = fqfn;
    out.value.mutations = empty;
    try {
      out.logEntry();
    } catch (Throwable e) {
      log.error("log failure, closing log", e);
      try {
        close(info, id);
      } catch (Throwable t) {
        log.error("failure closing log", t);
      }
      throw new LogWriteException(e);
    }
  }
  
  @Override
  public void defineTablet(TInfo info, long id, final long seq, final int tid, final TKeyExtent tablet) throws NoSuchLogIDException {
    Logger out = logs.get(id);
    if (out == null)
      throw new NoSuchLogIDException();
    out.key.event = DEFINE_TABLET;
    out.key.seq = seq;
    out.key.tid = tid;
    out.key.tablet = new KeyExtent(tablet);
    out.value.mutations = empty;
    try {
      out.logEntry();
    } catch (Throwable e) {
      log.error("log failure, closing log", e);
      try {
        close(info, id);
      } catch (Throwable t) {
        log.error("failure closing log", t);
      }
      throw new LogWriteException(e);
    }
  }
  
  public void shutdown() {
    // Make sure everything is closed
    log.info("Shutting down");
    List<Long> ids = new ArrayList<Long>();
    ids.addAll(logs.keySet());
    for (long id : ids) {
      try {
        close(null, id);
      } catch (Throwable t) {
        log.warn("Shutdown close exception:", t);
      }
    }
  }
  
  @Override
  public List<String> getClosedLogs(TInfo info, AuthInfo credentials) throws ThriftSecurityException {
    ArrayList<String> result = new ArrayList<String>();
    for (String root : roots) {
      for (File file : new File(root).listFiles()) {
        // skip dot-files
        if (file.getName().indexOf('.') >= 0)
          continue;
        // skip open logs
        if (file2id.containsKey(file.getName()))
          continue;
        if (LogArchiver.isArchive(file.getName()))
          continue;
        try {
          UUID.fromString(file.getName());
          result.add(file.getName());
        } catch (IllegalArgumentException ex) {
          log.debug("Ignoring file " + file.getName());
        }
      }
    }
    return result;
  }
  
  @Override
  public void remove(TInfo info, AuthInfo credentials, List<String> files) {
    log.info("Deleting " + files.size() + " log files");
    try {
      for (String file : files) {
        if (file2id.get(file) != null) {
          log.warn("ignoring attempt to delete open file " + file);
        } else {
          logArchiver.archive(this.findLocalFilename(file));
        }
      }
    } catch (IOException ex) {
      log.error("Unable to delete files", ex);
    }
  }
  
  @Override
  public void logManyTablets(TInfo info, long id, List<TabletMutations> mutations) throws NoSuchLogIDException {
    
    for (TabletMutations tm : mutations) {
      Mutation ma[] = new Mutation[tm.mutations.size()];
      int index = 0;
      for (TMutation tmut : tm.mutations)
        ma[index++] = new Mutation(tmut);
      logMany(info, id, tm.seq, tm.tabletID, ma);
    }
  }
  
  @Override
  synchronized public void beginShutdown(TInfo tinfo, AuthInfo credentials) throws TException {
    closed = true;
    synchronized (logs) {
      for (Logger out : logs.values()) {
        try {
          out.close();
        } catch (IOException ex) {
          log.error("IOException occurred closing file", ex);
        }
      }
      logs.clear();
      file2id.clear();
    }
  }
  
  @Override
  public void halt(TInfo tinfo, AuthInfo credentials) throws TException {}
  
}
