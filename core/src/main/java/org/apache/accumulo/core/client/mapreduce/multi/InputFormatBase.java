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
package org.apache.accumulo.core.client.mapreduce.multi;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.*;

import java.io.*;
import java.math.*;
import java.net.*;
import java.util.*;
import java.util.Map.*;

import org.apache.accumulo.core.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.*;
import org.apache.accumulo.core.client.mock.*;
import org.apache.accumulo.core.client.security.tokens.*;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.*;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.*;
import org.apache.accumulo.core.metadata.*;
import org.apache.accumulo.core.metadata.schema.*;
import org.apache.accumulo.core.security.*;
import org.apache.accumulo.core.util.*;
import org.apache.commons.codec.binary.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.*;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This input format provides keys and values of type K and V to the Map() and Reduce()
 * functions.
 * 
 * Subclasses must implement the following method: public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws
 * IOException, InterruptedException
 * 
 * This class includes a static class that can be used to create a RecordReader: protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V>
 * 
 * Subclasses of RecordReaderBase must implement the following method: public boolean nextKeyValue() throws IOException, InterruptedException This method should
 * set the following variables: K currentK V currentV Key currentKey (used for progress reporting) int numKeysRead (used for progress reporting)
 * 
 * See AccumuloInputFormat for an example implementation.
 * 
 * Other static methods are optional
 */

public abstract class InputFormatBase<K,V> extends InputFormat<K,V> {
  protected static final Logger log = Logger.getLogger(InputFormatBase.class);
  
  private static final String PREFIX = AccumuloInputFormat.class.getSimpleName();
  private static final String INPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
  private static final String USERNAME = PREFIX + ".username";
  private static final String PASSWORD_PATH = PREFIX + ".password";
  private static final String TABLE_NAMES = PREFIX + ".tablenames";
  private static final String AUTHORIZATIONS = PREFIX + ".authorizations";
  
  private static final String INSTANCE_NAME = PREFIX + ".instanceName";
  private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
  private static final String MOCK = ".useMockInstance";
  
  private static final String RANGES = PREFIX + ".ranges";
  private static final String AUTO_ADJUST_RANGES = PREFIX + ".ranges.autoAdjust";
  
  private static final String COLUMNS = PREFIX + ".columns";
  private static final String LOGLEVEL = PREFIX + ".loglevel";
  
  private static final String ISOLATED = PREFIX + ".isolated";
  
  private static final String LOCAL_ITERATORS = PREFIX + ".localiters";
  
  // Used to specify the maximum # of versions of an Accumulo cell value to return
  private static final String MAX_VERSIONS = PREFIX + ".maxVersions";
  
  // Used for specifying the iterators to be applied
  private static final String ITERATORS = PREFIX + ".iterators";
  private static final String ITERATORS_OPTIONS = PREFIX + ".iterators.options";
  private static final String ITERATORS_DELIM = ",";
  
  private static final String READ_OFFLINE = PREFIX + ".read.offline";
  
  /**
   * Enable or disable use of the {@link IsolatedScanner} in this configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the IsolatedScanner. Otherwise, disable.
   */
  public static void setIsolated(Configuration conf, boolean enable) {
    conf.setBoolean(ISOLATED, enable);
  }
  
  /**
   * Enable or disable use of the {@link ClientSideIteratorScanner} in this configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the ClientSideInteratorScanner. Otherwise, disable.
   */
  public static void setLocalIterators(Configuration conf, boolean enable) {
    conf.setBoolean(LOCAL_ITERATORS, enable);
  }
  
  /**
   * Initialize the user, table, and authorization information for the configuration object that will be used with an Accumulo InputFormat.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param user
   *          a valid accumulo user
   * @param passwd
   *          the user's password
   * @param tables
   *          the tables to read
   * @param auths
   *          the authorizations used to restrict data read
   */
  public static void setInputInfo(Configuration conf, String user, byte[] passwd, Iterable<String> tables, Authorizations auths) {
    if (conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
      throw new IllegalStateException("Input info can only be set once per job");
    conf.setBoolean(INPUT_INFO_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(user, passwd, tables);
    conf.set(USERNAME, user);
    
    ArgumentChecker.notEmpty (tables);
    String tablesCsv = StringUtils.join(tables.iterator(), ',');
    conf.set(TABLE_NAMES, tablesCsv);
    
    if (auths != null && !auths.isEmpty())
      conf.set(AUTHORIZATIONS, auths.serialize());
    
    try {
      FileSystem fs = FileSystem.get(conf);
      Path file = new Path(fs.getWorkingDirectory(), conf.get("mapred.job.name") + System.currentTimeMillis() + ".pw");
      conf.set(PASSWORD_PATH, file.toString());
      FSDataOutputStream fos = fs.create(file, false);
      fs.setPermission(file, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      fs.deleteOnExit(file);
      
      byte[] encodedPw = Base64.encodeBase64(passwd);
      fos.writeInt(encodedPw.length);
      fos.write(encodedPw);
      fos.close();
      
      DistributedCache.addCacheFile(file.toUri(), conf);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

  }
  
  /**
   * Configure a {@link ZooKeeperInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   */
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  /**
   * Configure a {@link MockInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   */
  public static void setMockInstance(Configuration conf, String instanceName) {
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    conf.setBoolean(MOCK, true);
    conf.set(INSTANCE_NAME, instanceName);
  }
  
  /**
   * Set the ranges to map over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param ranges
   *          the ranges that will be mapped over
   */
  public static void setRanges(Configuration conf, Map<String, Collection<Range>> ranges) {
    ArgumentChecker.notNull(ranges);
  
    ArrayList<String> rangeStrings = new ArrayList<String>(ranges.size());
    for(Entry<String, Collection<Range>> pair : ranges.entrySet()) {
      try {
        String tableName = pair.getKey();
        for (Range r : pair.getValue()) {
          TableRange tblRange = new TableRange(tableName, r);
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          tblRange.write(new DataOutputStream(baos));
          rangeStrings.add(new String(Base64.encodeBase64(baos.toByteArray())));
        }
      } catch (IOException ex) {
        throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
      }
    }
    conf.setStrings(RANGES, rangeStrings.toArray(new String[0]));
  }
  
  /**
   * Disables the adjustment of ranges for this configuration object. By default, overlapping ranges will be merged and ranges will be fit to existing tablet
   * boundaries. Disabling this adjustment will cause there to be exactly one mapper per range set using {@link #setRanges(Configuration, Map)}.
   *
   * @param conf
   *          the Hadoop configuration object
   */
  public static void disableAutoAdjustRanges(Configuration conf) {
    conf.setBoolean(AUTO_ADJUST_RANGES, false);
  }
  
  /**
   * Sets the max # of values that may be returned for an individual Accumulo cell. By default, applied before all other Accumulo iterators (highest priority)
   * leveraged in the scan by the record reader. To adjust priority use setIterator() & setIteratorOptions() w/ the VersioningIterator type explicitly.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param maxVersions
   *          the max number of versions per accumulo cell
   * @throws IOException
   *           if maxVersions is < 1
   */
  public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
    if (maxVersions < 1)
      throw new IOException("Invalid maxVersions: " + maxVersions + ".  Must be >= 1");
    conf.setInt(MAX_VERSIONS, maxVersions);
  }
  
  /**
   * <p>
   * Enable reading offline tables. This will make the map reduce job directly read the tables files. If the table is not offline, then the job will fail. If
   * the table comes online during the map reduce job, its likely that the job will fail.
   * 
   * <p>
   * To use this option, the map reduce user will need access to read the accumulo directory in HDFS.
   * 
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
   * on the mappers classpath. The accumulo-site.xml may need to be on the mappers classpath if HDFS or the accumlo directory in HDFS are non-standard.
   * 
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as the input table for a map reduce job. If you plan to map
   * reduce over the data many times, it may be better to the compact the table, clone it, take it offline, and use the clone for all map reduce jobs. The
   * reason to do this is that compaction will reduce each tablet in the table to one file, and its faster to read from one file.
   * 
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may see better read performance. Second, it will support
   * speculative execution better. When reading an online table speculative execution can put more load on an already slow tablet server.
   * 
   * @param conf
   *          the job
   * @param scanOff
   *          pass true to read offline tables
   */
  
  public static void setScanOffline(Configuration conf, boolean scanOff) {
    conf.setBoolean(READ_OFFLINE, scanOff);
  }
  
  /**
   * Restricts the columns that will be mapped over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param columnFamilyColumnQualifierPairs
   *          A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   */
  public static void fetchColumns(Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    ArgumentChecker.notNull(columnFamilyColumnQualifierPairs);
    ArrayList<String> columnStrings = new ArrayList<String>(columnFamilyColumnQualifierPairs.size());
    for (Pair<Text,Text> column : columnFamilyColumnQualifierPairs) {
      if (column.getFirst() == null)
        throw new IllegalArgumentException("Column family can not be null");
      
      String col = new String(Base64.encodeBase64(TextUtil.getBytes(column.getFirst())));
      if (column.getSecond() != null)
        col += ":" + new String(Base64.encodeBase64(TextUtil.getBytes(column.getSecond())));
      columnStrings.add(col);
    }
    conf.setStrings(COLUMNS, columnStrings.toArray(new String[0]));
  }
  
  /**
   * Sets the log level for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param level
   *          the logging level
   */
  public static void setLogLevel(Configuration conf, Level level) {
    ArgumentChecker.notNull(level);
    log.setLevel(level);
    conf.setInt(LOGLEVEL, level.toInt());
  }
  
  /**
   * Encode an iterator on the input for this configuration object.
   * 
   * @param conf
   *          The Hadoop configuration in which to save the iterator configuration
   * @param tableIterators
   *          A mapping of table names to IteratorSettings
   * @param conf
   *          The configuration of the iterator
   */
  public static void setIterators(Configuration conf, Map<String, ? extends Collection<IteratorSetting>> tableIterators) {
    // First check to see if anything has been set already
    String iterators = conf.get(ITERATORS);
    if(iterators != null) {
      throw new IllegalArgumentException("Iterators can only be set once per configuration.");
    }
    
    for(Entry<String, ? extends Collection<IteratorSetting>> tableItrs : tableIterators.entrySet()) {
      String table = tableItrs.getKey();
      for (IteratorSetting cfg : tableItrs.getValue()) {
        // No iterators specified yet, create a new string
        if (iterators == null || iterators.isEmpty()) {
          iterators = new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName(), table).toString();
        } else {
          // append the next iterator & reset
          iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName(), table).toString());
        }
        // Store the iterators w/ the job
        conf.set(ITERATORS, iterators);
        for (Entry<String,String> entry : cfg.getOptions().entrySet()) {
          if (entry.getValue() == null)
            continue;
          
          String iteratorOptions = conf.get(ITERATORS_OPTIONS);
          
          // No options specified yet, create a new string
          if (iteratorOptions == null || iteratorOptions.isEmpty()) {
            iteratorOptions = new AccumuloIteratorOption(table, cfg.getName(), entry.getKey(), entry.getValue()).toString();
          } else {
            // append the next option & reset
            iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(table, cfg.getName(), entry.getKey(), entry.getValue()));
          }
          
          // Store the options w/ the job
          conf.set(ITERATORS_OPTIONS, iteratorOptions);
        }
      }
    }
  }
  
  /**
   * Determines whether a configuration has isolation enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if isolation is enabled, false otherwise
   * @see #setIsolated(Configuration, boolean)
   */
  protected static boolean isIsolated(Configuration conf) {
    return conf.getBoolean(ISOLATED, false);
  }
  
  /**
   * Determines whether a configuration uses local iterators.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if uses local iterators, false otherwise
   * @see #setLocalIterators(Configuration, boolean)
   */
  protected static boolean usesLocalIterators(Configuration conf) {
    return conf.getBoolean(LOCAL_ITERATORS, false);
  }
  
  /**
   * Gets the user name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the user name
   * @see #setInputInfo(Configuration, String, byte[], List, Authorizations)
   */
  protected static String getUsername(Configuration conf) {
    return conf.get(USERNAME);
  }
  
  /**
   * Gets the password from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the BASE64-encoded password
   * @throws IOException
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static byte[] getPassword(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path file = new Path(conf.get(PASSWORD_PATH));
    
    FSDataInputStream fdis = fs.open(file);
    int length = fdis.readInt();
    byte[] encodedPassword = new byte[length];
    fdis.read(encodedPassword);
    fdis.close();
    
    return Base64.decodeBase64(encodedPassword);
  }
  
  /**
   * Gets the table name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the table name
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static String[] getTablenames(Configuration conf) {
    return StringUtils.split(conf.get(TABLE_NAMES), ',');
  }
  
  /**
   * Gets the authorizations to set for the scans from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the accumulo scan authorizations
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static Authorizations getAuthorizations(Configuration conf) {
    String authString = conf.get(AUTHORIZATIONS);
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.split(","));
  }
  
  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo instance
   * @see #setZooKeeperInstance(Configuration, String, String)
   * @see #setMockInstance(Configuration, String)
   */
  protected static Instance getInstance(Configuration conf) {
    if (conf.getBoolean(MOCK, false))
      return new MockInstance(conf.get(INSTANCE_NAME));
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   * @throws IOException
   *           if the input format is unable to read the password file from the FileSystem
   */
  protected static TabletLocator getTabletLocator(Configuration conf, String tableName) throws TableNotFoundException, IOException {
    if (conf.getBoolean(MOCK, false))
      return new MockTabletLocator();
    Instance instance = getInstance(conf);
    String username = getUsername(conf);
    byte[] password = getPassword(conf);

    return TabletLocator.getLocator (instance, new Text(Tables.getTableId (instance, tableName)));

    //TODO: Here we need to make sure we're using the auths correctly
//    return TabletLocator.getInstance(instance, new AuthInfo (username, ByteBuffer.wrap(password), instance.getInstanceID()),
//        new Text(Tables.getTableId(instance, tableName)));
  }
  
  /**
   * Gets the ranges to scan over from a configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @see #setRanges(Configuration, Map)
   */
  protected static TreeMap<String, List<Range>> getRanges(Configuration conf) throws IOException {
    TreeMap<String, List<Range>> ranges = new TreeMap<String, List<Range>>();
    
    // create collections for each table
    for(String table : getTablenames(conf)) {
      ranges.put(table, new ArrayList<Range>());
    }
    
    // parse out the ranges and add them to table's bucket
    for (String rangeString : conf.getStringCollection(RANGES)) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes()));
      TableRange range = new TableRange();
      range.readFields(new DataInputStream(bais));
      ranges.get(range.tableName()).add(range.range());
    }
    
    return ranges;
    
  }
  
  /**
   * Gets the columns to be mapped over from this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a set of columns
   * @see #fetchColumns(Configuration, Collection)
   */
  protected static Set<Pair<Text,Text>> getFetchedColumns(Configuration conf) {
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();
    for (String col : conf.getStringCollection(COLUMNS)) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes()) : Base64.decodeBase64(col.substring(0, idx).getBytes()));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
  }
  
  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if auto-adjust is enabled, false otherwise
   * @see #disableAutoAdjustRanges(Configuration)
   */
  protected static boolean getAutoAdjustRanges(Configuration conf) {
    return conf.getBoolean(AUTO_ADJUST_RANGES, true);
  }
  
  /**
   * Gets the log level from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the log level
   * @see #setLogLevel(Configuration, Level)
   */
  protected static Level getLogLevel(Configuration conf) {
    return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
  }
  
  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @throws IOException
   *           if the configuration is improperly configured
   */
  protected static void validateOptions(Configuration conf) throws IOException {
    if (!conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
      throw new IOException("Input info has not been set.");
    if (!conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IOException("Instance info has not been set.");
    // validate that we can connect as configured
    try {
      Connector c = getInstance(conf).getConnector(getUsername(conf), getPassword(conf));
      if (!c.securityOperations().authenticateUser(getUsername(conf), getPassword(conf)))
        throw new IOException("Unable to authenticate user");
      
      for(String tableName : getTablenames(conf)) {
        if (!c.securityOperations().hasTablePermission(getUsername(conf), tableName, TablePermission.READ))
          throw new IOException("Unable to access table");
      }
      
      if (!usesLocalIterators(conf)) {
        // validate that any scan-time iterators can be loaded by the the tablet servers
        for (AccumuloIterator iter : getIterators(conf)) {
          if (!c.instanceOperations().testClassLoad(iter.getIteratorClass(), SortedKeyValueIterator.class.getName()))
            throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass() + " as a " + SortedKeyValueIterator.class.getName());
        }
      }
      
    } catch (AccumuloException e) {
      throw new IOException(e);
    } catch (AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }
  
  /**
   * Gets the maxVersions to use for the {@link VersioningIterator} from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the max versions, -1 if not configured
   * @see #setMaxVersions(Configuration, int)
   */
  protected static int getMaxVersions(Configuration conf) {
    return conf.getInt(MAX_VERSIONS, -1);
  }
  
  protected static boolean isOfflineScan(Configuration conf) {
    return conf.getBoolean(READ_OFFLINE, false);
  }
  
  // Return a list of the iterator settings (for iterators to apply to a scanner)
  
  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterators
   * @see #setIterators(Configuration, IteratorSetting)
   */
  protected static List<AccumuloIterator> getIterators(Configuration conf) {
    
    String iterators = conf.get(ITERATORS);
    
    // If no iterators are present, return an empty list
    if (iterators == null || iterators.isEmpty())
      return new ArrayList<AccumuloIterator>();
    
    // Compose the set of iterators encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS), ITERATORS_DELIM);
    List<AccumuloIterator> list = new ArrayList<AccumuloIterator>();
    while (tokens.hasMoreTokens()) {
      String itstring = tokens.nextToken();
      list.add(new AccumuloIterator(itstring));
    }
    return list;
  }
  
  /**
   * Gets a list of the iterator options specified on this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterator options
   * @see #setIterators(Configuration, IteratorSetting)
   */
  protected static List<AccumuloIteratorOption> getIteratorOptions(Configuration conf) {
    String iteratorOptions = conf.get(ITERATORS_OPTIONS);
    
    // If no options are present, return an empty list
    if (iteratorOptions == null || iteratorOptions.isEmpty())
      return new ArrayList<AccumuloIteratorOption>();
    
    // Compose the set of options encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS_OPTIONS), ITERATORS_DELIM);
    List<AccumuloIteratorOption> list = new ArrayList<AccumuloIteratorOption>();
    while (tokens.hasMoreTokens()) {
      String optionString = tokens.nextToken();
      list.add(new AccumuloIteratorOption(optionString));
    }
    return list;
  }
  
  protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V> {
    protected long numKeysRead;
    protected Iterator<Entry<Key,Value>> scannerIterator;
    protected RangeInputSplit split;
    
    /**
     * Apply the configured iterators from the configuration to the scanner.
     * 
     * @param conf
     *          the Hadoop configuration object
     * @param scanner
     *          the scanner to configure
     * @throws AccumuloException
     */
    protected void setupIterators(Configuration conf, Scanner scanner, String table) throws AccumuloException {
      List<AccumuloIterator> iterators = getIterators(conf);
      List<AccumuloIteratorOption> options = getIteratorOptions(conf);
      
      Map<String,IteratorSetting> scanIterators = new HashMap<String,IteratorSetting>();
      for (AccumuloIterator iterator : iterators) {
        if(iterator.getTable().equals(table)) {
          scanIterators.put(iterator.getIteratorName(), new IteratorSetting(iterator.getPriority(), iterator.getIteratorName(), iterator.getIteratorClass()));
        }
      }
      for (AccumuloIteratorOption option : options) {
        if(option.getTable().equals(table)) {
          scanIterators.get(option.iteratorName).addOption(option.getKey(), option.getValue());
        }
      }
      for (IteratorSetting iterator : scanIterators.values()) {
        scanner.addScanIterator(iterator);
      }
    }
    
    /**
     * If maxVersions has been set, configure a {@link VersioningIterator} at priority 0 for this scanner.
     * 
     * @param conf
     *          the Hadoop configuration object
     * @param scanner
     *          the scanner to configure
     */
    protected void setupMaxVersions(Configuration conf, Scanner scanner) {
      int maxVersions = getMaxVersions(conf);
      // Check to make sure its a legit value
      if (maxVersions >= 1) {
        IteratorSetting vers = new IteratorSetting(0, "vers", VersioningIterator.class);
        VersioningIterator.setMaxVersions(vers, maxVersions);
        scanner.addScanIterator(vers);
      }
    }
    
    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
      initialize(inSplit, attempt.getConfiguration());
    }
    
    public void initialize(InputSplit inSplit, Configuration conf) throws IOException {
      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split.range);
      Instance instance = getInstance(conf);
      String user = getUsername(conf);
      byte[] password = getPassword(conf);
      Authorizations authorizations = getAuthorizations(conf);
      
      try {
        log.debug("Creating connector with user: " + user);
        Connector conn = instance.getConnector(user, password);
        log.debug("Creating scanner for table: " + split.getTableName ());
        log.debug("Authorizations are: " + authorizations);
        if (isOfflineScan(conf)) {
          // TODO: This used AuthInfo before- figure out a better equivalent
          scanner = new OfflineScanner(instance, new Credentials(user, new PasswordToken(password)), Tables.getTableId(instance,
              split.getTableName()), authorizations);
        } else {
          scanner = conn.createScanner(split.getTableName(), authorizations);
        }
        if (isIsolated(conf)) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (usesLocalIterators(conf)) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupMaxVersions(conf, scanner);
        setupIterators(conf, scanner, split.getTableName());
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      // setup a scanner within the bounds of this split
      for (Pair<Text,Text> c : getFetchedColumns(conf)) {
        if (c.getSecond() != null) {
          log.debug("Fetching column " + c.getFirst() + ":" + c.getSecond());
          scanner.fetchColumn(c.getFirst(), c.getSecond());
        } else {
          log.debug("Fetching column family " + c.getFirst());
          scanner.fetchColumnFamily(c.getFirst());
        }
      }
      
      scanner.setRange(split.range);
      
      numKeysRead = 0;
      
      // do this last after setting all scanner options
      scannerIterator = scanner.iterator();
    }
    
    public void close() {}
    
    public float getProgress() throws IOException {
      if (numKeysRead > 0 && currentKey == null)
        return 1.0f;
      return split.getProgress(currentKey);
    }
    
    protected K currentK = null;
    protected V currentV = null;
    protected Key currentKey = null;
    protected Value currentValue = null;
    
    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return currentK;
    }
    
    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return currentV;
    }
  }
  
  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(Configuration conf, String tableName, List<Range> ranges) throws TableNotFoundException,
      AccumuloException, AccumuloSecurityException, IOException {
    
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    
    Instance instance = getInstance(conf);
    Connector conn = instance.getConnector(getUsername(conf), getPassword(conf));
    String tableId = Tables.getTableId(instance, tableName);
    
    if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
      Tables.clearCache(instance);
      if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
        throw new AccumuloException("Table is online " + tableName + "(" + tableId + ") cannot scan table in offline mode ");
      }
    }
    
    for (Range range : ranges) {
      Text startRow;
      
      if (range.getStartKey() != null)
        startRow = range.getStartKey().getRow();
      else
        startRow = new Text();
      
      Range metadataRange = new Range(new KeyExtent(new Text(tableId), startRow, null).getMetadataEntry(), true, null, false);
      Scanner scanner = conn.createScanner(MetadataTable.NAME, Constants.NO_AUTHS);

      MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch (scanner);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LastLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
      scanner.setRange(metadataRange);
      
      RowIterator rowIter = new RowIterator(scanner);
      
      // TODO check that extents match prev extent
      
      KeyExtent lastExtent = null;
      
      while (rowIter.hasNext()) {
        Iterator<Entry<Key,Value>> row = rowIter.next();
        String last = "";
        KeyExtent extent = null;
        String location = null;
        
        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();
          
          if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.LastLocationColumnFamily.NAME)) {
            last = entry.getValue().toString();
          }
          
          if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)
              || key.getColumnFamily().equals(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME)) {
            location = entry.getValue().toString();
          }
          
          if (PREV_ROW_COLUMN.hasColumns(key)) {
            extent = new KeyExtent(key.getRow(), entry.getValue());
          }
          
        }
        
        if (location != null)
          return null;
        
        if (!extent.getTableId().toString().equals(tableId)) {
          throw new AccumuloException("Saw unexpected table Id " + tableId + " " + extent);
        }
        
        if (lastExtent != null && !extent.isPreviousExtent(lastExtent)) {
          throw new AccumuloException(" " + lastExtent + " is not previous extent " + extent);
        }
        
        Map<KeyExtent,List<Range>> tabletRanges = binnedRanges.get(last);
        if (tabletRanges == null) {
          tabletRanges = new HashMap<KeyExtent,List<Range>>();
          binnedRanges.put(last, tabletRanges);
        }
        
        List<Range> rangeList = tabletRanges.get(extent);
        if (rangeList == null) {
          rangeList = new ArrayList<Range>();
          tabletRanges.put(extent, rangeList);
        }
        
        rangeList.add(range);
        
        if (extent.getEndRow() == null || range.afterEndKey(new Key(extent.getEndRow()).followingKey(PartialKey.ROW))) {
          break;
        }
        
        lastExtent = extent;
      }
      
    }
    
    return binnedRanges;
  }
  
  /**
   * Read the metadata table to get tablets and match up ranges to them.
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    return getSplits(job.getConfiguration());
  }
  
  /*
   * TODO
   *  - I created the TableRange class; need to massage it into the getSplits code
   */
  public List<InputSplit> getSplits(Configuration conf) throws IOException {
    log.setLevel(getLogLevel(conf));
    validateOptions(conf);
    
    boolean autoAdjust = getAutoAdjustRanges(conf);
    Map<String, List<Range>> tablesRanges = getRanges(conf);
    LinkedList<InputSplit> splits = new LinkedList<InputSplit>();
    
    for (Entry<String, List<Range>> tableRanges : tablesRanges.entrySet() ) {
      String tableName = tableRanges.getKey();
      List<Range> ranges = autoAdjust ? Range.mergeOverlapping(tableRanges.getValue()) : tableRanges.getValue();
      
      if (ranges.isEmpty()) {
        ranges = new ArrayList<Range>(1);
        ranges.add(new Range());
      }
      
      // get the metadata information for these ranges
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
      TabletLocator tl;
      try {
        if (isOfflineScan(conf)) {
          binnedRanges = binOfflineTable(conf, tableName, ranges);
          while (binnedRanges == null) {
            // Some tablets were still online, try again
            UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
            binnedRanges = binOfflineTable(conf, tableName, ranges);
          }
        } else {
          Instance instance = getInstance(conf);
          String tableId = null;
          tl = getTabletLocator(conf, tableName);
          // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
          tl.invalidateCache();
          Credentials creds = new Credentials (getUsername (conf), new PasswordToken(getPassword (conf)));
          while (!tl.binRanges(creds, ranges, binnedRanges).isEmpty()) {
            if (!(instance instanceof MockInstance)) {
              if (tableId == null)
                tableId = Tables.getTableId(instance, tableName);
              if (!Tables.exists(instance, tableId))
                throw new TableDeletedException(tableId);
              if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
                throw new TableOfflineException(instance, tableId);
            }
            binnedRanges.clear();
            log.warn("Unable to locate bins for specified ranges. Retrying.");
            UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
            tl.invalidateCache();
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      HashMap<Range,ArrayList<String>> splitsToAdd = null;
      
      if (!autoAdjust)
        splitsToAdd = new HashMap<Range,ArrayList<String>>();
      
      HashMap<String,String> hostNameCache = new HashMap<String,String>();
      
      for (Entry<String,Map<KeyExtent,List<Range>>> tserverBin : binnedRanges.entrySet()) {
        String ip = tserverBin.getKey().split(":", 2)[0];
        String location = hostNameCache.get(ip);
        if (location == null) {
          InetAddress inetAddress = InetAddress.getByName(ip);
          location = inetAddress.getHostName();
          hostNameCache.put(ip, location);
        }
        
        for (Entry<KeyExtent,List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
          Range ke = extentRanges.getKey().toDataRange();
          for (Range r : extentRanges.getValue()) {
            if (autoAdjust) {
              // divide ranges into smaller ranges, based on the tablets
              splits.add(new RangeInputSplit(tableName, ke.clip(r), new String[] {location}));
            } else {
              // don't divide ranges
              ArrayList<String> locations = splitsToAdd.get(r);
              if (locations == null)
                locations = new ArrayList<String>(1);
              locations.add(location);
              splitsToAdd.put(r, locations);
            }
          }
        }
      }
      
      if (!autoAdjust)
        for (Entry<Range,ArrayList<String>> entry : splitsToAdd.entrySet())
          splits.add(new RangeInputSplit(tableName, entry.getKey(), entry.getValue().toArray(new String[0])));
    }
    return splits;
  }
  
  /**
   * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
   */
  public static class RangeInputSplit extends InputSplit implements Writable {
    private Range range;
    private String[] locations;
    private String tableName;
    
    public RangeInputSplit() {
      range = new Range();
      locations = new String[0];
      tableName = "";
    }
    
    public RangeInputSplit(RangeInputSplit split) throws IOException {
      this.setRange(split.getRange());
      this.setLocations(split.getLocations());
    }
    
    RangeInputSplit(String table, Range range, String[] locations) {
      this.tableName = table;
      this.range = range;
      this.locations = locations;
    }
    
    public Range getRange() {
      return range;
    }
    
    public void setRange(Range range) {
      this.range = range;
    }
    
    public String getTableName() {
      return tableName;
    }
    
    public void setTableName(String tableName) {
      this.tableName = tableName;
    }
    
    private static byte[] extractBytes(ByteSequence seq, int numBytes) {
      byte[] bytes = new byte[numBytes + 1];
      bytes[0] = 0;
      for (int i = 0; i < numBytes; i++) {
        if (i >= seq.length())
          bytes[i + 1] = 0;
        else
          bytes[i + 1] = seq.byteAt(i);
      }
      return bytes;
    }
    
    public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
      int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
      BigInteger startBI = new BigInteger(extractBytes(start, maxDepth));
      BigInteger endBI = new BigInteger(extractBytes(end, maxDepth));
      BigInteger positionBI = new BigInteger(extractBytes(position, maxDepth));
      return (float) (positionBI.subtract(startBI).doubleValue() / endBI.subtract(startBI).doubleValue());
    }
    
    public float getProgress(Key currentKey) {
      if (currentKey == null)
        return 0f;
      if (range.getStartKey() != null && range.getEndKey() != null) {
        if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0) {
          // just look at the row progress
          return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(), currentKey.getRowData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM) != 0) {
          // just look at the column family progress
          return getProgress(range.getStartKey().getColumnFamilyData(), range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL) != 0) {
          // just look at the column qualifier progress
          return getProgress(range.getStartKey().getColumnQualifierData(), range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
        }
      }
      // if we can't figure it out, then claim no progress
      return 0f;
    }
    

    
    /**
     * This implementation of length is only an estimate, it does not provide exact values. Do not have your code rely on this return value.
     */
    public long getLength() throws IOException {
      Text startRow = range.isInfiniteStartKey() ? new Text(new byte[] {Byte.MIN_VALUE}) : range.getStartKey().getRow();
      Text stopRow = range.isInfiniteStopKey() ? new Text(new byte[] {Byte.MAX_VALUE}) : range.getEndKey().getRow();
      int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
      long diff = 0;
      
      byte[] start = startRow.getBytes();
      byte[] stop = stopRow.getBytes();
      for (int i = 0; i < maxCommon; ++i) {
        diff |= 0xff & (start[i] ^ stop[i]);
        diff <<= Byte.SIZE;
      }
      
      if (startRow.getLength() != stopRow.getLength())
        diff |= 0xff;
      
      return diff + 1;
    }
    
    public String[] getLocations() throws IOException {
      return locations;
    }
    
    public void setLocations(String[] locations) {
      this.locations = locations;
    }
    
    public void readFields(DataInput in) throws IOException {
      range.readFields(in);
      int numLocs = in.readInt();
      locations = new String[numLocs];
      for (int i = 0; i < numLocs; ++i)
        locations[i] = in.readUTF();
      tableName = in.readUTF();
    }
    
    public void write(DataOutput out) throws IOException {
      range.write(out);
      out.writeInt(locations.length);
      for (int i = 0; i < locations.length; ++i)
        out.writeUTF(locations[i]);
      out.writeUTF(tableName);
    }
  }
  
  /**
   * Pairs together a table name and a range.
   */
  static class TableRange implements Writable {
    private String tableName;
    private Range range;
    
    public TableRange() {
      range = new Range();
    }
    
    public TableRange(String tableName, Range range) {
      this.tableName = tableName;
      this.range = range;
    }
    
    public String tableName() {
      return tableName;
    }
    
    public Range range() {
      return range;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      tableName = in.readUTF();
      range.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(tableName);
      range.write(out);
    }
    
  }
  
  /**
   * The Class IteratorSetting. Encapsulates specifics for an Accumulo iterator's name & priority.
   */
  static class AccumuloIterator {
    
    private static final String FIELD_SEP = ":";
    
    private int priority;
    private String iteratorClass;
    private String iteratorName;
    private String table;
    
    public AccumuloIterator(int priority, String iteratorClass, String iteratorName, String table) {
      this.priority = priority;
      this.iteratorClass = iteratorClass;
      this.iteratorName = iteratorName;
      this.table = table;
    }
    
    // Parses out a setting given an string supplied from an earlier toString() call
    public AccumuloIterator(String iteratorSetting) {
      // Parse the string to expand the iterator
      StringTokenizer tokenizer = new StringTokenizer(iteratorSetting, FIELD_SEP);
      table = tokenizer.nextToken();
      priority = Integer.parseInt(tokenizer.nextToken());
      iteratorClass = tokenizer.nextToken();
      iteratorName = tokenizer.nextToken();
    }
    
    public int getPriority() {
      return priority;
    }
    
    public String getIteratorClass() {
      return iteratorClass;
    }
    
    public String getIteratorName() {
      return iteratorName;
    }
    
    public String getTable() {
      return table;
    }
    
    @Override
    public String toString() {
      return new String(table + FIELD_SEP + priority + FIELD_SEP + iteratorClass + FIELD_SEP + iteratorName);
    }
    
  }
  
  /**
   * The Class AccumuloIteratorOption. Encapsulates specifics for an Accumulo iterator's optional configuration details - associated via the iteratorName.
   */
  static class AccumuloIteratorOption {
    private static final String FIELD_SEP = ":";
    
    private String table;
    private String iteratorName;
    private String key;
    private String value;
    
    public AccumuloIteratorOption(String table, String iteratorName, String key, String value) {
      this.table = table;
      this.iteratorName = iteratorName;
      this.key = key;
      this.value = value;
    }
    
    // Parses out an option given a string supplied from an earlier toString() call
    public AccumuloIteratorOption(String iteratorOption) {
      StringTokenizer tokenizer = new StringTokenizer(iteratorOption, FIELD_SEP);
      this.iteratorName = tokenizer.nextToken();
      try {
        this.key = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
        this.value = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    
    public String getIteratorName() {
      return iteratorName;
    }
    
    public String getKey() {
      return key;
    }
    
    public String getValue() {
      return value;
    }
    
    public String getTable() {
      return table;
    }
    
    @Override
    public String toString() {
      try {
        return new String(table + FIELD_SEP + iteratorName + FIELD_SEP + URLEncoder.encode(key, "UTF-8") + FIELD_SEP + URLEncoder.encode(value, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    
  }
  
}
