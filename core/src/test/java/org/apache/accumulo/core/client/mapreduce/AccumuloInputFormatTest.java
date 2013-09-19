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
package org.apache.accumulo.core.client.mapreduce;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.addIterator;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.fetchColumns;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.getFetchedColumns;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.getIterators;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.getRanges;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setConnectorInfo;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setInputTableNames;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setMockInstance;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setRanges;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setScanAuthorizations;
import static org.apache.accumulo.core.iterators.user.RegExFilter.setRegexs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class AccumuloInputFormatTest {
  
  private static final String PREFIX = AccumuloInputFormatTest.class.getSimpleName();
  private static final String INSTANCE_NAME = PREFIX + "_mapreduce_instance";
  private static final String TEST_TABLE_1 = PREFIX + "_mapreduce_table_1";
  private static final String TEST_TABLE_2 = PREFIX + "_mapreduce_table_2";

  /**
   * Check that the iterator configuration is getting stored in the Job conf correctly.
   * 
   * @throws IOException
   */
  @Test
  public void testSetIterator() throws IOException {
    Job job = new Job();

    IteratorSetting is = new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator");
    addIterator (job, is);
    Configuration conf = job.getConfiguration();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    is.write(new DataOutputStream(baos));
    String iterators = conf.get("AccumuloInputFormat.ScanOpts.Iterators");
    assertEquals(new String(Base64.encodeBase64(baos.toByteArray())), iterators);
  }
  
  @Test
  public void testAddIterator() throws IOException {
    Job job = new Job();

    addIterator (job, new IteratorSetting (1, "WholeRow", WholeRowIterator.class));
    addIterator (job, new IteratorSetting (2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    IteratorSetting iter = new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator");
    iter.addOption("v1", "1");
    iter.addOption("junk", "\0omg:!\\xyzzy");
    addIterator (job, iter);
    
    List<IteratorSetting> list = getIterators (job);
    
    // Check the list size
    assertTrue(list.size() == 3);
    
    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.user.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());
    assertEquals(0, setting.getOptions().size());
    
    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());
    assertEquals(0, setting.getOptions().size());
    
    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());
    assertEquals(2, setting.getOptions().size());
    assertEquals("1", setting.getOptions().get("v1"));
    assertEquals("\0omg:!\\xyzzy", setting.getOptions().get("junk"));
  }
  
  /**
   * Test adding iterator options where the keys and values contain both the FIELD_SEPARATOR character (':') and ITERATOR_SEPARATOR (',') characters. There
   * should be no exceptions thrown when trying to parse these types of option entries.
   * 
   * This test makes sure that the expected raw values, as appears in the Job, are equal to what's expected.
   */
  @Test
  public void testIteratorOptionEncoding() throws Throwable {
    String key = "colon:delimited:key";
    String value = "comma,delimited,value";
    IteratorSetting someSetting = new IteratorSetting(1, "iterator", "Iterator.class");
    someSetting.addOption(key, value);
    Job job = new Job();
    addIterator (job, someSetting);
    
    List<IteratorSetting> list = getIterators (job);
    assertEquals(1, list.size());
    assertEquals(1, list.get(0).getOptions().size());
    assertEquals(list.get(0).getOptions().get(key), value);
    
    someSetting.addOption (key + "2", value);
    someSetting.setPriority (2);
    someSetting.setName ("it2");
    addIterator (job, someSetting);
    list = getIterators (job);
    assertEquals(2, list.size());
    assertEquals(1, list.get(0).getOptions().size());
    assertEquals(list.get(0).getOptions().get(key), value);
    assertEquals(2, list.get(1).getOptions().size());
    assertEquals(list.get(1).getOptions().get(key), value);
    assertEquals(list.get(1).getOptions().get(key + "2"), value);
  }
  
  /**
   * Test getting iterator settings for multiple iterators set
   * 
   * @throws IOException
   */
  @Test
  public void testGetIteratorSettings() throws IOException {
    Job job = new Job();

    addIterator (job, new IteratorSetting (1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator"));
    addIterator (job, new IteratorSetting (2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    addIterator (job, new IteratorSetting (3, "Count", "org.apache.accumulo.core.iterators.CountingIterator"));
    
    List<IteratorSetting> list = getIterators (job);
    
    // Check the list size
    assertTrue(list.size() == 3);
    
    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());
    
    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());
    
    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());
    
  }
  
  @Test
  public void testSetRegex() throws IOException {
    Job job = new Job();

    String regex = ">\"*%<>\'\\";
    
    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    setRegexs (is, regex, null, null, null, false);
    addIterator (job, is);
    
    assertTrue(regex.equals(getIterators (job).get(0).getName()));
  }
  
  private static AssertionError e1 = null;
  private static AssertionError e2 = null;
  
  private static class MRTester extends Configured implements Tool {

    private static class TestMapper extends Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;
      
      @Override
      protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
        try {
          String tableName = ((InputFormatBase.RangeInputSplit)context.getInputSplit ()).getTableName ();
          if (key != null)
            assertEquals(key.getRow().toString(), new String(v.get()));
          assertEquals(new Text(String.format("%s_%09x", tableName, count + 1)), k.getRow());
          assertEquals(String.format("%s_%09x", tableName, count), new String(v.get()));
        } catch (AssertionError e) {
          e1 = e;
        }
        key = new Key(k);
        count++;
      }
      
      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }
    
    @Override
    public int run(String[] args) throws Exception {
      
      if (args.length != 4) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <table>");
      }
      
      String user = args[0];
      String pass = args[1];
      String table1 = args[2];
      String table2 = args[3];

      Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      
      job.setInputFormatClass(AccumuloInputFormat.class);
      
      setConnectorInfo(job, user, new PasswordToken(pass));
      setInputTableNames (job, asList (new String[]{table1, table2}));
      setMockInstance(job, INSTANCE_NAME);
      
      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      
      job.setNumReduceTasks(0);
      
      job.waitForCompletion(true);
      
      return job.isSuccessful() ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
      assertEquals(0, ToolRunner.run(CachedConfiguration.getInstance(), new MRTester(), args));
    }
  }

  /**
   * Generate incrementing counts and attach table name to the key/value so that order and multi-table data can be
   * verified.
   */
  @Test
  public void testMap() throws Exception {
    MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
    Connector c = mockInstance.getConnector("root", new PasswordToken(""));
    c.tableOperations().create(TEST_TABLE_1);
    c.tableOperations().create(TEST_TABLE_2);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE_1, new BatchWriterConfig());
    BatchWriter bw2 = c.createBatchWriter(TEST_TABLE_2, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation t1m = new Mutation(new Text(String.format("%s_%09x", TEST_TABLE_1, i + 1)));
      t1m.put(new Text(), new Text(), new Value(String.format("%s_%09x", TEST_TABLE_1,  i).getBytes()));
      bw.addMutation(t1m);
      Mutation t2m = new Mutation(new Text(String.format("%s_%09x", TEST_TABLE_2, i + 1)));
      t2m.put(new Text(), new Text(), new Value(String.format("%s_%09x", TEST_TABLE_2, i).getBytes()));
      bw2.addMutation(t2m);
    }
    bw.close();
    bw2.close ();
    
    MRTester.main(new String[] {"root", "", TEST_TABLE_1, TEST_TABLE_2});
    assertNull(e1);
    assertNull(e2);
  }


  /**
   * Asserts that the configuration contains the expected ranges for the tables.
   */
  @Test
  public void testMultitableRangeSerialization() throws Throwable {
    List<String> tables = asList ("t1", "t2", "t3");
    Job job = new Job(new Configuration());
    job.setInputFormatClass(AccumuloInputFormat.class);
    job.setMapperClass (MRTester.TestMapper.class);
    job.setNumReduceTasks (0);
    setConnectorInfo (job, "root", new PasswordToken (new byte[0]));
    setInputTableNames (job, tables);
    setScanAuthorizations (job, new Authorizations ());
    setMockInstance (job, "testmapinstance");

    HashMap<String, Collection<Range>> tblRanges = new HashMap<String, Collection<Range>>();
    for(String tbl : tables) {
      List<Range> ranges = asList (
              new Range ("a", "b"),
              new Range ("c", "d"),
              new Range ("e", "f"));
      tblRanges.put(tbl, ranges);
    }

    Range defaultRange = new Range("0", "1");

    // set a default range
    setRanges (job, singleton (defaultRange));
    setRanges (job, tblRanges);
    Map<String, List<Range>> configuredRanges = getRanges (job);

    for(Map.Entry<String, List<Range>> cfgRange : configuredRanges.entrySet()) {
      String tbl = cfgRange.getKey();
      HashSet<Range> originalRanges = new HashSet<Range>(tblRanges.remove(tbl));
      originalRanges.add(defaultRange);
      HashSet<Range> retrievedRanges = new HashSet<Range>(cfgRange.getValue());
      assertEquals (originalRanges.size (), retrievedRanges.size ());
      assertTrue (originalRanges.containsAll (retrievedRanges));
      assertTrue (retrievedRanges.containsAll (originalRanges));
    }
  }

  /**
   * Asserts that the configuration contains the expected iterators for the tables.
   */
  @Test
  public void testMultitableIteratorSerialization() throws Throwable {
    HashSet<String> tables = new HashSet<String>(asList ("t1", "t2"));
    Job job = new Job(new Configuration());
    job.setInputFormatClass (AccumuloInputFormat.class);
    job.setMapperClass (MRTester.TestMapper.class);
    job.setNumReduceTasks (0);
    setConnectorInfo (job, "root", new PasswordToken (new byte[0]));
    setInputTableNames (job, tables);
    setScanAuthorizations (job, new Authorizations ());

    // create + set iterators on configuration and build expected reference set
    IteratorSetting isetting1 = new IteratorSetting(1, "name1", "class1");
    IteratorSetting isetting2 = new IteratorSetting(2, "name2", "class2");
    IteratorSetting isetting3 = new IteratorSetting(2, "name3", "class3");

    addIterator (job, isetting1, "t1");
    addIterator (job, isetting2, "t2");
    addIterator (job, isetting3);

    // verify per-table iterators
    List<IteratorSetting> t1iters = getIterators (job, "t1");
    List<IteratorSetting> t2iters = getIterators (job, "t2");
    assertFalse (t1iters.isEmpty ());
    assertEquals(isetting1, t1iters.get(1));
    assertEquals(isetting3, t1iters.get(0));
    assertEquals (isetting2, t2iters.get (1));
    assertEquals (isetting3, t2iters.get (0));
  }

  @Test
  public void testMultitableColumnSerialization() throws IOException, AccumuloSecurityException {
    HashSet<String> tables = new HashSet<String>(asList ("t1", "t2"));
    Job job = new Job(new Configuration());
    job.setInputFormatClass (AccumuloInputFormat.class);
    job.setMapperClass (MRTester.TestMapper.class);
    job.setNumReduceTasks (0);
    setConnectorInfo (job, "root", new PasswordToken (new byte[0]));
    setInputTableNames (job, tables);
    setScanAuthorizations (job, new Authorizations ());

    Map<String, Collection<Pair<Text,Text>>> columns = new HashMap<String, Collection<Pair<Text,Text>>>();
    HashSet<Pair<Text,Text>> t1cols = new HashSet<Pair<Text,Text>>();
    t1cols.add(new Pair(new Text("a"), new Text("b")));
    HashSet<Pair<Text,Text>> t2cols = new HashSet<Pair<Text, Text>> ();
    t2cols.add(new Pair(new Text("b"), new Text("c")));
    columns.put("t1", t1cols);
    columns.put("t2", t2cols);

    Pair<Text, Text> defaultColumn = new Pair(new Text("c"), new Text("d"));

    fetchColumns (job, singleton (defaultColumn));
    fetchColumns (job, columns);

    columns.get("t1").add (defaultColumn);
    columns.get("t2").add (defaultColumn);

    Collection<Pair<Text,Text>> t1actual = getFetchedColumns (job, "t1");
    assertEquals(columns.get("t1"), t1actual);
    Collection<Pair<Text,Text>> t2actual = getFetchedColumns (job, "t2");
    assertEquals(columns.get("t2"), t2actual);
  }
}
