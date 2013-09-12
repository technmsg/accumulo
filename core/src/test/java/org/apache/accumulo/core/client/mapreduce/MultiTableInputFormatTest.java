package org.apache.accumulo.core.client.mapreduce;

import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.getRanges;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setConnectorInfo;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setInputTableNames;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setMockInstance;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setRanges;
import static org.apache.accumulo.core.client.mapreduce.InputFormatBase.setScanAuthorizations;
import static org.apache.accumulo.core.client.mapreduce.multi.ContextFactory.createMapContext;
import static org.apache.accumulo.core.client.mapreduce.multi.ContextFactory.createTaskAttemptContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

public class MultiTableInputFormatTest {
  
  /**
   * Writes data out to a table.
   * 
   * The data written out is 100 entries, with the row being a number 1-100 and the value
   * being a number one less than the row (0-99).
   * 
   * @param c
   * @param table
   * @throws Throwable
   */
  static void writeData(Connector c, String table) throws Throwable {
    c.tableOperations().create(table);
    BatchWriter bw = c.createBatchWriter(table, 10000L, 1000L, 4);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
  }
  
  /**
   * Creates five tables, table0 through table4, that get loaded with 100 keys each.
   * 
   * This test expects that each table is filled with 100 entries and that a sample
   * MapReduce job is created to scan all five. We should see five input splits; one for
   * each table. 
   * 
   * The sample job uses the TestMapper class defined locally to this test. Verification
   * of features such as expected table and number of keys is performed via the TestMapper.
   * 
   * @throws Throwable
   */
  @Test
  public void testMap() throws Throwable {
    MockInstance mockInstance = new MockInstance("testmapinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    StringBuilder tablesBuilder = new StringBuilder();
    LinkedList<String> tablesList = new LinkedList<String>();
    for(int i = 0; i < 5; ++i) {
      String table = "table" + i;
      tablesList.add(table);
      writeData(c, table);
      tablesBuilder.append(table).append(',');
    }
    tablesBuilder.setLength(tablesBuilder.length() - 1);
    
    Job job = new Job(new Configuration());
    job.setInputFormatClass (AccumuloInputFormat.class);
    job.setMapperClass (TestMapper.class);
    job.setNumReduceTasks (0);
    setConnectorInfo (job, "root", new PasswordToken (new byte[0]));
    setInputTableNames (job, tablesList);
    setScanAuthorizations (job, new Authorizations ());
    setMockInstance (job, "testmapinstance");

    AccumuloInputFormat input = new AccumuloInputFormat ();
    List<InputSplit> splits = input.getSplits(job);
    assertEquals(splits.size(), 5);
    
    TestMapper mapper = (TestMapper) job.getMapperClass().newInstance();
    for (InputSplit split : splits) {
      TaskAttemptContext tac = createTaskAttemptContext (job);
      RecordReader<Key,Value> reader = input.createRecordReader(split, tac);
      Mapper<Key,Value,Key,Value>.Context context = createMapContext (mapper, tac, reader, null, split);
      reader.initialize(split, context);
      mapper.expectedTable( new Text( ((RangeInputSplit) split).getTableName () ) );
      mapper.run(context);
    }
  }
  
  /**
   * Asserts that the configuration contains the expected ranges for the tables.
   */
  @Test
  public void testRangeSerialization() throws Throwable {
    List<String> tables = Arrays.asList("t1", "t2", "t3");
    Job job = new Job(new Configuration());
    job.setInputFormatClass(AccumuloInputFormat.class);
    job.setMapperClass (TestMapper.class);
    job.setNumReduceTasks (0);
    setConnectorInfo (job, "root", new PasswordToken (new byte[0]));
    setInputTableNames (job, tables);
    setScanAuthorizations (job, new Authorizations ());
    setMockInstance (job, "testmapinstance");
    
    HashMap<String, Collection<Range>> tblRanges = new HashMap<String, Collection<Range>>();
    for(String tbl : tables) {
      List<Range> ranges = Arrays.asList(
          new Range("a", "b"),
          new Range("c", "d"),
          new Range("e", "f") );
      tblRanges.put(tbl, ranges);
    }
    
    setRanges (job, tblRanges);
    Map<String, List<Range>> configuredRanges = getRanges (job);
    
    for(Entry<String, List<Range>> cfgRange : configuredRanges.entrySet()) {
      String tbl = cfgRange.getKey();
      HashSet<Range> originalRanges = new HashSet<Range>(tblRanges.remove(tbl));
      HashSet<Range> retrievedRanges = new HashSet<Range>(cfgRange.getValue());
      assertEquals (originalRanges.size (), retrievedRanges.size ());
      assertTrue (originalRanges.containsAll (retrievedRanges));
      assertTrue (retrievedRanges.containsAll (originalRanges));
    }
  }

//  /**
//   * Asserts that the configuration contains the expected iterators for the tables.
//   */
//  @Test
//  public void testIteratorSerialization() throws Throwable {
//    HashSet<String> tables = new HashSet<String>(Arrays.asList("t1", "t2", "t3"));
//    Job job = new Job(new Configuration());
//    job.setInputFormatClass(AccumuloInputFormat.class);
//    job.setMapperClass(TestMapper.class);
//    job.setNumReduceTasks(0);
//    AccumuloInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), tables, new Authorizations());
//    AccumuloInputFormat.setMockInstance(job.getConfiguration(), "testmapinstance");
//
//    // create + set iterators on configuration and build expected reference set
//    HashMap<String, List<IteratorSetting>> expectedIterators = new HashMap<String, List<IteratorSetting>>();
//    for(String tbl : tables) {
//      IteratorSetting isetting1 = new IteratorSetting(1, "name1", "class1"),
//          isetting2 = new IteratorSetting(2, "name2", "class3"),
//          isetting5 = new IteratorSetting(5, "name5", "class5");
//
//      expectedIterators.put(tbl, Arrays.asList(isetting1, isetting2, isetting5));
//    }
//
//    Map<String, List<IteratorSetting>> immutableView = Collections.unmodifiableMap(expectedIterators);
//    AccumuloInputFormat.setIterators(job.getConfiguration(), immutableView);
//
//    // get a list of all the iterators set on the configuration and validate that
//    // we find settings for all of the tables and assert that we actually configured
//    // the iterators we get back
//    List<AccumuloIterator> accItrs = AccumuloInputFormat.getIterators(job.getConfiguration());
//    Assert.assertFalse(accItrs.isEmpty());
//    for(AccumuloIterator accItr : accItrs) {
//      String table = accItr.getTable();
//      tables.remove( table );
//      Assert.assertTrue( expectedIterators.containsKey(table) );
//      Assert.assertTrue( findIteratorMatch( expectedIterators.get(table), accItr ) );
//    }
//
//    Assert.assertTrue(tables.isEmpty());
//  }

//  /*
//   * Helper method to do a linear search for the AccumuloIterator in the list of IteratorSettings.
//   */
//  static boolean findIteratorMatch(List<IteratorSetting> iterators, AccumuloIterator itr) {
//    boolean match = false;
//    for(IteratorSetting setting : iterators) {
//      match = setting.getPriority() == itr.getPriority() &&
//          setting.getName().equals( itr.getIteratorName() ) &&
//          setting.getIteratorClass().equals( itr.getIteratorClass() );
//      if(match) break;
//    }
//    return match;
//  }
  
  /**
   * A sample Mapper that verifies aspects of the input.
   * 
   * This mapper verifies that all keys passed to it are for the expected
   * table and that it sees exactly 100 keys.
   *
   */
  static class TestMapper extends Mapper<Key,Value,Key,Value> {
    private int count;
    private Text expectedTable;
    
    public void expectedTable(Text t) {
      this.expectedTable = t;
    }
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      count = 0;
    }

    @Override
    protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
      assertEquals(expectedTable.toString (), ((RangeInputSplit)context.getInputSplit ()).getTableName ());
      ++count;
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      super.cleanup(context);
      Assert.assertEquals(100, count);
    }
  }
}
