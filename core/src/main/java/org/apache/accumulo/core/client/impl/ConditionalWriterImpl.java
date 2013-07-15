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

package org.apache.accumulo.core.client.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletServerMutations;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.TCMResult;
import org.apache.accumulo.core.data.thrift.TCMStatus;
import org.apache.accumulo.core.data.thrift.TCondition;
import org.apache.accumulo.core.data.thrift.TConditionalMutation;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;


class ConditionalWriterImpl implements ConditionalWriter {
  
  private Text tableId;
  private Authorizations auths;
  private VisibilityEvaluator ve;
  private Map cache;
  private Instance instance;
  private TCredentials credentials;
  
  ConditionalWriterImpl(Instance instance, TCredentials credentials, String tableId, Authorizations authorizations) {
    cache = Collections.synchronizedMap(new LRUMap(1000));
    this.instance = instance;
    this.credentials = credentials;
    this.tableId = new Text(tableId);
    this.auths = authorizations;
    this.ve = new VisibilityEvaluator(authorizations);
  }

  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
    

    TabletLocator locator = TabletLocator.getLocator(instance, tableId);
    
    List<Mutation> mutationList = new ArrayList<Mutation>();

    ArrayList<Result> results = new ArrayList<Result>();
    
    mloop: while (mutations.hasNext()) {
      ConditionalMutation mut = mutations.next();

      for (Condition cond : mut.getConditions()) {
        if (!isVisible(cond.getVisibility())) {
          results.add(new Result(Status.INVISIBLE_VISIBILITY, mut));
          continue mloop;
        }
      }

      mutationList.add(mut);
    }

    try {
      List<Mutation> ignored = (List<Mutation>) (ArrayList<? extends Mutation>) sendToServers(locator, mutationList, results);
      
      while (ignored.size() > 0) {
        // TODO requeue ignored and return whats done for iteration
        ignored = (List<Mutation>) (ArrayList<? extends Mutation>) sendToServers(locator, ignored, results);
      }

      return results.iterator();
    } catch (AccumuloException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (AccumuloSecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (TableNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return null;
  }

  private class SendTask implements Runnable {
    
    private TabletServerMutations mutations;
    private String location;
    private ArrayList<Result> results;
    private List<ConditionalMutation> ignored;
    private TabletLocator locator;
    
    public SendTask(String location, TabletServerMutations mutations, ArrayList<Result> results, ArrayList<ConditionalMutation> ignored, TabletLocator locator) {
      this.location = location;
      this.mutations = mutations;
      this.results = results;
      this.ignored = ignored;
      this.locator = locator;
    }
    
    @Override
    public void run() {
      ArrayList<Result> tmpResults = new ArrayList<ConditionalWriter.Result>();
      List<ConditionalMutation> tmpIgnored = new ArrayList<ConditionalMutation>();
      
      sendToServer(location, mutations, tmpResults, tmpIgnored, locator);
      
      synchronized (results) {
        results.addAll(tmpResults);
        ignored.addAll(tmpIgnored);
      }
    }
  }
  protected ArrayList<ConditionalMutation> sendToServers(TabletLocator locator, List<Mutation> mutationList, ArrayList<Result> results) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {

    List<Mutation> failures = new ArrayList<Mutation>();
    Map<String,TabletServerMutations> binnedMutations = new HashMap<String,TabletLocator.TabletServerMutations>();

    do {
      binnedMutations.clear();
      failures.clear();

      locator.binMutations(mutationList, binnedMutations, failures, credentials);
      
      // TODO queue failed mutations to be retried in a bit and write what can be written
      if (failures.size() > 0)
        UtilWaitThread.sleep(100);

    } while (failures.size() > 0);
    
    ArrayList<ConditionalMutation> ignored = new ArrayList<ConditionalMutation>();

    ArrayList<Thread> threads = new ArrayList<Thread>();

    for (Entry<String,TabletServerMutations> entry : binnedMutations.entrySet()) {
      Thread t = new Thread(new SendTask(entry.getKey(), entry.getValue(), results, ignored, locator));
      threads.add(t);
      t.start();
    }
    
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    return ignored;
  }
  
  private static class CMK {

    ConditionalMutation cm;
    KeyExtent ke;
    
    public CMK(KeyExtent ke, ConditionalMutation cm) {
      this.ke = ke;
      this.cm = cm;
    }
  }

  private void sendToServer(String location, TabletServerMutations mutations, ArrayList<Result> results, List<ConditionalMutation> ignored,
      TabletLocator locator) {
    TabletClientService.Iface client = null;
    
    TInfo tinfo = Tracer.traceInfo();

    Map<Long,CMK> cmidToCm = new HashMap<Long,CMK>();
    MutableLong cmid = new MutableLong(0);

    try {
      client = ThriftUtil.getTServerClient(location, instance.getConfiguration());

      Map<TKeyExtent,List<TConditionalMutation>> tmutations = new HashMap<TKeyExtent,List<TConditionalMutation>>();

      convertMutations(mutations, cmidToCm, cmid, tmutations);

      List<TCMResult> tresults = client.conditionalUpdate(tinfo, credentials, ByteBufferUtil.toByteBuffers(auths.getAuthorizations()), tmutations);

      HashSet<KeyExtent> extentsToInvalidate = new HashSet<KeyExtent>();

      for (TCMResult tcmResult : tresults) {
        if (tcmResult.status == TCMStatus.IGNORED) {
          CMK cmk = cmidToCm.get(tcmResult.cmid);
          ignored.add(cmk.cm);
          extentsToInvalidate.add(cmk.ke);
        } else {
          results.add(new Result(fromThrift(tcmResult.status), cmidToCm.get(tcmResult.cmid).cm));
        }
      }

      // TODO maybe have thrift call return bad extents

      for (KeyExtent ke : extentsToInvalidate) {
        locator.invalidateCache(ke);
      }

    } catch (TTransportException e) {
      locator.invalidateCache(location);
      for (CMK cmk : cmidToCm.values())
        results.add(new Result(Status.UNKNOWN, cmk.cm));
    } catch (TApplicationException tae) {
      for (CMK cmk : cmidToCm.values())
        results.add(new Result(Status.UNKNOWN, cmk.cm));
      // TODO should another status be used?
      // TODO need to get server where error occurred back to client
    } catch (TException e) {
      locator.invalidateCache(location);
      for (CMK cmk : cmidToCm.values())
        results.add(new Result(Status.UNKNOWN, cmk.cm));
    } finally {
      ThriftUtil.returnClient((TServiceClient) client);
    }
  }

  private Status fromThrift(TCMStatus status) {
    switch (status) {
      case ACCEPTED:
        return Status.ACCEPTED;
      case REJECTED:
        return Status.REJECTED;
      case VIOLATED:
        return Status.VIOLATED;
      default:
        throw new IllegalArgumentException(status.toString());
    }
  }

  private void convertMutations(TabletServerMutations mutations, Map<Long,CMK> cmidToCm, MutableLong cmid,
      Map<TKeyExtent,List<TConditionalMutation>> tmutations) {

    // TODO compress repeated iterator configurations

    Set<Entry<KeyExtent,List<Mutation>>> es = mutations.getMutations().entrySet();
    for (Entry<KeyExtent,List<Mutation>> entry : es) {
      TKeyExtent tke = entry.getKey().toThrift();
      ArrayList<TConditionalMutation> tcondMutaions = new ArrayList<TConditionalMutation>();
      
      List<ConditionalMutation> condMutations = (List<ConditionalMutation>) (List<? extends Mutation>) entry.getValue();
      
      for (ConditionalMutation cm : condMutations) {
        TMutation tm = cm.toThrift();
        
        
        List<TCondition> conditions = convertConditions(cm);

        cmidToCm.put(cmid.longValue(), new CMK(entry.getKey(), cm));
        TConditionalMutation tcm = new TConditionalMutation(conditions, tm, cmid.longValue());
        cmid.increment();
        tcondMutaions.add(tcm);
      }
      
      tmutations.put(tke, tcondMutaions);
    }
  }

  private List<TCondition> convertConditions(ConditionalMutation cm) {
    List<TCondition> conditions = new ArrayList<TCondition>(cm.getConditions().size());
    
    for (Condition cond : cm.getConditions()) {
      long ts = 0;
      boolean hasTs = false;
      
      if (cond.getTimestamp() != null) {
        ts = cond.getTimestamp();
        hasTs = true;
      }
      
      IteratorSetting[] iters = cond.getIterators();
      
      List<IterInfo> ssiList = new ArrayList<IterInfo>(iters.length);
      Map<String,Map<String,String>> sso = new HashMap<String,Map<String,String>>();
      
      if (iters.length == 0) {
        ssiList = Collections.emptyList();
        sso = Collections.emptyMap();
      } else {
        ssiList = new ArrayList<IterInfo>(iters.length);
        sso = new HashMap<String,Map<String,String>>();
        
        for (IteratorSetting is : iters) {
          ssiList.add(new IterInfo(is.getPriority(), is.getIteratorClass(), is.getName()));
          sso.put(is.getName(), is.getOptions());
        }
      }
      
      TCondition tc = new TCondition(ByteBufferUtil.toByteBuffers(cond.getFamily()), ByteBufferUtil.toByteBuffers(cond.getQualifier()),
          ByteBufferUtil.toByteBuffers(cond.getVisibility()), ts, hasTs, ByteBufferUtil.toByteBuffers(cond.getValue()), ssiList, sso);
      
      conditions.add(tc);
    }
    
    return conditions;
  }

  private boolean isVisible(ByteSequence cv) {
    Text testVis = new Text(cv.toArray());
    if (testVis.getLength() == 0)
      return true;
    
    Boolean b = (Boolean) cache.get(testVis);
    if (b != null)
      return b;
    
    try {
      Boolean bb = ve.evaluate(new ColumnVisibility(testVis));
      cache.put(new Text(testVis), bb);
      return bb;
    } catch (VisibilityParseException e) {
      return false;
    } catch (BadArgumentException e) {
      return false;
    }
  }

  public Result write(ConditionalMutation mutation) {
    return write(Collections.singleton(mutation).iterator()).next();
  }
  
  public void setTimeout(long timeOut, TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }
  
  public long getTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }
  
}
