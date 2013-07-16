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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
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
  private TabletLocator locator;


  private Map<String,BlockingQueue<TabletServerMutations>> serverQueues;
  private DelayQueue<QCMutation> failedMutations = new DelayQueue<QCMutation>();
  private ScheduledExecutorService threadPool;
  
  private static class RQIterator implements Iterator<Result> {
    
    private BlockingQueue<Result> rq;
    private int count;
    
    public RQIterator(BlockingQueue<Result> resultQueue, int count) {
      this.rq = resultQueue;
      this.count = count;
    }
    
    @Override
    public boolean hasNext() {
      return count > 0;
    }
    
    @Override
    public Result next() {
      try {
        // TODO maybe call drainTo after take to get a batch efficiently
        Result result = rq.take();
        count--;
        return result;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
    
  }

  private static class QCMutation extends ConditionalMutation implements Delayed {
    private BlockingQueue<Result> resultQueue;
    private long resetTime;
    private long delay = 50;
    
    QCMutation(ConditionalMutation cm, BlockingQueue<Result> resultQueue) {
      super(cm);
      this.resultQueue = resultQueue;
    }
    
    @Override
    public int compareTo(Delayed o) {
      QCMutation oqcm = (QCMutation) o;
      return Long.valueOf(resetTime).compareTo(Long.valueOf(oqcm.resetTime));
    }
    
    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delay - (System.currentTimeMillis() - resetTime), TimeUnit.MILLISECONDS);
    }
    
    void resetDelay() {
      // TODO eventually timeout a mutation
      delay = Math.min(delay * 2, 5000);
      resetTime = System.currentTimeMillis();
    }
  }
  
  private BlockingQueue<TabletServerMutations> getServerQueue(String location) {
    BlockingQueue<TabletServerMutations> queue;
    synchronized (serverQueues) {
      queue = serverQueues.get(location);
      if (queue == null) {
        queue = new LinkedBlockingQueue<TabletServerMutations>();
        serverQueues.put(location, queue);
      }
    }
    return queue;
  }
  
  private void queueFailed(List<QCMutation> mutations) {
    for (QCMutation qcm : mutations) {
      qcm.resetDelay();
    }
    
    failedMutations.addAll(mutations);
  }

  private void queue(List<QCMutation> mutations) {
    List<Mutation> failures = new ArrayList<Mutation>();
    Map<String,TabletServerMutations> binnedMutations = new HashMap<String,TabletLocator.TabletServerMutations>();
    
    List<Mutation> ml = (List<Mutation>) (List<? extends Mutation>) mutations;
    
    try {
      locator.binMutations(ml, binnedMutations, failures, credentials);
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
    
    if (failures.size() > 0)
      queueFailed((List<QCMutation>) (List<? extends Mutation>) failures);

    for (Entry<String,TabletServerMutations> entry : binnedMutations.entrySet()) {
      queue(entry.getKey(), entry.getValue());
    }

  }

  private void queue(String location, TabletServerMutations mutations) {
    
    BlockingQueue<TabletServerMutations> queue = getServerQueue(location);
    
    queue.add(mutations);
    threadPool.execute(new SendTask(location));
  }

  private TabletServerMutations dequeue(String location) {
    BlockingQueue<TabletServerMutations> queue = getServerQueue(location);
    
    ArrayList<TabletServerMutations> mutations = new ArrayList<TabletLocator.TabletServerMutations>();
    queue.drainTo(mutations);
    
    if (mutations.size() == 0)
      return null;
    
    if (mutations.size() == 1) {
      return mutations.get(0);
    } else {
      // merge multiple request to a single tablet server
      TabletServerMutations tsm = mutations.get(0);
      
      for (int i = 1; i < mutations.size(); i++) {
        for (Entry<KeyExtent,List<Mutation>> entry : mutations.get(i).getMutations().entrySet()) {
          List<Mutation> list = tsm.getMutations().get(entry.getKey());
          if (list == null) {
            list = new ArrayList<Mutation>();
            tsm.getMutations().put(entry.getKey(), list);
          }
          
          list.addAll(entry.getValue());
        }
      }
      
      return tsm;
    }
  }

  ConditionalWriterImpl(Instance instance, TCredentials credentials, String tableId, Authorizations authorizations) {
    cache = Collections.synchronizedMap(new LRUMap(1000));
    this.instance = instance;
    this.credentials = credentials;
    this.tableId = new Text(tableId);
    this.auths = authorizations;
    this.ve = new VisibilityEvaluator(authorizations);
    // TODO make configurable
    this.threadPool = Executors.newScheduledThreadPool(3);
    this.locator = TabletLocator.getLocator(instance, new Text(tableId));
    this.serverQueues = new HashMap<String,BlockingQueue<TabletServerMutations>>();
    
    Runnable failureHandler = new Runnable() {
      
      @Override
      public void run() {
        try {
          List<QCMutation> mutations = new ArrayList<QCMutation>();
          failedMutations.drainTo(mutations);
          queue(mutations);
        } catch (Exception e) {
          // TODO log
          e.printStackTrace();
        }
        
      }
    };
    
    threadPool.scheduleAtFixedRate(failureHandler, 100, 100, TimeUnit.MILLISECONDS);
    
    // TODO need to shutdown thread pool
  }

  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {

    BlockingQueue resultQueue = new LinkedBlockingQueue<Result>();

    List<QCMutation> mutationList = new ArrayList<QCMutation>();

    int count = 0;

    mloop: while (mutations.hasNext()) {
      // TODO stop reading from iterator if too much memory
      ConditionalMutation mut = mutations.next();
      count++;

      for (Condition cond : mut.getConditions()) {
        if (!isVisible(cond.getVisibility())) {
          resultQueue.add(new Result(Status.INVISIBLE_VISIBILITY, mut));
          continue mloop;
        }
      }

      // copy the mutations so that even if caller changes it, it will not matter
      mutationList.add(new QCMutation(mut, resultQueue));
    }

    queue(mutationList);

    return new RQIterator(resultQueue, count);

  }

  private class SendTask implements Runnable {
    

    private String location;
    
    public SendTask(String location) {
      this.location = location;

    }
    
    @Override
    public void run() {
      TabletServerMutations mutations = dequeue(location);
      if (mutations != null)
        sendToServer(location, mutations);
    }
  }
  
  private static class CMK {

    QCMutation cm;
    KeyExtent ke;
    
    public CMK(KeyExtent ke, QCMutation cm) {
      this.ke = ke;
      this.cm = cm;
    }
  }

  private void sendToServer(String location, TabletServerMutations mutations) {
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

      ArrayList<QCMutation> ignored = new ArrayList<QCMutation>();

      for (TCMResult tcmResult : tresults) {
        if (tcmResult.status == TCMStatus.IGNORED) {
          CMK cmk = cmidToCm.get(tcmResult.cmid);
          ignored.add(cmk.cm);
          extentsToInvalidate.add(cmk.ke);
        } else {
          QCMutation qcm = cmidToCm.get(tcmResult.cmid).cm;
          qcm.resultQueue.add(new Result(fromThrift(tcmResult.status), qcm));
        }
      }


      // TODO maybe have thrift call return bad extents

      for (KeyExtent ke : extentsToInvalidate) {
        locator.invalidateCache(ke);
      }

      queueFailed(ignored);

    } catch (TTransportException e) {
      locator.invalidateCache(location);
      for (CMK cmk : cmidToCm.values())
        cmk.cm.resultQueue.add(new Result(Status.UNKNOWN, cmk.cm));
    } catch (TApplicationException tae) {
      for (CMK cmk : cmidToCm.values())
        cmk.cm.resultQueue.add(new Result(Status.UNKNOWN, cmk.cm));
      // TODO should another status be used?
      // TODO need to get server where error occurred back to client
    } catch (TException e) {
      locator.invalidateCache(location);
      for (CMK cmk : cmidToCm.values())
        cmk.cm.resultQueue.add(new Result(Status.UNKNOWN, cmk.cm));
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
      
      List<QCMutation> condMutations = (List<QCMutation>) (List<? extends Mutation>) entry.getValue();
      
      for (QCMutation cm : condMutations) {
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
