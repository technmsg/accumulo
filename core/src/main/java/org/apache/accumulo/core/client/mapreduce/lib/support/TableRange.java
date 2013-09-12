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
package org.apache.accumulo.core.client.mapreduce.lib.support;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Pairs together a table name and a range.
 */
public class TableRange implements Writable {
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
