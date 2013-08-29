package org.apache.accumulo.core.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TableKey implements WritableComparable<TableKey> {
  private Text table;
  private Key key;
  
  public TableKey() {
    table = new Text();
    key = new Key();
  }
  
  public TableKey(Text table, Key key) {
    this.table = table;
    this.key = key;
  }
  
  public Text table() {
    return table;
  }
  
  public void table(Text table) {
    this.table = table;
  }
  
  public Key key() {
    return key;
  }
  
  public void key(Key key) {
    this.key = key;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    table.readFields(in);
    key.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    table.write(out);
    key.write(out);
  }

  @Override
  public int compareTo(TableKey other) {
    int diff = table.compareTo(other.table);
    if(diff == 0) {
      diff = key.compareTo(other.key);
    }
    return diff;
  }
  
  @Override
  public String toString() {
    return "[" + table.toString() + "] " + key.toString();
  }
}
