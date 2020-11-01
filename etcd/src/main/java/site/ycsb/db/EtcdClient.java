package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * etcd client binding for YCSB.
 *
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public final class EtcdClient extends DB {
  public static final String URL_PROPERTY = "etcd.url";

  private KV client;

  @Override
  public void init() throws DBException {
    Properties props = this.getProperties();
    final String url = props.getProperty(URL_PROPERTY, "http://127.0.0.1:2379");
    this.client = Client.builder().endpoints(url).build().getKVClient();
  }

  @Override
  public void cleanup() throws DBException {
    this.client.close();
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      final ByteSequence keyBytes = ByteSequence.from(key.getBytes());
      // First *get* the future, than *get* the response from the future, than
      // *get* the key-value list from the response, then *get* the first
      // key-value pair. Yey *get*!
      final KeyValue keyValue = this.client.get(keyBytes).get().getKvs().get(0);
      final ByteSequence value = keyValue.getValue();

      // Parse the value as a JSON map.
      final ObjectMapper mapper = new ObjectMapper();
      final HashMap<String, String> values = mapper.readValue(value.getBytes(), HashMap.class);
      if (fields != null) {
        // Remove the fields not required.
        values.keySet().retainAll(fields);
      }

      // Store all values in the result map.
      StringByteIterator.putAllAsByteIterators(result, values);
      return Status.OK;
    } catch(IOException | InterruptedException | ExecutionException e) {
      System.out.println("Exception thrown while reading: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    // etcd doesn't support scanning.
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    return this.insert(table, key, values);
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      final ByteSequence keyBytes = ByteSequence.from(key.getBytes());

      final ObjectMapper mapper = new ObjectMapper();
      final ByteArrayOutputStream value = new ByteArrayOutputStream();
      mapper.writeValue(value, StringByteIterator.getStringMap(values));

      this.client.put(keyBytes, ByteSequence.from(value.toByteArray())).get();
      return Status.OK;
    } catch(IOException | InterruptedException | ExecutionException e) {
      System.out.println("Exception thrown inserting: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      final ByteSequence keyBytes = ByteSequence.from(key.getBytes());
      this.client.delete(keyBytes).get();
      return Status.OK;
    } catch(InterruptedException | ExecutionException e) {
      System.out.println("Exception thrown while deleting: " + e.getMessage());
      return Status.ERROR;
    }
  }
}
