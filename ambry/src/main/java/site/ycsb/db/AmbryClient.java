package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class that wraps a HTTP client to allow it to be interfaced with YCSB.
 *
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public final class AmbryClient extends DB {
  public static final String URL_PROPERTY = "ambry.url";
  public static final String MAPPING_ID = "ambry.mapping_id";

  private CloseableHttpClient client;
  private String baseUrl;

  // Maps key provided by the workload to its id as returned by Ambry in the
  // "Location" header of a response to a POST request.
  // This is `static` because it has to be shared between the different threads
  // to ensure they have the same mappings.
  private static ConcurrentHashMap<String, String> idMapping = new ConcurrentHashMap();
  // State of `keyMapping`, protects the following fields.
  private static ReentrantLock mappingStateLock = new ReentrantLock(false);
  // Whether or not the `keyMapping` is loaded, only used for `run`.
  private static boolean mappingLoaded = false;
  // Latch to wait for all threads to be done with storing the values, only used
  // in `load`.
  private static CyclicBarrier allDone;
  // Whether or not the `keyMapping` is stored in a `load` action.
  private static boolean mappingStored = false;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    this.baseUrl = props.getProperty(URL_PROPERTY, "http://127.0.0.1:1174");

    this.client = HttpClients.custom()
      .disableContentCompression()
      .disableCookieManagement()
      .disableRedirectHandling()
      .setUserAgent("YCSB/ambry")
      .build();

    if (this.isRun()) {
      final String mappingId = props.getProperty(MAPPING_ID);
      if (mappingId == null) {
        throw new DBException("missing property '" + MAPPING_ID + "'");
      } else {
        this.loadIdMapping(mappingId);
      }
    } else if (this.isLoad()) {
      this.mappingStateLock.lock();
      if (this.allDone == null) {
        this.allDone = new CyclicBarrier(this.threads());
      }
      this.mappingStateLock.unlock();
    }

    // Test if the server is running.
    try {
      HttpGet testRequest = new HttpGet(this.baseUrl + "/healthCheck");
      CloseableHttpResponse testResponse = this.client.execute(testRequest);

      try {
        StatusLine status = testResponse.getStatusLine();
        assert status.getStatusCode() == 200;
        assert status.getProtocolVersion() == new ProtocolVersion("HTTP", 1, 1);

        HttpEntity entity = testResponse.getEntity();
        assert entity.getContentLength() == 4;
        assert entity.getContent().toString() == "GOOD";
      } finally {
        testResponse.close();
      }
    } catch(IOException e) {
      throw new DBException(e);
    }
  }

  /**
   * @return Returns true if its loading a workload.
   */
  private boolean isLoad() {
    final Properties props = getProperties();
    return props.getProperty(Client.DO_TRANSACTIONS_PROPERTY) == String.valueOf(false);
  }

  /**
   * @return Returns true if its running a workload.
   */
  private boolean isRun() {
    final Properties props = getProperties();
    return props.getProperty(Client.DO_TRANSACTIONS_PROPERTY) == String.valueOf(true);
  }

  /**
   * @return Returns the number of threads.
   */
  private int threads() {
    final Properties props = getProperties();
    return Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
  }

  private void loadIdMapping(final String mappingId) throws DBException {
    try {
      this.mappingStateLock.lock();
      if (!this.mappingLoaded) {
        // Load the mapping.
        final HashMap<String, String> mapping = this.readBlob(this.baseUrl + mappingId);
        if (mapping == null) {
          throw new DBException("invalid '" + MAPPING_ID + "'");
        }

        this.idMapping.putAll(mapping);
        // Only do this once.
        this.mappingLoaded = true;
      }
      this.mappingStateLock.unlock();
    } catch(IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (this.isLoad()) {
        // Wait for all others to read this point.
        this.allDone.await();

        this.mappingStateLock.lock();
        if (!this.mappingStored) {
          final Status status = this.insert(null, MAPPING_ID, StringByteIterator.getByteIteratorMap(this.idMapping));
          if (status != Status.OK) {
            throw new DBException("failed to insert mapping");
          }

          final String key = this.idMapping.get(MAPPING_ID);
          System.out.println("=====================");
          System.out.println("Next run use '-p " + MAPPING_ID + "=" + key + "'.");
          System.out.println("=====================");
          // Only do this once.
          this.mappingStored = true;
        }
        this.mappingStateLock.unlock();
      }

      this.client.close();
    } catch(IOException | InterruptedException | BrokenBarrierException e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    final String url = this.createUrl(key);
    if (url == null) {
      return Status.ERROR;
    }

    try {
      final HttpGet request = new HttpGet(url);
      final CloseableHttpResponse response = this.client.execute(request);

      try {
        final StatusLine status = response.getStatusLine();
        final int code = status.getStatusCode();
        if (code >= 500) {
          return Status.ERROR;
        } else if (code == 404 || code == 410) {
          return Status.NOT_FOUND;
        } else if (code >= 400) {
          return Status.BAD_REQUEST;
        }

        // Parse the response as a JSON map.
        final HttpEntity entity = response.getEntity();
        final ObjectMapper mapper = new ObjectMapper();
        final HashMap<String, String> values = mapper.readValue(entity.getContent(), HashMap.class);

        if (fields != null) {
          // Remove the fields not required.
          values.keySet().retainAll(fields);
        }

        StringByteIterator.putAllAsByteIterators(result, values);

        return Status.OK;
      } finally {
        response.close();
      }
    } catch(IOException e) {
      return Status.ERROR;
    }
  }

  private HashMap<String, String> readBlob(final String url) throws IOException {
    final HttpGet request = new HttpGet(url);
    final CloseableHttpResponse response = this.client.execute(request);

    try {
      final StatusLine status = response.getStatusLine();
      if (status.getStatusCode() != 200) {
        return null;
      }

      // Parse the response as a JSON map.
      final HttpEntity entity = response.getEntity();
      final ObjectMapper mapper = new ObjectMapper();
      final HashMap<String, String> values = mapper.readValue(entity.getContent(), HashMap.class);

      return values;
    } finally {
      response.close();
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      mapper.writeValue(body, StringByteIterator.getStringMap(values));

      final HttpPost request = new HttpPost(this.baseUrl);
      request.addHeader("x-ambry-service-id", "YCSB/ambry");
      request.addHeader("x-ambry-content-type", "application/json");
      final ByteArrayEntity requestEntity = new ByteArrayEntity(body.toByteArray());
      request.setEntity(requestEntity);

      final CloseableHttpResponse response = this.client.execute(request);

      try {
        final StatusLine status = response.getStatusLine();
        final int code = status.getStatusCode();
        final HttpEntity entity = response.getEntity();

        if (code >= 500) {
          System.err.print("Server error: '");
          entity.writeTo(System.err);
          System.err.print("'\n");
          return Status.ERROR;
        } else if (code >= 400) {
          System.err.print("Bad request: '");
          entity.writeTo(System.err);
          System.err.print("'\n");
          return Status.BAD_REQUEST;
        } else if (code == 201) {
          final String id = response.getLastHeader("Location").getValue();
          if (id == null) {
            System.err.println("Missing 'Location' header");
            return Status.ERROR;
          }
          this.idMapping.put(key, id);
          EntityUtils.consume(entity);
          return Status.OK;
        } else {
          System.err.print("unknown response: '");
          entity.writeTo(System.err);
          System.err.print("'\n");
          return Status.ERROR;
        }
      } finally {
        response.close();
      }
    } catch(IOException e) {
      System.out.println("Exception throw: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    final String url = this.createUrl(key);
    if (url == null) {
      return Status.ERROR;
    }

    try {
      final HttpDelete request = new HttpDelete(url);
      final CloseableHttpResponse response = this.client.execute(request);

      try {
        final StatusLine status = response.getStatusLine();
        final int code = status.getStatusCode();
        if (code >= 500) {
          return Status.ERROR;
        } else if (code == 404) {
          return Status.NOT_FOUND;
        } else if (code == 202) {
          return Status.OK;
        } else {
          return Status.BAD_REQUEST;
        }
      } finally {
        response.close();
      }
    } catch(IOException e) {
      return Status.ERROR;
    }
  }

  /**
   * Creates the url for the provided key.
   *
   * @param key The YCSB provided key.
   *
   * @return The full url to get the blob from, or null if no mapping for key
   * can be found.
   */
  private String createUrl(final String key) {
    final String id = this.idMapping.get(key);
    if (id == null) {
      return null;
    }
    return this.baseUrl + id;
  }
}
