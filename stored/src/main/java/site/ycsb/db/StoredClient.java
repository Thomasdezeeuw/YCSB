package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
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
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A class that wraps a HTTP client to allow it to be interfaced with YCSB.
 *
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class StoredClient extends DB {
  public static final String URL_PROPERTY = "stored.url";
  public static final String MAPPING_KEY = "stored.mapping_key";

  private PoolingHttpClientConnectionManager manager;
  private CloseableHttpClient client;
  private String baseUrl;
  // Maps key provided by the workload to it's path as returned by Stored in the
  // "Location" header of a response to a POST request.
  private HashMap<String, String> keyMapping;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    // TODO: check if pipelining is enabled.
    this.manager = new PoolingHttpClientConnectionManager();
    this.manager.setDefaultMaxPerRoute(2000); // TODO.
    this.manager.setMaxTotal(2000);
    //this.manager.setDefaultSocketConfig(/* TODO. */);

    this.client = HttpClients.custom()
      .disableContentCompression() // Not supported.
      .disableCookieManagement() // Not supported.
      .disableRedirectHandling() // Not used.
      .setConnectionManager(this.manager)
      .setUserAgent("YCSB/stored")
      .build();

    this.baseUrl = props.getProperty(URL_PROPERTY, "http://127.0.0.1:8080");

    final String mappingKey = props.getProperty(MAPPING_KEY);
    if (mappingKey == null) {
      this.keyMapping = new HashMap();
    } else {
      try {
        this.keyMapping = this.readBlob(this.baseUrl + mappingKey);
      } catch(IOException e) {
        throw new DBException(e);
      }
      if (this.keyMapping == null) {
        throw new DBException("invalid '" + MAPPING_KEY + "' setting");
      }
    }

    // Test if the server is running.
    try {
      HttpGet testRequest = new HttpGet(this.baseUrl + "/health");
      CloseableHttpResponse testResponse = this.client.execute(testRequest);

      try {
        StatusLine status = testResponse.getStatusLine();
        assert status.getStatusCode() == 200;
        assert status.getProtocolVersion() == new ProtocolVersion("HTTP", 1, 1);

        HttpEntity entity = testResponse.getEntity();
        assert entity.getContentLength() == 2;
        assert entity.getContent().toString() == "Ok";
      } finally {
        testResponse.close();
      }
    } catch(IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.insert(null, MAPPING_KEY, StringByteIterator.getByteIteratorMap(this.keyMapping));
      final String key = this.keyMapping.get(MAPPING_KEY);
      System.out.println("=====================");
      System.out.println("Set '" + MAPPING_KEY + "' to '" + key + "' for the run.");
      System.out.println("=====================");

      this.client.close();
      this.manager.shutdown();
    } catch(IOException e) {
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
    // Stored doesn't support this.
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    // Stored doesn't support updating blobs, there immutable.
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      mapper.writeValue(body, StringByteIterator.getStringMap(values));

      final HttpPost request = new HttpPost(this.baseUrl + "/blob");
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
          final String storedUrl = response.getLastHeader("Location").getValue();
          if (storedUrl == null) {
            System.err.println("Missing 'Location' header");
            return Status.ERROR;
          }
          this.keyMapping.put(key, storedUrl);
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
        } else if (code == 410) {
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
    final String storedKey = this.keyMapping.get(key);
    if (storedKey == null) {
      return null;
    }
    return this.baseUrl + storedKey;
  }
}
