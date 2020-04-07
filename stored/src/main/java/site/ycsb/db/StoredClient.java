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

  private PoolingHttpClientConnectionManager manager;
  private CloseableHttpClient client;
  private String url;

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

    this.url = props.getProperty(URL_PROPERTY, "http://127.0.0.1:8080");

    // Test if the server is running.
    try {
      HttpGet testRequest = new HttpGet(this.url + "/health");
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
      this.client.close();
      this.manager.shutdown();
    } catch(IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      final HttpGet request = new HttpGet(this.url + "/blob/" + key);
      final CloseableHttpResponse response = this.client.execute(request);

      try {
        final StatusLine status = response.getStatusLine();
        final int code = status.getStatusCode();
        if (code >= 500) {
          return Status.ERROR;
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

      final HttpPost request = new HttpPost(this.url + "/blob");
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
    // TODO: implement this in Stored.
    return Status.NOT_IMPLEMENTED;
  }
}
