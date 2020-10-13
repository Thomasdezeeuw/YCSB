package site.ycsb.measurements.exporter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

/**
 * Export measurements into a machine readable CSV.
 */
public class CSVMeasurementsExporter implements MeasurementsExporter {
  private final BufferedWriter output;
  // Maps: metric -> measurement -> value (as formatted string).
  private final HashMap<String, HashMap<String, String>> values;

  // Metrics and measurements that get printed.
  private static final List<String> METRICS = Arrays.asList("READ", "SCAN", "UPDATE", "INSERT", "DELETE", "CLEANUP");
  private static final List<String> MEASUREMENTS = Arrays.asList("Operations", "AverageLatency", "MinLatency",
      "MaxLatency", "95thPercentileLatency", "99thPercentileLatency");

  public CSVMeasurementsExporter(final OutputStream output) {
    this.output = new BufferedWriter(new OutputStreamWriter(output));
    this.values = new HashMap();
  }

  public void write(final String metric, final String measurement, final int value) throws IOException {
    this.addValue(metric, measurement, String.valueOf(value));
  }

  public void write(final String metric, final String measurement, final long value) throws IOException {
    this.addValue(metric, measurement, String.valueOf(value));
  }

  public void write(final String metric, final String measurement, final double value) throws IOException {
    this.addValue(metric, measurement, String.valueOf(value));
  }

  /**
   * Add a new value to the values map.
   */
  private void addValue(final String metric, String measurement, String value) {
    // Some measurements have the time unit in the measurement variable, we move
    // that to the measured value.
    // For example "AverageLatency(us)" and "320.125", becomes "AverageLatency"
    // and "320.125 us".
    if (measurement.endsWith(")")) {
      final int idx = measurement.lastIndexOf('(');
      final String unit = measurement.substring(idx + 1, measurement.length() - 1).trim();
      // Remove the time unit from the measurement string.
      measurement = measurement.substring(0, idx).trim();
      // And add it to the value.
      value += " " + unit;
    }
    this.values.computeIfAbsent(metric, k -> new HashMap()).put(measurement, value);
  }

  public void close() throws IOException {
    this.writeHeader();
    for (final String metric : METRICS) {
      this.writeLine(metric);
    }
    // Add an empty line at the end to indicate EOF, tools like R like too
    // complain about it otherwise.
    this.output.newLine();
    this.output.close();
  }

  /**
   * Write the CSV header of all measurements collected.
   */
  private void writeHeader() throws IOException {
    final HashMap<String, String> overallValues = this.values.get("OVERALL");
    if (overallValues != null) {
      final String runtime = overallValues.get("RunTime");
      if (runtime != null) {
        this.output.write("# Total runtime: ");
        this.output.write(runtime);
        this.output.write(".\n");
      }
      final String throughput = overallValues.get("Throughput");
      if (runtime != null) {
        this.output.write("# Total throughput: ");
        this.output.write(throughput);
        this.output.write(".\n");
      }
    }

    this.output.write("# Metric");
    for (final String measurement : MEASUREMENTS) {
      this.output.write(", ");
      this.output.write(measurement);
    }
    this.output.newLine();
  }

  /**
   * Write a single line of the CSV file for the measurements.
   *
   * If measurements is null it will not print a line.
   */
  private void writeLine(final String metric) throws IOException {
    final HashMap<String, String> measurements = this.values.get(metric);
    if (measurements == null) {
      return;
    }

    this.output.write(metric);
    for (final String measurement : MEASUREMENTS) {
      this.output.write(", ");
      this.output.write(measurements.getOrDefault(measurement, "N/A"));
    }
    this.output.newLine();
  }
}
