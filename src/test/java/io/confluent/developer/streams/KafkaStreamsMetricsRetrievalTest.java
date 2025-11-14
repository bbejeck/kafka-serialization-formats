
package io.confluent.developer.streams;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for StatefulStreamsRunner metric retrieval logic.
 * Verifies correct extraction of Kafka Streams metrics including:
 * - process-total, process-latency-avg, process-rate
 * - put-latency-avg, put-rate
 * - get-latency-avg, get-rate
 */
class KafkaStreamsMetricsRetrievalTest {

    private Map<MetricName, Metric> testMetrics;

    @BeforeEach
    void setUp() {
        testMetrics = new HashMap<>();
    }

    @Test
    @DisplayName("Should retrieve process-total metric correctly")
    void testProcessTotalMetric() throws Exception {
        // Given
        double expectedValue = 10000.0;
        addMetricToMap("process-total", expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "process-total");

        // Then
        assertEquals(expectedValue, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should retrieve process-latency-avg metric correctly")
    void testProcessLatencyAvgMetric() {
        // Given
        double expectedValue = 5.25;
        addMetricToMap("process-latency-avg", expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "process-latency-avg");

        // Then
        assertEquals(expectedValue, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should retrieve put-latency-avg metric correctly")
    void testPutLatencyAvgMetric() throws Exception {
        // Given
        double expectedValue = 2.75;
        addMetricToMap("put-latency-avg", expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "put-latency-avg");

        // Then
        assertEquals(expectedValue, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should retrieve get-latency-avg metric correctly")
    void testGetLatencyAvgMetric() throws Exception {
        // Given
        double expectedValue = 1.85;
        addMetricToMap("get-latency-avg", expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "get-latency-avg");

        // Then
        assertEquals(expectedValue, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should retrieve process-rate metric correctly")
    void testProcessRateMetric() throws Exception {
        // Given
        double expectedValue = 1500.0;
        addMetricToMap("process-rate", expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "process-rate");

        // Then
        assertEquals(expectedValue, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should retrieve put-rate metric correctly")
    void testPutRateMetric() throws Exception {
        // Given
        double expectedValue = 800.0;
        addMetricToMap("put-rate", expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "put-rate");

        // Then
        assertEquals(expectedValue, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should retrieve get-rate metric correctly")
    void testGetRateMetric() throws Exception {
        // Given
        double expectedValue = 850.0;
        addMetricToMap("get-rate", expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "get-rate");

        // Then
        assertEquals(expectedValue, actualValue, 0.001);
    }

    @ParameterizedTest
    @MethodSource("provideAllMetricNames")
    @DisplayName("Should retrieve all Kafka Streams metrics correctly")
    void testAllMetrics(String metricName, double expectedValue) throws Exception {
        // Given
        addMetricToMap(metricName, expectedValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, metricName);

        // Then
        assertEquals(expectedValue, actualValue, 0.001,
                "Failed to retrieve metric: " + metricName);
    }

    @Test
    @DisplayName("Should return 0.0 when metric does not exist")
    void testNonExistentMetric() throws Exception {
        // Given
        addMetricToMap("some-other-metric", 100.0);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "non-existent-metric");

        // Then
        assertEquals(0.0, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should return 0.0 when metric value is not a Number")
    void testNonNumericMetricValue() throws Exception {
        // Given
        MetricName metricName = createMetricName("string-metric");
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn("not a number");
        testMetrics.put(metricName, metric);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "string-metric");

        // Then
        assertEquals(0.0, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should return 0.0 when metrics map is empty")
    void testEmptyMetricsMap() throws Exception {
        // When
        double actualValue = invokeGetMetricValue(testMetrics, "process-total");

        // Then
        assertEquals(0.0, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should handle multiple metrics with same name prefix")
    void testMultipleMetricsWithSimilarNames() throws Exception {
        // Given
        addMetricToMap("process-total", 1000.0);
        addMetricToMap("process-rate", 500.0);
        addMetricToMap("process-latency-avg", 2.5);

        // When
        double totalValue = invokeGetMetricValue(testMetrics, "process-total");
        double rateValue = invokeGetMetricValue(testMetrics, "process-rate");
        double latencyValue = invokeGetMetricValue(testMetrics, "process-latency-avg");

        // Then
        assertEquals(1000.0, totalValue, 0.001);
        assertEquals(500.0, rateValue, 0.001);
        assertEquals(2.5, latencyValue, 0.001);
    }

    @Test
    @DisplayName("Should handle Integer metric values")
    void testIntegerMetricValue() throws Exception {
        // Given
        MetricName metricName = createMetricName("integer-metric");
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(42);
        testMetrics.put(metricName, metric);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "integer-metric");

        // Then
        assertEquals(42.0, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should handle Long metric values")
    void testLongMetricValue() throws Exception {
        // Given
        MetricName metricName = createMetricName("long-metric");
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(1000000L);
        testMetrics.put(metricName, metric);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "long-metric");

        // Then
        assertEquals(1000000.0, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should handle Float metric values")
    void testFloatMetricValue() throws Exception {
        // Given
        MetricName metricName = createMetricName("float-metric");
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(3.14f);
        testMetrics.put(metricName, metric);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "float-metric");

        // Then
        assertEquals(3.14, actualValue, 0.01);
    }

    @Test
    @DisplayName("Should handle zero values correctly")
    void testZeroMetricValue() throws Exception {
        // Given
        addMetricToMap("zero-metric", 0.0);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "zero-metric");

        // Then
        assertEquals(0.0, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should handle negative values correctly")
    void testNegativeMetricValue() throws Exception {
        // Given
        addMetricToMap("negative-metric", -5.5);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "negative-metric");

        // Then
        assertEquals(-5.5, actualValue, 0.001);
    }

    @Test
    @DisplayName("Should handle very large values correctly")
    void testLargeMetricValue() throws Exception {
        // Given
        double largeValue = 1.0e15;
        addMetricToMap("large-metric", largeValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "large-metric");

        // Then
        assertEquals(largeValue, actualValue, 1.0);
    }

    @Test
    @DisplayName("Should handle very small values correctly")
    void testSmallMetricValue() throws Exception {
        // Given
        double smallValue = 1.0e-10;
        addMetricToMap("small-metric", smallValue);

        // When
        double actualValue = invokeGetMetricValue(testMetrics, "small-metric");

        // Then
        assertEquals(smallValue, actualValue, 1.0e-12);
    }

    // Helper methods

    private void addMetricToMap(String name, double value) {
        MetricName metricName = createMetricName(name);
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(value);
        testMetrics.put(metricName, metric);
    }

    private MetricName createMetricName(String name) {
        return new MetricName(
                name,
                "kafka-streams-metrics",
                "Test metric",
                Map.of()
        );
    }

    private double invokeGetMetricValue(Map<MetricName, Metric> metrics, String metricName) {
          return KafkaStreamsRunner.getMetricValue(metrics, metricName);
    }

    private static Stream<Arguments> provideAllMetricNames() {
        return Stream.of(
                Arguments.of("process-total", 10000.0),
                Arguments.of("process-latency-avg", 5.25),
                Arguments.of("put-latency-avg", 2.75),
                Arguments.of("get-latency-avg", 1.85),
                Arguments.of("process-rate", 1500.0),
                Arguments.of("put-rate", 800.0),
                Arguments.of("get-rate", 850.0)
        );
    }
}