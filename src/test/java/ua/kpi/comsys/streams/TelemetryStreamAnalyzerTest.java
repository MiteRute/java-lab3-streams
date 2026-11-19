package ua.kpi.comsys.streams;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TelemetryStreamAnalyzerTest {

    private final TelemetryStreamAnalyzer analyzer = new TelemetryStreamAnalyzer();

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void findFirstAnomaly_onInfiniteStream_returnsFirstMatchWithoutHanging() {
        Stream<SensorMetric> infiniteStream = Stream.iterate(1, i -> i + 1)
                .map(i -> new SensorMetric("sensor-1", "TEMP", i * 10.0, Instant.now()));

        Optional<SensorMetric> result = analyzer.findFirstAnomaly(infiniteStream, "sensor-1", 55.0);

        assertTrue(result.isPresent());
        assertEquals(60.0, result.map(SensorMetric::value).orElseThrow());
    }

    @Test
    void findFirstAnomaly_whenNoMatch_returnsEmptyOptional() {
        Stream<SensorMetric> finiteStream = Stream.of(
                new SensorMetric("sensor-1", "TEMP", 20.0, Instant.now()),
                new SensorMetric("sensor-2", "TEMP", 100.0, Instant.now())
        );

        Optional<SensorMetric> result = analyzer.findFirstAnomaly(finiteStream, "sensor-1", 50.0);

        assertTrue(result.isEmpty());
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void streamActiveMetrics_onInfiniteStream_skipsNormalAndLimitsOutput() {
        Stream<SensorMetric> infiniteStream = Stream.iterate(1, i -> i + 1)
                .map(i -> new SensorMetric("dev-1", "CPU", i * 10.0, Instant.now()));

        List<SensorMetric> active = analyzer.streamActiveMetrics(infiniteStream, 30.0, 3);

        assertEquals(3, active.size());
        assertEquals(List.of(40.0, 50.0, 60.0), active.stream().map(SensorMetric::value).toList());
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void calculateMovingAverages_onInfiniteStream_computesCorrectAverages() {
        Stream<SensorMetric> infiniteStream = Stream.iterate(10.0, v -> v + 10.0)
                .map(v -> new SensorMetric("dev-1", "TEMP", v, Instant.now()));
        List<Double> averages = analyzer.calculateMovingAverages(infiniteStream, "TEMP", 3, 4);

        assertEquals(List.of(10.0, 15.0, 20.0, 30.0), averages);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void calculateMovingAverages_withStreamGenerate_filtersByMetricType() {
        Stream<SensorMetric> infiniteStream = Stream.generate(
                () -> new SensorMetric("dev-1", "HUMIDITY", 50.0, Instant.now())
        );

        List<Double> averages = analyzer.calculateMovingAverages(infiniteStream, "HUMIDITY", 2, 3);

        assertEquals(List.of(50.0, 50.0, 50.0), averages);
    }

    @Test
    void validation_throwsIllegalArgumentExceptionOnInvalidInputs() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstAnomaly(null, "dev-1", 10.0));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstAnomaly(Stream.empty(), null, 10.0));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.streamActiveMetrics(null, 10.0, 5));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.streamActiveMetrics(Stream.empty(), 10.0, -1));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.calculateMovingAverages(null, "TEMP", 3, 5));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.calculateMovingAverages(Stream.empty(), null, 3, 5));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.calculateMovingAverages(Stream.empty(), "TEMP", 0, 5));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.calculateMovingAverages(Stream.empty(), "TEMP", 3, -1));
    }
}