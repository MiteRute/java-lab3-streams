package ua.kpi.comsys.streams;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

public class TelemetryStreamAnalyzer {

    public Optional<SensorMetric> findFirstAnomaly(
            Stream<SensorMetric> stream,
            String targetDeviceId,
            double criticalThreshold
    ) {
        if (stream == null || targetDeviceId == null) {
            throw new IllegalArgumentException("Stream and targetDeviceId must not be null");
        }

        return stream
                .filter(Objects::nonNull)
                .filter(metric -> targetDeviceId.equals(metric.deviceId()))
                .filter(metric -> metric.value() > criticalThreshold)
                .findFirst();
    }

    public List<SensorMetric> streamActiveMetrics(
            Stream<SensorMetric> stream,
            double normalThreshold,
            int maxCount
    ) {
        if (stream == null) {
            throw new IllegalArgumentException("Stream must not be null");
        }
        if (maxCount < 0) {
            throw new IllegalArgumentException("maxCount must not be negative");
        }

        return stream
                .filter(Objects::nonNull)
                .dropWhile(metric -> metric.value() <= normalThreshold)
                .limit(maxCount)
                .toList();
    }

    public List<Double> calculateMovingAverages(
            Stream<SensorMetric> stream,
            String metricType,
            int windowSize,
            int outputLimit
    ) {
        if (stream == null || metricType == null) {
            throw new IllegalArgumentException("Stream and metricType must not be null");
        }
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be greater than 0");
        }
        if (outputLimit < 0) {
            throw new IllegalArgumentException("outputLimit must not be negative");
        }
        if (outputLimit == 0) {
            return List.of();
        }

        return stream
                .filter(Objects::nonNull)
                .filter(metric -> metricType.equals(metric.metricType()))
                .gather(movingAverageGatherer(windowSize))
                .limit(outputLimit)
                .toList();
    }

    private Gatherer<SensorMetric, ?, Double> movingAverageGatherer(int windowSize) {
        class WindowState {
            final Deque<Double> window = new ArrayDeque<>(windowSize);
            double sum = 0.0;
        }

        return Gatherer.ofSequential(
                WindowState::new,
                Gatherer.Integrator.ofGreedy((state, metric, downstream) -> {
                    double val = metric.value();
                    state.window.addLast(val);
                    state.sum += val;

                    if (state.window.size() > windowSize) {
                        state.sum -= state.window.removeFirst();
                    }

                    double average = state.sum / state.window.size();
                    return downstream.push(average);
                })
        );
    }
}