# Лабораторна робота №3: Java Stream API, Optional та Stream Gatherers

## Варіант
Розрахунок: `22 mod 3 = 1` = **Телеметрія IoT-пристроїв у реальному часі**.

## Опис проєкту
Реалізовано сервіс `TelemetryStreamAnalyzer` для обробки нескінченних потоків метрик `SensorMetric`:
- `findFirstAnomaly` — пошук першої аномалії з використанням short-circuiting (`findFirst`) та `Optional`.
- `streamActiveMetrics` — пропуск стабільних метрик на початку (`dropWhile`) та обмеження вибірки (`limit`).
- `calculateMovingAverages` — обчислення ковзного середнього за допомогою кастомного `Gatherer`.

## Запуск тестів
./gradlew test
