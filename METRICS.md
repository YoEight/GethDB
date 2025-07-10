# Geth Engine Metrics Documentation

This document describes the metrics exposed by the Geth engine and monitored in the Grafana dashboard.

## Overview

The Geth engine exposes Prometheus-format metrics that provide insights into system performance, I/O operations, and resource utilization. These metrics are organized into several categories for comprehensive monitoring.

## System Resource Metrics

### CPU Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_sys_cpu_usage_percent` | Gauge | Current CPU usage as a percentage (0-100). Indicates how much CPU resources the Geth engine is consuming. |

**Usage**: Monitor for high CPU usage that might indicate performance bottlenecks or heavy computational load.

### Memory Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_sys_memory_total_bytes` | Gauge | Total system memory available in bytes. This represents the total RAM capacity of the system. |
| `geth_sys_memory_used_bytes` | Gauge | Currently used system memory in bytes. Shows how much RAM is being utilized. |

**Usage**: Calculate memory usage percentage and monitor for memory pressure or potential out-of-memory conditions.

### Swap Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_sys_swap_total_bytes` | Gauge | Total swap space available in bytes. Represents the disk space allocated for virtual memory. |
| `geth_sys_swap_used_bytes` | Gauge | Currently used swap space in bytes. Shows how much swap is being utilized. |

**Usage**: Monitor swap usage to detect memory pressure. High swap usage indicates the system is running low on physical memory.

## I/O Operation Metrics

### Read Operations

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_read_entry_entries_total` | Counter | Total number of read entries processed since startup. Continuously increments with each read operation. |

**Usage**: Track read operation volume and calculate read rates using `rate()` function.

### Write Operations

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_write_propose_event_events_total` | Counter | Total number of write propose events processed since startup. Tracks write operation frequency. |

**Usage**: Monitor write operation volume and calculate write rates to understand database activity.

### Cache Performance

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_index_cache_miss_misses_total` | Counter | Total number of index cache misses since startup. High values indicate poor cache performance. |

**Usage**: Monitor cache efficiency. High miss rates may indicate need for cache tuning or insufficient cache size.

## I/O Size Distribution Metrics

### Read Size Histograms

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_read_size_bytes_bucket` | Histogram | Distribution of read operation sizes across different byte ranges (buckets). |
| `geth_read_size_bytes_sum` | Histogram | Total bytes read across all operations. |
| `geth_read_size_bytes_count` | Histogram | Total number of read operations. |

**Buckets**: 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, +Inf

### Write Size Histograms

| Metric Name | Type | Description |
|-------------|------|-------------|
| `geth_write_size_bytes_bucket` | Histogram | Distribution of write operation sizes across different byte ranges (buckets). |
| `geth_write_size_bytes_sum` | Histogram | Total bytes written across all operations. |
| `geth_write_size_bytes_count` | Histogram | Total number of write operations. |

**Buckets**: 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, +Inf

**Usage**: Analyze I/O patterns, calculate percentiles, and understand the distribution of operation sizes to optimize performance.

## Common Metric Calculations

### Rate Calculations
```promql
# Read operations per second
rate(geth_read_entry_entries_total[5m])

# Write operations per second
rate(geth_write_propose_event_events_total[5m])

# Cache misses per second
rate(geth_index_cache_miss_misses_total[5m])
```

### Memory Usage Percentage
```promql
# Memory usage as percentage
geth_sys_memory_used_bytes / geth_sys_memory_total_bytes * 100
```

### Average I/O Sizes
```promql
# Average read size
rate(geth_read_size_bytes_sum[5m]) / rate(geth_read_size_bytes_count[5m])

# Average write size
rate(geth_write_size_bytes_sum[5m]) / rate(geth_write_size_bytes_count[5m])
```

### I/O Throughput
```promql
# Read throughput (bytes per second)
rate(geth_read_size_bytes_sum[5m])

# Write throughput (bytes per second)
rate(geth_write_size_bytes_sum[5m])
```

### Percentile Analysis
```promql
# 90th percentile read size
histogram_quantile(0.90, rate(geth_read_size_bytes_bucket[5m]))

# 99th percentile write size
histogram_quantile(0.99, rate(geth_write_size_bytes_bucket[5m]))
```

## Alerting Recommendations

### Critical Alerts
- **High CPU Usage**: `geth_sys_cpu_usage_percent > 90`
- **High Memory Usage**: `(geth_sys_memory_used_bytes / geth_sys_memory_total_bytes) * 100 > 90`
- **Swap Usage**: `geth_sys_swap_used_bytes > 0` (any swap usage may indicate memory pressure)

### Warning Alerts
- **Moderate CPU Usage**: `geth_sys_cpu_usage_percent > 70`
- **Moderate Memory Usage**: `(geth_sys_memory_used_bytes / geth_sys_memory_total_bytes) * 100 > 70`
- **High Cache Miss Rate**: `rate(geth_index_cache_miss_misses_total[5m]) > threshold`

## Dashboard Panels

The Grafana dashboard includes the following visualization panels:

1. **System Overview**: CPU usage, memory usage, memory details, swap usage
2. **I/O Operations**: Read/write operation rates, cache miss rate
3. **I/O Size Analysis**: Read/write size distributions with percentiles
4. **Summary Stats**: Total operation counters, system resource timeline

## Troubleshooting

### No Data in Panels
- Verify Prometheus is scraping the Geth metrics endpoint
- Check that metric names match exactly (case-sensitive)
- Ensure time range covers period when metrics were generated
- Confirm Prometheus datasource is configured correctly

### High Resource Usage
- Check CPU and memory trends over time
- Analyze I/O patterns for unusual spikes
- Review cache performance metrics
- Consider scaling resources if sustained high usage

### Performance Issues
- Monitor I/O operation rates and sizes
- Check for large operations that might cause bottlenecks
- Analyze cache miss patterns
- Review histogram data for operation size distribution

## Metric Labels

All metrics include the following labels:
- `job`: "geth-engine"
- `otel_scope_name`: "geth-engine"
- `otel_scope_schema_url`: ""
- `otel_scope_version`: ""

These labels can be used for filtering and aggregation in Prometheus queries.
