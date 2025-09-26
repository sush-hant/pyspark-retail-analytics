# Spark Configuration Recommendations for Retail Data Processing

## Overview
This document provides Spark configuration recommendations for optimal performance when processing large retail datasets (>1M records) in the retail analytics pipeline.

## Core Configuration Settings

### Adaptive Query Execution (AQE)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

### Serialization Optimization
```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

### Memory Management
```python
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.executor.memoryFraction", "0.8")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
```

## Cluster Size Recommendations

### Small Dataset (1M - 10M records)
- **Executors**: 2-4
- **Executor Memory**: 2-4GB
- **Executor Cores**: 2-4
- **Partitions**: 50-100

### Medium Dataset (10M - 100M records)
- **Executors**: 4-8
- **Executor Memory**: 4-8GB
- **Executor Cores**: 4-6
- **Partitions**: 100-200

### Large Dataset (100M+ records)
- **Executors**: 8-16
- **Executor Memory**: 8-16GB
- **Executor Cores**: 4-8
- **Partitions**: 200-500

## Performance Tuning Guidelines

### Partitioning Strategy
- Use 10,000-50,000 records per partition for optimal performance
- Repartition by time dimensions (Year, Month) before aggregations
- Coalesce before expensive operations like window functions

### Caching Strategy
- Cache DataFrames that are accessed multiple times
- Use `.cache()` for DataFrames used in multiple actions
- Unpersist cached DataFrames when no longer needed

### Join Optimization
- Enable broadcast joins for small lookup tables
- Use bucketing for large join operations
- Consider salting for skewed join keys

### Window Function Optimization
- Coalesce partitions before window operations
- Use appropriate partitioning in window specifications
- Consider using approximate functions when exact results aren't required

## Monitoring and Debugging

### Enable Spark UI
Access Spark UI at `http://<driver-node>:4040` to monitor:
- Job execution times
- Stage details and task distribution
- Storage tab for cached DataFrames
- SQL tab for query plans

### Key Metrics to Monitor
- **Task Duration**: Should be relatively uniform
- **Shuffle Read/Write**: Minimize shuffle operations
- **GC Time**: Should be <10% of task time
- **Spill**: Minimize disk spills

### Common Performance Issues
1. **Data Skew**: Use salting or custom partitioning
2. **Small Files**: Use coalesce before writing
3. **Memory Issues**: Increase executor memory or reduce partition size
4. **Shuffle Overhead**: Reduce number of shuffles through operation combining

## Environment-Specific Settings

### Databricks
```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

### EMR
```python
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.speculation", "true")
```

### On-Premises
```python
spark.conf.set("spark.local.dir", "/tmp/spark")
spark.conf.set("spark.sql.warehouse.dir", "/opt/spark/warehouse")
```

## Best Practices Summary

1. **Always enable Adaptive Query Execution** for automatic optimization
2. **Cache strategically** - only DataFrames used multiple times
3. **Partition appropriately** - balance parallelism with overhead
4. **Combine operations** to reduce data passes
5. **Monitor performance** using Spark UI and logs
6. **Test configurations** with representative data volumes
7. **Clean up resources** by unpersisting unused cached data

## Testing Your Configuration

Use the following code to test your Spark configuration:

```python
# Test data loading performance
start_time = time.time()
df = spark.read.csv("your_data_path", header=True, inferSchema=True)
df.cache().count()
print(f"Data loading time: {time.time() - start_time:.2f} seconds")

# Test aggregation performance
start_time = time.time()
result = df.groupBy("some_column").agg(sum("numeric_column")).collect()
print(f"Aggregation time: {time.time() - start_time:.2f} seconds")
```

Adjust configurations based on these performance tests for your specific environment and data characteristics.
