# Kafka Serialization Benchmarks

A comprehensive JMH (Java Microbenchmark Harness) benchmark suite comparing various serialization formats and libraries commonly used with Apache Kafka.

## Purpose

This project provides performance benchmarks for different serialization approaches in Kafka-based applications. It measures both serialization and deserialization performance across multiple formats, helping developers make informed decisions about which serialization strategy best fits their use case.

## Serialization Formats Covered

### 1. **Jackson (JSON)**
- **Type**: Text-based
- **Use Case**: Human-readable, REST APIs, debugging
- **Trade-offs**: Larger payload size, slower than binary formats

### 2. **Apache Avro**
- **Type**: Binary, schema-based
- **Variants Tested**:
  - Raw Avro serialization
  - Confluent Schema Registry integration
- **Use Case**: Schema evolution, data lakes, analytics pipelines
- **Trade-offs**: Compact, schema evolution support, requires Schema Registry for full benefits

### 3. **Protocol Buffers (Protobuf)**
- **Type**: Binary, schema-based
- **Variants Tested**:
  - Raw Protobuf serialization
  - Confluent Schema Registry integration
- **Use Case**: gRPC services, polyglot environments, microservices
- **Trade-offs**: Fast, compact, strong typing, excellent cross-language support

### 4. **Simple Binary Encoding (SBE)**
- **Type**: Binary, zero-copy
- **Variants Tested**:
  - Direct ByteBuffer (26 bytes)
  - Non-direct ByteBuffer
  - Direct ByteBuffer (1024 bytes)
- **Use Case**: Ultra-low latency trading systems, high-frequency data
- **Trade-offs**: Fastest binary format, no schema evolution, fixed-size fields

### 5. **Kryo**
- **Type**: Binary, Java-specific
- **Use Case**: Internal Java-to-Java communication, fast prototyping
- **Trade-offs**: Very fast, compact, but Java-only and fragile across versions

### 6. **Apache Fory**
- **Type**: Binary, modern JVM-optimized
- **Use Case**: High-performance Java serialization, modern JVM features
- **Trade-offs**: Fast, modern, supports Java 21+, requires class registration

## Benchmark Metrics

The benchmarks measure:
- **Average Time**: Mean time per operation (microseconds)
- **Throughput**: Operations per time unit

Each benchmark includes:
- 5 warmup iterations
- 5 measurement iterations
- 1 JVM fork

## Prerequisites

- **Java 21+** 
- **Gradle 9.x**
- **8GB+ RAM recommended** for JMH benchmarks
- **async-profiler** (optional, for flame graph generation)

## Installing async-profiler

async-profiler is a low-overhead sampling profiler for Java that can generate flame graphs for CPU and memory profiling [[5]](https://github.com/async-profiler/async-profiler).

### macOS Installation

1. Download the latest macOS release:
