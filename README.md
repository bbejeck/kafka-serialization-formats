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
- 3 warmup iterations
- 3 measurement iterations
- 1 JVM fork



## Optional Components

## Installing async-profiler
async-profiler is a low-overhead sampling profiler for Java that can generate flame graphs for CPU and memory profiling. 
Install the correct version for your OS [from the GitHub project](https://github.com/async-profiler/async-profiler).
1. unzip the archive
2. save `config.properties.original` as `config.properties`
3. set `envName` to `DYLD_LIBRARY_PATH`
4. set `envValue` to the path of the `lib` directory where you extracted the archive
5. run: `./gradlew jmh` to run the benchmarks with async-profiler

1. Download the latest macOS release:
### Cap'n Proto Support

Cap'n Proto benchmarks are **optional** and will be automatically skipped if the `capnpc-java` compiler is not present in the project root.

**To enable Cap'n Proto benchmarks:**

1. Install Captain Proto (`brew install capnp` on macOS)
2. Clone `capnpc-java` from https://github.com/capnproto/capnproto-java
3. Run `make` and place the `capnpc-java` executable in the project root directory
4. Run `capnp  compile -o ./capnpc-java:./build src/main/resources/capnp-schema/stock-schema.capnp --src-prefix=src/main/resources/capnp-schema` from the project root directory

**To disable Cap'n Proto benchmarks:**

Simply don't install `capnpc-java`. The build will skip Cap'n Proto code generation and the related benchmarks will be excluded automatically.

