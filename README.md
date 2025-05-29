# Spark Scala 3 Big Data Project
A set of Spark applications written in Scala 3 for processing social network and warehouse data, designed to run on a local Spark cluster using Docker.

# 📦 Project Overview
This project demonstrates how to use Apache Spark with Scala 3 to process and analyze large-scale data for two use cases:

Warehouse Operations – Tracking inventory movements and computing statistics such as current stock, min/max/avg amounts per product and warehouse.

Social Network Analysis – Tracing retweet chains and computing the most influential users based on retweet waves.

Both applications run on a Dockerized local Spark cluster, allowing easy development, testing, and scalability simulation.


## Project Structure

```
.
├── build.sbt               # SBT build configuration
├── src/                    # Source code directory
├── data/                   # Data files and datasets
├── spark-apps/             # Contains compiled JARs for Spark applications
├── docker/                 # Docker configuration files
│   ├── Dockerfile          # Dockerfile for Spark nodes
│   └── start-spark.sh      # Script to start Spark within containers
├── docker-compose.yml      # Docker Compose configuration
└── Scripts:
    ├── start-spark-cluster.sh      # Start the Spark cluster with Docker Compose
    ├── stop-spark-cluster.sh       # Stop the Spark cluster
    ├── open-spark-UIs.sh           # Open Spark UI in browser
    ├── compile-run-on-cluster.sh   # Compile and run any Spark job on the cluster
    ├── run-on-cluster.sh           # Run a compiled Spark job on the cluster
    ├── compile-and-run-warehouse.sh # Compile and run the warehouse application
    └── run-warehouse-info.sh       # Run the compiled warehouse application
```

## Technical Stack

- **Scala**: Version 3.1.1
- **Apache Spark**: Version 3.2.0 (with compatibility for Scala 2.13)
- **Build Tool**: SBT (Scala Build Tool)
- **Container Technology**: Docker and Docker Compose
- **JVM**: Java 11

## Docker Setup

The project uses Docker Compose to set up a local Spark cluster consisting of:

- **Spark Master**: 
  - Exposes port 8080 for the Spark Master UI
  - Exposes port 4040 for the Spark Application UI
  - Mounts `./spark-apps` and `./data` as volumes

- **Spark Workers**: 
  - Each worker has 1G of memory and 1 core
  - Mounts `./data` as a volume
  - Dynamically assigned ports for Worker UIs

### Starting the Cluster

```bash
./start-spark-cluster.sh
```

This script:
1. Creates the `./spark-apps` directory (if it doesn't exist)
2. Starts a Spark master node and 2 worker nodes using Docker Compose

### Stopping the Cluster

```bash
./stop-spark-cluster.sh
```

This shuts down all Docker containers in the Spark cluster.

## Build and Run Instructions

### Compiling the Project

The project uses SBT for building. To compile and create an assembly JAR:

```bash
sbt clean assembly
```

This produces a JAR file at `target/scala-3.1.1/proyectsspark-0.1.0.jar`.

### Running on the Cluster

To compile and run the warehouse application:

```bash
./compile-and-run-warehouse.sh
```

To run the previously compiled warehouse application:

```bash
./run-warehouse-info.sh
```

To run any Spark application on the cluster:

```bash
./run-on-cluster.sh <fully-qualified-class-name>
```

Or to compile and run in one step:

```bash
./compile-run-on-cluster.sh <fully-qualified-class-name>
```

### Accessing Spark UIs

To open the Spark Master and Worker UIs in your browser:

```bash
./open-spark-UIs.sh
```

This will open:
- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040 (when a job is running)
- Worker UIs on dynamically assigned ports

## Development Setup

### Prerequisites

- Java 11 or higher
- SBT 1.5.0 or higher
- Docker and Docker Compose
- Optional: Scala plugin for your IDE

### IDE Setup

This project works well with IntelliJ IDEA with the Scala plugin installed:

1. Install IntelliJ IDEA
2. Install the Scala plugin
3. Open the project as an SBT project
4. Import the SBT build

### Local Development Workflow

1. Write your Scala code in the `src` directory
2. Build with `sbt clean assembly`
3. Start the Spark cluster with `./start-spark-cluster.sh`
4. Run your application with `./run-on-cluster.sh <your-main-class>`
5. View results in the Spark UI and console output
6. Shut down the cluster with `./stop-spark-cluster.sh` when done

## Notes on Scala 3 Compatibility

This project uses Scala 3.1.1, but Spark is still built for Scala 2.13. The build.sbt file is configured to handle this cross-version compatibility using:

```scala
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.2.0" % "provided").cross(CrossVersion.for3Use2_13)
)
```

This ensures that the Scala 3 code can work with the Scala 2.13 Spark libraries.

