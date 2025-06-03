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
    ├── Cluster Management:
    │   ├── start-spark-cluster.sh      # Start the Spark cluster with Docker Compose
    │   ├── stop-spark-cluster.sh       # Stop the Spark cluster
    │   └── open-spark-UIs.sh           # Open Spark UI in browser
    │
    ├── Warehouse Application:
    │   ├── compile-and-run-warehouse.sh # Compile and run the warehouse application
    │   ├── run-warehouse-info.sh       # Run the compiled warehouse application
    │   ├── compile-run-on-cluster.sh   # Compile and run any Spark job on the cluster
    │   └── run-on-cluster.sh           # Run a compiled Spark job on the cluster
    │
    └── Social Network Application:
        ├── compile-and-run-socialNetwork.sh # Compile and run the social network application
        ├── run-socialNetwork-info.sh   # Run the compiled social network application
        └── compile-run-on-cluster-sn.sh # Compile and run social network job on cluster
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

### Script Details

#### Cluster Management Scripts

- **start-spark-cluster.sh**: Initializes the Docker-based Spark cluster with a master node and 2 worker nodes. Creates necessary directories and sets up the Dockerized environment.

- **stop-spark-cluster.sh**: Shuts down all Docker containers in the Spark cluster and cleans up resources.

- **open-spark-UIs.sh**: Opens browser tabs for the Spark Master UI and Spark Application UI (if running) for monitoring and debugging.

#### Warehouse Application Scripts

- **compile-and-run-warehouse.sh**: Compiles the entire project and runs the warehouse analysis application on the Spark cluster in one step.

- **run-warehouse-info.sh**: Runs the pre-compiled warehouse application JAR on the Spark cluster without recompiling.

- **compile-run-on-cluster.sh**: Compiles the project and runs any specified class as a Spark job. Usage: `./compile-run-on-cluster.sh <fully-qualified-class-name>`.

- **run-on-cluster.sh**: Runs any specified class from the pre-compiled JAR as a Spark job. Usage: `./run-on-cluster.sh <fully-qualified-class-name>`.

#### Social Network Application Scripts

- **compile-and-run-socialNetwork.sh**: Compiles the entire project and runs the social network analysis application on the Spark cluster in one step.

- **run-socialNetwork-info.sh**: Runs the pre-compiled social network application JAR on the Spark cluster without recompiling.

- **compile-run-on-cluster-sn.sh**: Specialized script to compile and run the social network analysis on the Spark cluster with optimized settings.

### Running on the Cluster

To compile and run the warehouse application:

```bash
./compile-and-run-warehouse.sh
```

To run the previously compiled warehouse application:

```bash
./run-warehouse-info.sh
```

To compile and run the social network application:

```bash
./compile-and-run-socialNetwork.sh
```

To run the previously compiled social network application:

```bash
./run-socialNetwork-info.sh
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

## Data Format Requirements and Processing

### Warehouse Analysis
- Input files expected in `/opt/spark-data/input/warehouses/`:
  - **amounts.csv**: Contains columns `positionId`, `amount`, and `eventTime`
  - **positions.csv**: Contains columns `positionId`, `warehouse`, `product`, and `eventTime`
- Output is written to:
  - `/opt/spark-data/output/warehouses/CurrentAmounts`: Current amount per position
  - `/opt/spark-data/output/warehouses/WarehouseStats`: Statistics per warehouse and product

### Social Network Analysis
- Requires Avro files in the `/opt/spark-data/input/socialNetwork/` directory:
  - **MESSAGE.avro**: Contains user posts with `USER_ID` and `MESSAGE_ID`
  - **RETWEET.avro**: Contains retweet information with `USER_ID`, `SUBSCRIBER_ID`, and `MESSAGE_ID`
  - **MESSAGE_DIR.avro**: Contains message content with `MESSAGE_ID` and `TEXT`
  - **USER_DIR.avro**: Contains user information with `USER_ID`, `FIRST_NAME`, and `LAST_NAME`

### Data Processing Flow
1. Raw data is loaded from CSV/Avro files
2. Data is processed using Spark transformations with the defined schemas
3. For warehouse data: latest amounts are calculated and statistics are generated
4. For social network data: retweet waves are analyzed to identify influence patterns
5. Results are displayed and/or written to output locations

## Notes on Scala 3 Compatibility

This project uses Scala 3.1.1, but Spark is still built for Scala 2.13. The build.sbt file is configured to handle this cross-version compatibility using:

```scala
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.2.0" % "provided").cross(CrossVersion.for3Use2_13)
)
```

This ensures that the Scala 3 code can work with the Scala 2.13 Spark libraries.

