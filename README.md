# Prodware: Real-time Data Analytics Pipeline

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Druid](https://img.shields.io/badge/Apache%20Druid-2E353B?style=for-the-badge&logo=apachedruid&logoColor=white)](https://druid.apache.org/)
[![Apache Superset](https://img.shields.io/badge/Apache%20Superset-00A699?style=for-the-badge&logo=apachesuperset&logoColor=white)](https://superset.apache.org/)

This project demonstrates a complete real-time data analytics pipeline. It captures data changes from a source database (simulating Microsoft Business Central using SQL Server), processes them in-flight with Spark, and visualizes the results on a live dashboard with Superset. The entire architecture is containerized using Docker for easy setup and deployment.

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Architecture

The data flows through the system as follows:

![Architecture Diagram](https://raw.githubusercontent.com/SAAD2003D/Prodware-Real-time-Data-Analytics-of-Business-Central-Data/main/prodware-diagram-no-hadoop.png)
*(This diagram is based on the provided screenshot, excluding Hadoop)*

1.  **Data Source (Microsoft SQL Server):** Acts as the primary database, simulating a transactional system like Microsoft Dynamics 365 Business Central.
2.  **Change Data Capture (Kafka Connect):** A Debezium SQL Server connector is deployed via Kafka Connect. It monitors the transaction log of the SQL Server database for any inserts, updates, or deletes and publishes these changes as events to a Kafka topic in real-time.
3.  **Stream Processing (Apache Spark):** An Apache Spark Streaming job subscribes to the Kafka topic. It consumes the change events, performs necessary transformations and aggregations (e.g., cleaning data, calculating metrics), and prepares the data for analysis.
4.  **Real-time Datastore (Apache Druid):** The processed data from Spark is ingested into Apache Druid, a high-performance, real-time analytics database designed for fast ad-hoc queries on large datasets.
5.  **Visualization (Apache Superset):** Apache Superset connects to Druid as a data source. It is used to build interactive dashboards and charts that visualize the real-time data, providing live insights into the business operations.

## Tech Stack

-   **Containerization:** Docker & Docker Compose
-   **Data Source:** Microsoft SQL Server
-   **Change Data Capture:** Apache Kafka, Kafka Connect, Debezium
-   **Stream Processing:** Apache Spark Streaming
-   **Real-time OLAP Datastore:** Apache Druid
-   **Data Visualization:** Apache Superset

## Features

-   **End-to-End Pipeline:** A complete, working example of a modern real-time data stack.
-   **Change Data Capture:** Efficiently captures database changes without impacting the source system's performance.
-   **Scalable Processing:** Leverages Apache Spark for distributed and scalable data processing.
-   **Low-Latency Analytics:** Uses Apache Druid to enable interactive queries on real-time data.
-   **Interactive Dashboards:** Visualizes key metrics and trends using Apache Superset.
-   **Containerized & Reproducible:** The entire stack is defined in `docker-compose.yml` for a one-command setup.

## Prerequisites

Before you begin, ensure you have the following installed on your machine:
-   [**Docker**](https://www.docker.com/get-started)
-   [**Docker Compose**](https://docs.docker.com/compose/install/)
-   [**Git**](https://git-scm.com/)
-   **System Resources:** This pipeline runs multiple services and is resource-intensive. A minimum of **16GB of RAM** is recommended for a smooth experience.

## Getting Started

Follow these steps to get the project up and running:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/SAAD2003D/Prodware-Real-time-Data-Analytics-of-Business-Central-Data.git
    cd Prodware-Real-time-Data-Analytics-of-Business-Central-Data
    ```

2.  **Start the services:**
    Use Docker Compose to build and start all the containers in the background.
    ```bash
    docker-compose up -d
    ```
    The initial startup may take several minutes as Docker needs to download the images for all the services.

3.  **Check the status of the containers:**
    ```bash
    docker-compose ps
    ```
    Ensure all services are in the `Up` or `Healthy` state.

## Usage

Once all services are running, you can interact with the pipeline.

### 1. Access Apache Superset

-   Open your web browser and navigate to `http://localhost:8088`.
-   Log in with the default credentials:
    -   **Username:** `admin`
    -   **Password:** `admin`

### 2. Connect Superset to Druid

-   In the Superset UI, go to **Data** -> **Databases** and click the `+ DATABASE` button.
-   Select **Apache Druid**.
-   In the **SQLAlchemy URI** field, enter the following connection string:
    ```
    druid://druid-broker:8888/druid/v2/sql/
    ```
    *(Note: We use the service name `druid-broker` because Superset is running in the same Docker network)*.
-   Test the connection and save the database. You can now create charts and dashboards using the data ingested into Druid.

### 3. Generate Sample Data

To see the pipeline in action, you need to insert or update data in the source SQL Server database.

1.  Connect to the SQL Server instance using your favorite SQL client (e.g., DBeaver, Azure Data Studio). Find the connection details (port, password) in the `docker-compose.yml` file under the `sqlserver` service.
2.  Run SQL `INSERT` or `UPDATE` commands on the tables being monitored by Debezium.
3.  Observe the data flowing through the pipeline and appearing on your Superset dashboard within seconds.

## Project Structure
