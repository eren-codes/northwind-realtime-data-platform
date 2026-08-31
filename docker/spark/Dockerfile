FROM apache/spark:4.0.4-scala2.13-java17-python3-r-ubuntu

USER root

ARG POSTGRES_JDBC_VERSION=42.7.13
ARG CLICKHOUSE_CONNECTOR_VERSION=0.10.0
ARG CLICKHOUSE_JDBC_VERSION=0.9.5

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL --retry 5 \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    && curl -fsSL --retry 5 \
    "https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-4.0_2.13/${CLICKHOUSE_CONNECTOR_VERSION}/clickhouse-spark-runtime-4.0_2.13-${CLICKHOUSE_CONNECTOR_VERSION}.jar" \
    -o "/opt/spark/jars/clickhouse-spark-runtime-4.0_2.13-${CLICKHOUSE_CONNECTOR_VERSION}.jar" \
    && curl -fsSL --retry 5 \
    "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
    -o "/opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"

USER 185

WORKDIR /opt/spark/work-dir

CMD ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m", "--conf", "spark.ui.enabled=false", "--conf", "spark.sql.shuffle.partitions=2", "--conf", "spark.default.parallelism=1", "--conf", "spark.sql.session.timeZone=UTC", "/opt/spark/work-dir/spark/connectivity_test.py"]
