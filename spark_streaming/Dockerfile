
FROM docker.io/bitnami/spark:3.2.3 


WORKDIR /app


COPY --chown=1001:1001 ./consumer.py /app/consumer.py


CMD [ \
    "spark-submit", \
    "--master", "spark://spark:7077", \
    "--deploy-mode", "client", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3", \
    "/app/consumer.py" \
]