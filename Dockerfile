FROM apache/spark:4.1.3-scala2.13-java17-python3-r-ubuntu

WORKDIR /app

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
