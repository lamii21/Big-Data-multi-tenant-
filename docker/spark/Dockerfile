# syntax=docker/dockerfile:1
# =============================================================================
# Image Spark unique du projet (master, workers ET driver Jupyter).
# Pourquoi une seule image ? Le driver (Jupyter) et les executors (workers)
# doivent partager exactement la même version de Spark, de Scala, de Java et
# la même version mineure de Python, sinon PySpark lève PYTHON_VERSION_MISMATCH.
# Les JARs sont intégrés à l'image (pas de --packages au runtime) :
# démarrage plus rapide, reproductible et fonctionnel hors-ligne (soutenance).
# =============================================================================
ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

# -----------------------------------------------------------------------------
# Stage "spark" : utilisé par spark-master, spark-worker-*, spark-history
# -----------------------------------------------------------------------------
FROM ${SPARK_IMAGE} AS spark

ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY=2.12
# Connecteur Kafka : kafka-clients 3.4.1 / commons-pool2 2.11.1 = versions du pom Spark 3.5.9
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
# Delta Lake 3.3.x est construit pour Spark 3.5.3+ (Scala 2.12)
ARG DELTA_VERSION=3.3.2
# S3A : hadoop-aws doit égaler la version Hadoop embarquée dans Spark 3.5 (3.3.4)
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
# GraphFrames pour Spark 3.5 / Scala 2.12 (dépôt spark-packages)
ARG GRAPHFRAMES_VERSION=0.8.3-spark3.5-s_2.12
ARG JAVA_SEMVER_VERSION=0.9.0

USER root

# 1) JARs additionnels déposés dans $SPARK_HOME/jars => classpath du driver et des executors
RUN set -eux; \
    MVN=https://repo1.maven.org/maven2; \
    SPK=https://repos.spark-packages.org; \
    cd /opt/spark/jars; \
    for url in \
      "$MVN/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "$MVN/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "$MVN/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "$MVN/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "$MVN/io/delta/delta-spark_${SCALA_BINARY}/${DELTA_VERSION}/delta-spark_${SCALA_BINARY}-${DELTA_VERSION}.jar" \
      "$MVN/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "$MVN/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "$MVN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      "$MVN/com/github/zafarkhaja/java-semver/${JAVA_SEMVER_VERSION}/java-semver-${JAVA_SEMVER_VERSION}.jar" \
      "$SPK/graphframes/graphframes/${GRAPHFRAMES_VERSION}/graphframes-${GRAPHFRAMES_VERSION}.jar" \
    ; do wget -nv --tries=3 "$url"; done; \
    chown spark:spark /opt/spark/jars/*.jar

# 2) Dépendances Python communes au driver ET aux executors (pandas UDF, Arrow, Delta)
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r /tmp/requirements-spark.txt; \
    # delta-spark sans dépendances : pyspark est déjà fourni par $SPARK_HOME/python
    python3 -m pip install --no-cache-dir --no-deps delta-spark==${DELTA_VERSION}; \
    SITE=$(python3 -c "import sysconfig; print(sysconfig.get_paths()['purelib'])"); \
    # PySpark + py4j de l'image visibles par tout interpréteur python3 (Jupyter compris)
    PY4J=$(ls /opt/spark/python/lib/py4j-*-src.zip); \
    printf '/opt/spark/python\n%s\n' "$PY4J" > "$SITE/pyspark-home.pth"; \
    # Le module Python de GraphFrames est livré dans le JAR : on l'extrait
    python3 -c "import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); [z.extract(n, sys.argv[2]) for n in z.namelist() if n.startswith('graphframes/') and n.endswith('.py')]" \
      /opt/spark/jars/graphframes-${GRAPHFRAMES_VERSION}.jar "$SITE"; \
    python3 -c "import pyspark, delta, graphframes; print('pyspark', pyspark.__version__)"

# 3) Répertoires de travail accessibles à l'utilisateur spark (uid 185)
RUN set -eux; \
    mkdir -p /opt/spark/spark-events /workspace /home/spark; \
    chown -R spark:spark /opt/spark/spark-events /workspace /home/spark

ENV HOME=/home/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/workspace/src \
    SPARK_NO_DAEMONIZE=true

USER spark
WORKDIR /workspace

# -----------------------------------------------------------------------------
# Stage "jupyter" : driver interactif (mode client) = même base que les workers
# -----------------------------------------------------------------------------
FROM spark AS jupyter

USER root
COPY requirements-jupyter.txt /tmp/requirements-jupyter.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements-jupyter.txt
USER spark

EXPOSE 8888 4040
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.root_dir=/workspace"]
