#!/usr/bin/env bash
# Usage: sudo bash scripts/install_node.sh <node-number 1..4>
# Installs Java 11, Hadoop, Spark (+Iceberg jar) and writes all configs.
# Node 1 and Node 4 get the binaries as CLIENTS only (no daemons are started there).
set -euo pipefail
N="${1:?give node number 1-4}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"; source "$DIR/cluster.env"
MYIP_VAR="NODE${N}_IP"; MYIP="${!MYIP_VAR}"

apt-get update -y
apt-get install -y openjdk-11-jdk-headless python3 python3-pip python3-venv curl wget rsync net-tools ufw

# PySpark refuses to run if the driver (e.g. node1) and the executors (node2/3) use a
# different Python minor version. Pin EVERY node to python3.10, whatever the Ubuntu
# release (22.04 ships it; 20.04/24.04 get it from the deadsnakes PPA).
PY=python3.10
if ! command -v "$PY" >/dev/null || ! "$PY" -c 'import venv, ensurepip' 2>/dev/null; then
  apt-get install -y software-properties-common
  add-apt-repository -y ppa:deadsnakes/ppa
  apt-get update -y
  apt-get install -y python3.10 python3.10-venv
fi
"$PY" --version

cd /opt
if [ ! -d /opt/hadoop ]; then
  wget -q --show-progress "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
  tar xzf "hadoop-${HADOOP_VERSION}.tar.gz" && mv "hadoop-${HADOOP_VERSION}" hadoop && rm "hadoop-${HADOOP_VERSION}.tar.gz"
fi
if [ "$N" != "4" ] && [ ! -d /opt/spark ]; then
  wget -q --show-progress "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  tar xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && mv "spark-${SPARK_VERSION}-bin-hadoop3" spark && rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz"
fi
if [ "$N" != "4" ]; then
  JAR="iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
  [ -f "/opt/spark/jars/$JAR" ] || wget -q --show-progress -O "/opt/spark/jars/$JAR" \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/$JAR"
fi

JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
cat > /etc/profile.d/bigdata.sh <<EOP
export JAVA_HOME=$JAVA_HOME_PATH
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=$PY
export PYSPARK_DRIVER_PYTHON=$PY
export PATH=\$PATH:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin
EOP

# ---- Hadoop configs (identical on every node) ----
sed "s/__CLUSTER_USER__/$CLUSTER_USER/" "$DIR/conf/core-site.xml" > /opt/hadoop/etc/hadoop/core-site.xml
cp "$DIR/conf/hdfs-site.xml" "$DIR/conf/workers" /opt/hadoop/etc/hadoop/
grep -q '^export JAVA_HOME=' /opt/hadoop/etc/hadoop/hadoop-env.sh || \
  echo "export JAVA_HOME=$JAVA_HOME_PATH" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

# ---- Spark configs ----
if [ "$N" != "4" ]; then
  cp "$DIR/conf/spark-defaults.conf" /opt/spark/conf/spark-defaults.conf
  cat > /opt/spark/conf/spark-env.sh <<EOS
export JAVA_HOME=$JAVA_HOME_PATH
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PYSPARK_PYTHON=$PY
export SPARK_LOCAL_IP=$MYIP
export SPARK_LOCAL_HOSTNAME=node$N
export SPARK_MASTER_HOST=node3
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_DIR=/data/spark/work
EOS
  # every driver advertises its own hostname so executors can call back to it
  echo "spark.driver.host                           node$N" >> /opt/spark/conf/spark-defaults.conf
fi

mkdir -p /data/hdfs/namenode /data/hdfs/datanode /data/hadoop/tmp /data/spark/work /serving
# Owned by whoever ran "sudo bash ..." (works even if this machine's login isn't CLUSTER_USER;
# HDFS permissions are disabled, so usernames don't have to match across nodes).
OWNER="${SUDO_USER:-$CLUSTER_USER}"
chown -R "$OWNER:" /opt/hadoop /data /serving
[ -d /opt/spark ] && chown -R "$OWNER:" /opt/spark

# ---- Firewall: allow everything from the ZeroTier subnet, plus SSH ----
if grep -qi microsoft /proc/version; then
  # WSL: ufw usually can't run here, and ZeroTier traffic arrives directly inside
  # the WSL VM (it doesn't pass through a Linux firewall). Skip it.
  echo "WSL detected: skipping ufw firewall setup"
else
  ufw allow 22/tcp >/dev/null          # SSH by port number (works even if openssh-server is not installed)
  ufw allow from "$ZT_SUBNET" >/dev/null
  ufw --force enable >/dev/null
fi

# Search API venv: node4, or any node given WITH_SEARCH=1 (node1 = backup for node4).
if [ "$N" = "4" ] || [ "${WITH_SEARCH:-0}" = "1" ]; then
  "$PY" -m venv /opt/searchenv
  /opt/searchenv/bin/pip install -q -r "$DIR/search/requirements.txt"
fi
echo "Node $N installed. Log out and back in (or: source /etc/profile.d/bigdata.sh)."
