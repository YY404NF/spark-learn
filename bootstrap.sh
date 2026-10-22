#!/usr/bin/env bash
set -euo pipefail
ROLE="$1"; MASTER_IP="$2"; NODE_COUNT="$3"
export DEBIAN_FRONTEND=noninteractive
ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
apt-get -o DPkg::Lock::Timeout=180 update -qq
apt-get -o DPkg::Lock::Timeout=180 install -y -qq openjdk-17-jdk-headless curl python3
id hadoop >/dev/null 2>&1 || useradd -m -s /bin/bash hadoop
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
export HADOOP_HOME=/opt/hadoop SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"
mkdir -p /opt /var/cache/spark-lab
fetch_extract() {
    local url="$1" dest="$2" archive="$3"
    if [ ! -x "$dest/bin/$(test "$dest" = /opt/spark && echo spark-submit || echo hadoop)" ]; then
        curl --fail --location --retry 3 --connect-timeout 15 "$url" -o "$archive"
        mkdir -p "$dest"
        tar -xzf "$archive" -C "$dest" --strip-components=1
        rm -f "$archive"
    fi
}
fetch_extract https://mirrors.aliyun.com/apache/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz /opt/hadoop /var/cache/spark-lab/hadoop.tgz &
HADOOP_PID=$!
fetch_extract https://mirrors.aliyun.com/apache/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz /opt/spark /var/cache/spark-lab/spark.tgz &
SPARK_PID=$!
wait "$HADOOP_PID"
wait "$SPARK_PID"
cat > /etc/profile.d/spark-lab.sh <<EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH=\$JAVA_HOME/bin:/opt/hadoop/bin:/opt/spark/bin:\$PATH
EOF
cat > "$HADOOP_CONF_DIR/hadoop-env.sh" <<EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_HEAPSIZE_MAX=128
export HADOOP_LOG_DIR=/var/log/hadoop
EOF
cat > "$HADOOP_CONF_DIR/yarn-env.sh" <<EOF
export JAVA_HOME=$JAVA_HOME
export YARN_RESOURCEMANAGER_HEAPSIZE=128
export YARN_NODEMANAGER_HEAPSIZE=128
export YARN_LOG_DIR=/var/log/hadoop
EOF
mkdir -p /var/log/hadoop /var/lib/hadoop/{nn,dn,tmp,yarn/local,yarn/logs} /opt/lab /var/log/spark-events
cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<configuration>
<property><name>fs.defaultFS</name><value>hdfs://$MASTER_IP:9000</value></property>
<property><name>hadoop.tmp.dir</name><value>/var/lib/hadoop/tmp</value></property>
</configuration>
EOF
REPLICAS=2
if [ "$NODE_COUNT" -lt 2 ]; then REPLICAS=1; fi
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
<property><name>dfs.replication</name><value>$REPLICAS</value></property>
<property><name>dfs.namenode.name.dir</name><value>file:///var/lib/hadoop/nn</value></property>
<property><name>dfs.datanode.data.dir</name><value>file:///var/lib/hadoop/dn</value></property>
<property><name>dfs.permissions.enabled</name><value>false</value></property>
<property><name>dfs.namenode.safemode.min.datanodes</name><value>$NODE_COUNT</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<EOF
<configuration>
<property><name>yarn.resourcemanager.hostname</name><value>$MASTER_IP</value></property>
<property><name>yarn.nodemanager.resource.memory-mb</name><value>1536</value></property>
<property><name>yarn.nodemanager.resource.cpu-vcores</name><value>2</value></property>
<property><name>yarn.scheduler.minimum-allocation-mb</name><value>128</value></property>
<property><name>yarn.scheduler.maximum-allocation-mb</name><value>1536</value></property>
<property><name>yarn.nodemanager.local-dirs</name><value>/var/lib/hadoop/yarn/local</value></property>
<property><name>yarn.nodemanager.log-dirs</name><value>/var/lib/hadoop/yarn/logs</value></property>
<property><name>yarn.log-aggregation-enable</name><value>true</value></property>
<property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
<property><name>yarn.nodemanager.delete.debug-delay-sec</name><value>0</value></property>
<property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>95</value></property>
</configuration>
EOF
cat > /opt/spark/conf/spark-defaults.conf <<EOF
spark.master yarn
spark.submit.deployMode client
spark.driver.host $MASTER_IP
spark.driver.bindAddress 0.0.0.0
spark.driver.memory 512m
spark.executor.instances 2
spark.executor.cores 1
spark.executor.memory 512m
spark.executor.memoryOverhead 384
spark.yarn.am.memory 256m
spark.yarn.am.memoryOverhead 128
spark.dynamicAllocation.enabled false
spark.default.parallelism 4
spark.eventLog.enabled true
spark.eventLog.dir hdfs://$MASTER_IP:9000/spark-events
spark.ui.enabled true
EOF
chown -R hadoop:hadoop /var/lib/hadoop /var/log/hadoop /var/log/spark-events /opt/lab /opt/hadoop/logs 2>/dev/null || chown -R hadoop:hadoop /var/lib/hadoop /var/log/hadoop /var/log/spark-events /opt/lab
start_daemon() {
    local service="$1" class="$2" command="$3"
    if ! pgrep -u hadoop -f "$class" >/dev/null; then
        runuser -u hadoop -- "$command" --daemon start "$service"
    fi
}
if [ "$ROLE" = master ]; then
    if [ ! -f /var/lib/hadoop/nn/current/VERSION ]; then
        runuser -u hadoop -- /opt/hadoop/bin/hdfs namenode -format -nonInteractive
    fi
    start_daemon namenode org.apache.hadoop.hdfs.server.namenode.NameNode /opt/hadoop/bin/hdfs
    start_daemon resourcemanager org.apache.hadoop.yarn.server.resourcemanager.ResourceManager /opt/hadoop/bin/yarn
fi
start_daemon datanode org.apache.hadoop.hdfs.server.datanode.DataNode /opt/hadoop/bin/hdfs
if [ "$ROLE" = worker ]; then
    start_daemon nodemanager org.apache.hadoop.yarn.server.nodemanager.NodeManager /opt/hadoop/bin/yarn
fi
java -version
free -m
df -h /
jps
