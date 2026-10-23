#!/usr/bin/env bash
# 此脚本在主节点由 hadoop 用户执行；命令追踪日志用于报告。
set -euxo pipefail
source /etc/profile.d/spark-lab.sh
cd /opt/lab
export LAB_ROOT="/user/hadoop/homework2-${1:-test}"
rm -rf results
mkdir -p results
{
    date -Is
    java -version
    hadoop version
    spark-submit --version
    hdfs dfsadmin -report
    yarn node -list
    free -m
    df -h /
} > results/environment.txt 2>&1
hdfs dfsadmin -safemode wait
hdfs dfs -rm -r -f "$LAB_ROOT"
hdfs dfs -mkdir -p /spark-events "$LAB_ROOT/input/generated"
hdfs dfs -chmod 1777 /spark-events
bash build.sh
python3 tools/generate_join_data.py --output-dir data/generated > results/generation.json
hdfs dfs -put -f data/alice.txt "$LAB_ROOT/input/"
hdfs dfs -put -f data/generated/orders.txt data/generated/customers.txt "$LAB_ROOT/input/generated/"
date -Is > results/started.txt
SECONDS=0
spark-submit --class lab.Main --master yarn --deploy-mode client \
    --num-executors 2 --executor-cores 1 --executor-memory 512m --driver-memory 512m \
    --conf spark.executor.memoryOverhead=384 \
    --conf spark.yarn.am.memory=256m --conf spark.yarn.am.memoryOverhead=128 \
    --conf spark.dynamicAllocation.enabled=false \
    target/homework2.jar suite "$LAB_ROOT/input" "$LAB_ROOT/output" \
    > results/spark-suite.log 2>&1
echo "$SECONDS" > results/application-wall-seconds.txt
date -Is > results/finished.txt
for name in topn-alice apriori-alice join-early join-late; do
    hdfs dfs -get "$LAB_ROOT/output/$name" "results/$name"
done
hdfs dfs -get /spark-events results/spark-events
python3 tools/verify.py --data data --results results > results/verification.json
grep '^LAB_' results/spark-suite.log > results/metrics.tsv
free -m > results/memory-after.txt
df -h / > results/disk-after.txt
echo '实验与独立校验完成'
