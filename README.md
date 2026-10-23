# Spark 实验代码

作业2：TopN 词频、常见词语组合挖掘（Apriori 算法）、两张表连接。

## 运行流程

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 填入 VPC / 交换机 / 安全组
cp deploy-config.example.json deploy-config.json

# 创建 ECS 并初始化
python deploy.py

# 打包并提交实验
python run.py start

# 查看进度和结果
python run.py status
python run.py fetch
```

凭据默认读 `../.aliyun-accessKey`。

## 编译与单独执行

```bash
source /etc/profile.d/spark-lab.sh
bash build.sh
spark-submit --class lab.Main --master yarn target/homework2.jar topn <input> <output> <N>
spark-submit --class lab.Main --master yarn target/homework2.jar apriori <input> <output> <N> <minSupport>
spark-submit --class lab.Main --master yarn target/homework2.jar join <orders> <customers> <output> early|late
spark-submit --class lab.Main --master yarn target/homework2.jar suite <input-dir> <output-dir>
```

## 数据与校验

```bash
python3 tools/generate_join_data.py --customers 10000 --orders 100000 --seed 20261020 --output-dir data/generated
python3 tools/verify.py --data data --results results
```
