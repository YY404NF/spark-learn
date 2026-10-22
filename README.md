# Spark RDD 实验代码

作业2：TopN 词频、二元/三元词组、Apriori 频繁项集、两张表连接。只用 Spark RDD，不用 Spark SQL。

## 运行流程

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp deploy-config.example.json deploy-config.json    # 填入 VPC / 交换机 / 安全组
python deploy.py                                    # 创建 3 台 ECS 并初始化，约 30 分钟后自动释放
python run.py start                                 # 打包并提交实验
python run.py status                                # 查看进度
python run.py fetch                                 # 到期前下载结果
```

`deploy-config.json` 含云资源标识，不入库。凭据默认读 `../.aliyun-accessKey`，也可用环境变量 `ALIYUN_CREDENTIAL_FILE` 指定；只在本机读取，不上传远端。

## 编译与单独执行

```bash
source /etc/profile.d/spark-lab.sh
bash build.sh          # 产出 target/homework2.jar
spark-submit --class lab.Main --master yarn target/homework2.jar topn <input> <output> <N>
spark-submit --class lab.Main --master yarn target/homework2.jar phrases <input> <output> <N> <minCount>
spark-submit --class lab.Main --master yarn target/homework2.jar apriori <input> <output> <N> <minSupport>
spark-submit --class lab.Main --master yarn target/homework2.jar join <orders> <customers> <output> early|late
spark-submit --class lab.Main --master yarn target/homework2.jar suite <input-dir> <output-dir>
```

输出目录必须不存在。`suite` 在一个应用里依次跑完九个案例。英文按 ASCII 字母切词并转小写，同频按字典序；Apriori 以句子为事务、项集最长 3，超过 2 万个频繁项或多词事务会明确报错。

## 数据与校验

```bash
python3 tools/generate_join_data.py --customers 10000 --orders 100000 --seed 20261020 --output-dir data/generated
python3 tools/verify.py --data data --results results
```

合成数据用固定种子，可精确重建。`data/alice-source.txt` 取自 [Project Gutenberg](https://www.gutenberg.org/cache/epub/11/pg11.txt)，`data/alice.txt` 由 `tools/prepare_text.py` 合并段内换行生成。

`target/`、`runs/`、`results/`、`data/generated/`、`.venv/` 都是可重建的产物，已被 `.gitignore` 忽略；`results/` 与 `runs/download/results/` 是上一轮验证快照，`tools/verify.py` 对其可重做 21 项校验。
