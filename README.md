# Spark RDD 实验代码

## 本地控制入口

在本目录执行：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python deploy.py
python run.py start
python run.py status
python run.py fetch
```

`deploy-config.json` 默认创建 3 台 2核2GB、20GB 系统盘 ECS，API 请求中设置约 30 分钟后自动释放（向上取整到整分钟）。该文件记录本机云资源标识，已被 `.gitignore` 忽略；首次使用请复制 `deploy-config.example.json` 再填写 VPC、交换机和安全组。凭据默认读 `../.aliyun-accessKey`，可用环境变量 `ALIYUN_CREDENTIAL_FILE` 或配置项 `credential_file` 指向其他位置，只在本地读取，不上传远端，也不进入日志与打包产物。`python deploy.py --resume` 只恢复已有节点初始化；不会延长有效期。

主节点运行 NameNode、ResourceManager、DataNode 和 client Driver；两个工作节点运行 DataNode、NodeManager 和 Executor。不启用 Standalone Master/Worker。节点使用私网通信。

`run.py` 只打包白名单源码、数据和脚本，在主节点后台执行 `run-remote.sh`；状态/日志与结果通过独立命令读取，断线无需重复提交。请先确认部署完成，再提交实验。打包时排除 `__pycache__`、`.DS_Store` 和 `data/generated/`（远端用固定种子重新生成），避免把本地产物发给远端。

## 目录与产物

仓库只提交源码、脚本和可复现的输入数据；所有产物都落在被忽略的目录里，可随时删除后重建。

| 路径 | 内容 | 是否入库 |
| --- | --- | --- |
| `src/main/java/lab/Main.java` | Spark RDD 程序 | 是 |
| `tools/*.py` | 数据生成、文本预处理、结果校验、事件日志汇总 | 是 |
| `data/*.txt` | 手工小样本与预处理后的语料 | 是 |
| `data/generated/` | 固定种子生成的 10 万订单两张表与预期结果 | 否，由 `tools/generate_join_data.py` 重建 |
| `target/` | `build.sh` 产出的 class 与 JAR | 否 |
| `runs/` | 实例状态、运行时密钥、初始化日志、`code.tar.gz`、`fetch` 下载的结果 | 否 |
| `results/` | 本地保留的验证快照（与远端 `results/` 同名） | 否 |
| `.venv/` | 本地虚拟环境 | 否 |

凭据 `../.aliyun-accessKey`、`deploy-config.json`、`spark-lab-id_rsa*`、`*.known_hosts` 一律不入库。`.gitignore` 与 `.gitattributes` 本身入库，保证换一台机器克隆后忽略规则和换行符仍然一致。

## 编译与独立执行

安装 Spark 3.5.8、Hadoop 3.5.0 和 Java 17 后：

```bash
source /etc/profile.d/spark-lab.sh
bash build.sh
# 或在有 Maven 的环境执行 mvn clean package
```

`build.sh` 直接复用 Spark 安装包的 JAR 编译，避免短时小磁盘节点重复下载 Maven 依赖。

```bash
spark-submit --class lab.Main --master yarn target/homework2.jar topn <input> <output> <N>
spark-submit --class lab.Main --master yarn target/homework2.jar phrases <input> <output> <N> <minCount>
spark-submit --class lab.Main --master yarn target/homework2.jar apriori <input> <output> <N> <minSupport>
spark-submit --class lab.Main --master yarn target/homework2.jar join <orders> <customers> <output> early
spark-submit --class lab.Main --master yarn target/homework2.jar join <orders> <customers> <output> late
```

上述尖括号参数需要替换为实际值。输出目录必须不存在。Java 程序只使用 RDD；英文按 ASCII 字母切词并转小写。TopN 同频按字典序排序。Apriori 以句子为事务，句子不跨行，事务内去重，最大项集长度3，保留完整频繁结果与TopN；广播集合上限2万，单事务不同词数上限256，超过时明确失败。

客户输入要求编号唯一；空行跳过，坏字段报错。正常合成数据由标准库生成，不依赖 Faker；固定随机种子，记录外键和准确预期结果。

```bash
python3 tools/generate_join_data.py --customers 10000 --orders 100000 --seed 20261020 --output-dir data/generated
python3 tools/verify.py --data data --results results
```

`suite <input-directory> <output-directory>` 会在同一个 Spark 应用中依次执行九个案例，避免多次启动应用。案例时间是动作链耗时，不能等同于独立应用冷启动时间，也不能将顺序执行的 early/late 比较直接解释为可靠性能结论。

## 数据来源

可用 `python3 tools/prepare_text.py data/alice-source.txt data/alice.txt` 重建相同的预处理文本。

`data/alice-source.txt` 来自 Project Gutenberg 的 *Alice's Adventures in Wonderland*：<https://www.gutenberg.org/cache/epub/11/pg11.txt>，保留来源文件及其声明。`alice.txt` 将段内换行合并为一行；本次保留文件头尾，报告按同一数据说明。其他小样本由本项目手工编写。

## 结果与资源

`runs/deployment-state.json` 保存本轮实例、SSH 密钥路径和释放时间；`runs/download/results` 保存环境、指标、HDFS 输出、事件日志及标准库独立校验结果。云端自动释放不替代本地下载；应在到期前完成 `run.py fetch`。

每节点实际可见内存低于标称2GB，守护进程堆128MB，Executor堆512MB与额外开销384MB，AM 256MB与128MB额外开销，Driver堆512MB。YARN配置1536MB是调度额度，必须同时观察实际物理内存；这套配置针对小样本顺序测试，不能保证大数据或更多并发应用不发生内存不足。

本轮验证快照位于 `results/`，完整原始结果位于 `runs/download/results/`（两者都不入库，可用 `python run.py fetch` 重新取回）。执行 `python3 tools/verify.py --data data --results runs/download/results` 可重做21项校验；`tools/summarize_events.py` 可重新汇总原始事件日志。
