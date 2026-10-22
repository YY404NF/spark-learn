#!/usr/bin/env python3
"""只上传白名单文件，通过已有部署状态执行；支持断线后的状态查询/下载。

所有本地产物统一写入 runs/（已被 .gitignore 忽略），源码目录保持干净：
  runs/code.tar.gz          白名单源码包（每次 start 重建）
  runs/deployment-state.json 实例状态（deploy.py 写入）
  runs/*.log                初始化日志
  runs/results.tar.gz       fetch 下载的远端结果归档
  runs/download/            归档解压目录
"""
import argparse
import json
import subprocess
import tarfile
import time
from pathlib import Path
import cloud_common as cloud

BASE = Path(__file__).resolve().parent
RUNS = BASE / 'runs'
STATE = RUNS / 'deployment-state.json'
PACKAGE = RUNS / 'code.tar.gz'
RESULTS_ARCHIVE = RUNS / 'results.tar.gz'
DOWNLOAD = RUNS / 'download'
PACKAGE_ITEMS = ['src', 'data', 'tools', 'build.sh', 'run-remote.sh']
# 不入包：缓存、系统杂项、远端会重新生成的数据
EXCLUDED = {'__pycache__', '.DS_Store', 'generated'}


def package_filter(info: tarfile.TarInfo):
    """排除缓存与可再生成的目录，避免把本地产物发给远端。"""
    if any(part in EXCLUDED for part in Path(info.name).parts):
        return None
    if info.name.endswith('.pyc'):
        return None
    return info


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start', 'status', 'fetch', 'inspect'])
    args = parser.parse_args()
    if not STATE.is_file():
        raise SystemExit(f"缺少部署状态 {STATE}；先执行 python deploy.py")
    state = json.loads(STATE.read_text())
    node = state['nodes'][0]
    key, user = state['ssh_private_key_file'], state['ssh_user']
    host = node['public_ip']
    options = ['-i', key, '-o', cloud.known_hosts_option(key), '-o', 'StrictHostKeyChecking=accept-new']
    def remote(command, timeout=40):
        return subprocess.run(['ssh', *options, '-o', 'ServerAliveInterval=10', f'{user}@{host}', command],
            timeout=timeout, check=True)
    if args.action == 'start':
        RUNS.mkdir(parents=True, exist_ok=True)
        with tarfile.open(PACKAGE, 'w:gz') as tar:
            for name in PACKAGE_ITEMS:
                tar.add(BASE / name, arcname=name, filter=package_filter)
        subprocess.run(['scp', *options, str(PACKAGE), f'{user}@{host}:/tmp/homework2-code.tar.gz'], check=True)
        stamp = str(int(time.time()))
        remote("set -e; test -x /opt/spark/bin/spark-submit; test -d /opt/lab; "
            "test ! -f /opt/lab/results/started.txt; tar -xzf /tmp/homework2-code.tar.gz -C /opt/lab; "
            "chown -R hadoop:hadoop /opt/lab; "
            f"nohup runuser -u hadoop -- bash /opt/lab/run-remote.sh {stamp} "
            "> /opt/lab/run.log 2>&1 < /dev/null &", 30)
        print('已提交后台实验。使用 run.py status 查询，run.py fetch 下载。')
    elif args.action == 'status':
        remote("tail -n 12 /opt/lab/run.log; tail -n 12 /opt/lab/results/spark-suite.log 2>/dev/null; "
            "test ! -f /opt/lab/results/verification.json || cat /opt/lab/results/verification.json")
    elif args.action == 'inspect':
        for n in state['nodes']:
            r = cloud.ssh_command(n['public_ip'], user, key,
                'date -Is; free -m; df -h /; pgrep -af "NameNode|DataNode|ResourceManager|NodeManager|CoarseGrainedExecutorBackend"', 25)
            print(n['name'], r.stdout.decode(), r.stderr.decode())
    else:
        RUNS.mkdir(parents=True, exist_ok=True)
        remote('tar -czf /tmp/homework2-results.tar.gz -C /opt/lab results run.log', 60)
        subprocess.run(['scp', *options, f'{user}@{host}:/tmp/homework2-results.tar.gz',
            str(RESULTS_ARCHIVE)], check=True, timeout=180)
        DOWNLOAD.mkdir(parents=True, exist_ok=True)
        with tarfile.open(RESULTS_ARCHIVE) as tar:
            tar.extractall(DOWNLOAD, filter='data')
        print(DOWNLOAD)


if __name__ == '__main__':
    main()
