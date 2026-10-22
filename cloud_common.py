#!/usr/bin/env python3
"""创建短时 ECS 集群并配置节点间 SSH 互信"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo



BASE_DIR = Path(__file__).resolve().parent
BEIJING = ZoneInfo("Asia/Shanghai")
CREDENTIAL_ENV = "ALIYUN_CREDENTIAL_FILE"
CREDENTIAL_DEFAULT = "../.aliyun-accessKey"


def log(message: str) -> None:
    print(f"[{datetime.now(BEIJING):%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def progress(message: str) -> None:
    sys.stdout.write(f"\r\033[2K[{datetime.now(BEIJING):%Y-%m-%d %H:%M:%S}] {message}")
    sys.stdout.flush()


def finish_progress() -> None:
    print(flush=True)


def known_hosts_option(private_key: str) -> str:
    path = f"{private_key}.known_hosts".replace("\\", "\\\\").replace('"', '\\"')
    return f'UserKnownHostsFile="{path}"'


def credential_path(config: dict[str, Any] | None = None) -> Path:
    """按 环境变量 > 配置项 > 上级目录默认文件 的顺序定位凭据；找不到时明确报错。

    凭据只在本地读取，不进入打包产物，也不写入任何运行日志。
    """
    configured = os.environ.get(CREDENTIAL_ENV) or (config or {}).get("credential_file")
    path = Path(configured) if configured else Path(CREDENTIAL_DEFAULT)
    path = path if path.is_absolute() else (BASE_DIR / path).resolve()
    if not path.is_file():
        raise SystemExit(f"未找到凭据文件 {path}；请设置 {CREDENTIAL_ENV} "
                         f"或在配置文件的 credential_file 中指定")
    return path


def make_client(region: str, credential_file: Path | None = None):
    """读取阿里云密钥并创建指定地域的 ECS 客户端"""
    from alibabacloud_ecs20140526.client import Client
    from alibabacloud_tea_openapi import models

    credentials = json.loads((credential_file or credential_path()).read_text(encoding="utf-8"))
    config = models.Config(
        access_key_id=credentials["AccessKeyId"],
        access_key_secret=credentials["AccessKeySecret"],
        region_id=region,
    )
    config.endpoint = f"ecs.{region}.aliyuncs.com"
    return Client(config)


def make_keypair(config: dict[str, Any]) -> tuple[str, str, str, str]:
    """生成带时间戳的 SSH 密钥对并返回密钥信息"""
    stamp = int(time.time() * 1000)
    private_path = BASE_DIR / f"{config['private_key_file']}-{stamp}"
    public_path = Path(f"{private_path}.pub")
    subprocess.run([
        "ssh-keygen", "-q", "-t", "rsa", "-b", "3072", "-N", "", "-C",
        "temporary-hadoop-lab", "-f", str(private_path),
    ], check=True)
    private_path.chmod(0o600)
    public_path.chmod(0o644)
    key_name = f"{config['key_pair_name_prefix']}-{stamp}"
    return key_name, str(private_path), public_path.read_text(encoding="utf-8").strip(), str(public_path)


def call_model(client: Any, model_module: Any, method_name: str, request_name: str,
               request_data: dict[str, Any]):
    """按名称创建 SDK 请求并调用对应的 ECS API"""
    request_type = getattr(model_module, request_name)
    return getattr(client, method_name)(request_type(**request_data))


def auto_release_at(now: datetime, minutes: int) -> datetime:
    """计算自动释放时间，并向上取整到整分钟"""
    target = now + timedelta(minutes=minutes)
    if target.second or target.microsecond:
        target = target.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return target


def wait_instances(client: Any, models: Any, region: str, ids: list[str], timeout: int):
    """轮询实例状态，直到全部运行或出现失败、超时"""
    deadline = time.monotonic() + timeout
    ready_ids = set()
    progress(f"实例就绪进度：0/{len(ids)}")
    while time.monotonic() < deadline:
        response = call_model(client, models, "describe_instances", "DescribeInstancesRequest", {
            "region_id": region,
            "instance_ids": json.dumps(ids),
            "page_size": 100,
        })
        instances = [item.to_map() for item in response.body.instances.instance]
        states = {item.get("InstanceId"): item.get("Status") for item in instances}
        for instance_id in ids:
            if states.get(instance_id) == "Running" and instance_id not in ready_ids:
                ready_ids.add(instance_id)
                progress(f"实例就绪进度：{len(ready_ids)}/{len(ids)}")
        if len(instances) == len(ids) and all(s == "Running" for s in states.values()):
            finish_progress()
            return instances
        if any(s in {"CreateFailed", "Deleted", "Deleting"} for s in states.values()):
            finish_progress()
            raise RuntimeError(f"实例创建失败或已删除：{states}")
        time.sleep(8)
    finish_progress()
    raise TimeoutError("等待实例运行超时；实例仍会按创建请求中的自动释放时间到期释放")


def ssh_command(host: str, user: str, private_key: str, command: str,
                timeout: int = 20) -> subprocess.CompletedProcess[str]:
    """使用指定私钥执行一次 SSH 命令"""
    return subprocess.run([
        "ssh", "-i", private_key,
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=7",
        "-o", known_hosts_option(private_key),
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ServerAliveInterval=8",
        f"{user}@{host}", command,
    ], capture_output=True, timeout=timeout, check=False)


def ssh_endpoint(node: dict[str, Any]) -> str:
    """优先返回公网地址，没有公网地址时使用内网地址"""
    return node.get("public_ip") or node["private_ip"]


def wait_ssh(nodes: list[dict[str, Any]], private_key: str, user: str, timeout: int) -> None:
    """并行等待所有实例的 SSH 服务就绪"""
    pending = {node["instance_id"]: node for node in nodes}
    deadline = time.monotonic() + timeout
    ready = 0
    progress(f"SSH 就绪进度：0/{len(nodes)}")
    while pending and time.monotonic() < deadline:
        with ThreadPoolExecutor(max_workers=32) as pool:
            jobs = {pool.submit(ssh_command, ssh_endpoint(n), user, private_key, "true", 12): iid
                    for iid, n in pending.items()}
            for future in as_completed(jobs):
                iid = jobs[future]
                if future.result().returncode == 0:
                    del pending[iid]
                    ready += 1
                    progress(f"SSH 就绪进度：{ready}/{len(nodes)}")
        if pending:
            time.sleep(6)
    if pending:
        finish_progress()
        raise TimeoutError(f"以下实例 SSH 不可达：{list(pending)}")
    finish_progress()


def configure_hosts(nodes: list[dict[str, Any]], private_key: str, user: str) -> None:
    """配置节点内网名称解析，并把私钥分发到各节点以建立互信"""
    entries = "\n".join(f"{n['private_ip']} {n['name']}" for n in nodes)
    command = "printf '%s\\n' " + shlex.quote(entries + " # hadoop-lab-managed") + " >> /etc/hosts"
    with ThreadPoolExecutor(max_workers=100) as pool:
        list(pool.map(lambda n: subprocess.run([
            "ssh", "-i", private_key, "-o", "BatchMode=yes",
            "-o", known_hosts_option(private_key),
            "-o", "StrictHostKeyChecking=accept-new",
            f"{user}@{ssh_endpoint(n)}", command,
        ], check=True, timeout=25), nodes))

    key_data = Path(private_key).read_text(encoding="utf-8")
    key_command = "install -d -m 700 ~/.ssh && cat > ~/.ssh/id_rsa && chmod 600 ~/.ssh/id_rsa"
    with ThreadPoolExecutor(max_workers=100) as pool:
        list(pool.map(lambda node: subprocess.run([
            "ssh", "-i", private_key, "-o", "BatchMode=yes",
            "-o", known_hosts_option(private_key),
            "-o", "StrictHostKeyChecking=accept-new",
            f"{user}@{ssh_endpoint(node)}", key_command,
        ], input=key_data, text=True, check=True, timeout=25), nodes))


def write_state(path: Path, state: dict[str, Any]) -> None:
    """以受限权限原子写入部署状态文件"""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


