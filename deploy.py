#!/usr/bin/env python3
"""复用作业1 API/SSH 工具。先在服务端设置释放时间并保存状态，再配置集群。"""
import argparse
import json
import shlex
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import cloud_common as cloud

BASE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default=str(BASE / 'deploy-config.json'))
    parser.add_argument('--resume', action='store_true', help='仅恢复已有集群的初始化，不创建实例')
    args = parser.parse_args()
    config_path = Path(args.config)
    if not config_path.is_file():
        raise SystemExit(f"缺少配置文件 {config_path}；"
                         "复制 deploy-config.example.json 并填入自己的云资源信息")
    config = json.loads(config_path.read_text())
    missing = [key for key in ('region_id', 'image_id', 'instance_type', 'instance_count',
        'v_switch_id', 'security_group_id', 'ssh_user', 'system_disk_size_gb',
        'lifetime_minutes', 'output_file', 'private_key_file', 'key_pair_name_prefix')
        if key not in config]
    if missing:
        raise SystemExit(f"配置缺少字段：{', '.join(missing)}")
    state_path = BASE / config['output_file']
    state_path.parent.mkdir(parents=True, exist_ok=True)
    if args.resume:
        state = json.loads(state_path.read_text())
    else:
        if state_path.exists():
            old = json.loads(state_path.read_text())
            if datetime.fromisoformat(old['expires_at'].replace('Z', '+00:00')) > datetime.now(timezone.utc):
                raise SystemExit('已有未到期部署状态。使用 --resume，避免重复创建实例。')
        from alibabacloud_ecs20140526 import models
        client = cloud.make_client(config['region_id'], cloud.credential_path(config))
        key_name, private, public, public_path = cloud.make_keypair(config)
        cloud.call_model(client, models, 'import_key_pair', 'ImportKeyPairRequest', {
            'region_id': config['region_id'], 'key_pair_name': key_name, 'public_key_body': public})
        created = datetime.now(timezone.utc)
        # ECS 按分钟处理，向上取整，保证不少于指定分钟。
        release = cloud.auto_release_at(created, config['lifetime_minutes'])
        request = dict(region_id=config['region_id'], image_id=config['image_id'],
            instance_type=config['instance_type'], v_switch_id=config['v_switch_id'],
            security_group_id=config['security_group_id'], internet_max_bandwidth_out=1,
            key_pair_name=key_name, amount=config['instance_count'], min_amount=config['instance_count'],
            auto_release_time=release.strftime('%Y-%m-%dT%H:%M:%SZ'),
            system_disk=models.RunInstancesRequestSystemDisk(size=str(config['system_disk_size_gb'])),
            instance_name=config['name_prefix'], description='Homework2 Spark RDD temporary test')
        response = cloud.call_model(client, models, 'run_instances', 'RunInstancesRequest', request)
        ids = list(response.body.instance_id_sets.instance_id_set or [])
        state = dict(region_id=config['region_id'], instance_ids=ids,
            instance_type=config['instance_type'], created_at=created.isoformat(),
            expires_at=release.isoformat(), ssh_user=config['ssh_user'],
            ssh_private_key_file=private, key_pair_name=key_name, nodes=[])
        cloud.write_state(state_path, state)  # 创建后立即持久化，即使等待/SSH失败也能找回实例。
        cloud.log(f'已创建 {len(ids)} 台，服务端计划释放：{release.astimezone(cloud.BEIJING).isoformat()}')
        instances = cloud.wait_instances(client, models, config['region_id'], ids, 400)
        instances.sort(key=lambda row: row['InstanceId'])
        for index, instance in enumerate(instances, 1):
            state['nodes'].append(dict(name=f"spark-lab-{index:03d}",
                instance_id=instance['InstanceId'],
                private_ip=instance['VpcAttributes']['PrivateIpAddress']['IpAddress'][0],
                public_ip=instance['PublicIpAddress']['IpAddress'][0],
                auto_release_time=instance.get('AutoReleaseTime')))
        cloud.write_state(state_path, state)
    if datetime.fromisoformat(state['expires_at']) <= datetime.now(timezone.utc):
        raise SystemExit('集群已到期，不继续初始化。')
    nodes, key, user = state['nodes'], state['ssh_private_key_file'], state['ssh_user']
    cloud.wait_ssh(nodes, key, user, 300)
    cloud.configure_hosts(nodes, key, user)
    master = nodes[0]['private_ip']

    def bootstrap(node):
        name = node['name']
        log_path = state_path.parent / f'{name}-bootstrap.log'
        command = ['scp', '-q', '-i', key, '-o', cloud.known_hosts_option(key),
            '-o', 'StrictHostKeyChecking=accept-new', str(BASE / 'bootstrap.sh'),
            f"{user}@{cloud.ssh_endpoint(node)}:/tmp/spark-bootstrap.sh"]
        subprocess.run(command, check=True, timeout=45)
        role = 'master' if node == nodes[0] else 'worker'
        remote = f'bash /tmp/spark-bootstrap.sh {role} {shlex.quote(master)} {len(nodes)}'
        # 远端持久化日志与退出码，SSH 断线不终止初始化。
        launcher = ('nohup bash -c ' + shlex.quote(remote +
            '; code=$?; echo "$code" > /tmp/spark-bootstrap.exit; exit "$code"') +
            ' > /tmp/spark-bootstrap.log 2>&1 < /dev/null &')
        started = cloud.ssh_command(cloud.ssh_endpoint(node), user, key,
            'if test -f /tmp/spark-bootstrap.exit; then true; '
            'elif pgrep -f "^bash /tmp/spark-bootstrap.sh" >/dev/null; then true; else '
            + launcher + ' fi', 30)
        if started.returncode:
            raise RuntimeError(f'{name} 无法启动初始化')
        deadline = time.monotonic() + 1100
        while time.monotonic() < deadline:
            status = cloud.ssh_command(cloud.ssh_endpoint(node), user, key,
                'if test -f /tmp/spark-bootstrap.exit; then cat /tmp/spark-bootstrap.exit; else echo running; fi', 25)
            value = status.stdout.decode().strip()
            if value.isdigit():
                log = cloud.ssh_command(cloud.ssh_endpoint(node), user, key,
                    'cat /tmp/spark-bootstrap.log', 40)
                log_path.write_bytes(log.stdout)
                if value != '0':
                    raise RuntimeError(f'{name} 初始化失败，查看 {log_path}')
                break
            time.sleep(12)
        else:
            raise TimeoutError(f'{name} 初始化等待超时，服务端自动释放时间不变')
        cloud.log(f'{name} 初始化完成')

    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        list(pool.map(bootstrap, nodes))
    state['ready_at'] = datetime.now(timezone.utc).isoformat()
    cloud.write_state(state_path, state)
    cloud.log('Spark / HDFS / YARN 初始化完成；主节点不启动 NodeManager。')


if __name__ == '__main__':
    main()
