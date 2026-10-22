#!/usr/bin/env python3
"""流式汇总未压缩 Spark JSON event log。"""
import argparse
import json
from pathlib import Path

def summarize(path):
    result = dict(executors={}, jobs=0, stages=0, tasks=0,
        shuffle_read_bytes=0, shuffle_write_bytes=0, failed_tasks=0)
    with path.open() as source:
        for line in source:
            event = json.loads(line)
            kind = event['Event']
            if kind == 'SparkListenerExecutorAdded':
                result['executors'][event['Executor ID']] = event['Executor Info']['Host']
            elif kind == 'SparkListenerJobStart':
                result['jobs'] += 1
            elif kind == 'SparkListenerStageCompleted':
                result['stages'] += 1
            elif kind == 'SparkListenerTaskEnd':
                result['tasks'] += 1
                result['failed_tasks'] += int(event['Task End Reason'].get('Reason') != 'Success')
                metrics = event.get('Task Metrics', {})
                read = metrics.get('Shuffle Read Metrics', {})
                result['shuffle_read_bytes'] += read.get('Remote Bytes Read', 0) + read.get('Local Bytes Read', 0)
                result['shuffle_write_bytes'] += metrics.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('event_log', type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.event_log), ensure_ascii=False, indent=2))
