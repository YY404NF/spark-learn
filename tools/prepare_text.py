#!/usr/bin/env python3
"""保留原文，合并段内换行，使句子不会因排版换行被截断。"""
import argparse
from pathlib import Path

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    paragraphs, pending = [], []
    for line in args.source.read_text(encoding='utf-8-sig').splitlines():
        if line.strip():
            pending.append(line.strip())
        elif pending:
            paragraphs.append(' '.join(pending))
            pending = []
    if pending:
        paragraphs.append(' '.join(pending))
    args.output.write_text('\n'.join(paragraphs) + '\n', encoding='utf-8')
    print(f'paragraphs={len(paragraphs)}, bytes={args.output.stat().st_size}')
