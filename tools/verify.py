#!/usr/bin/env python3
"""与 Spark 独立的标准库参考计算，按多重集核对全部结果。"""
import argparse
from collections import Counter
from itertools import combinations
import json
import math
from pathlib import Path
import re


def lines(path):
    files = sorted(path.glob('part-*')) if path.is_dir() else [path]
    return [line for f in files for line in f.read_text().splitlines() if line]


def tokens(text):
    return re.findall('[a-z]+', text.lower())


def sentence_tokens(text):
    # 与输入约定一致：句子不跨段落行。
    return [tokens(s) for line in text.splitlines() for s in re.split(r'[.!?。！？]+', line) if tokens(s)]


def ranked(counts, n):
    return [f'{word}\t{count}' for word, count in sorted(counts.items(), key=lambda row: (-row[1], row[0]))[:n]]


def verify(data, result):
    checks = []
    def same(label, actual, expected, ordered=False):
        observed = lines(actual)
        ok = observed == expected if ordered else Counter(observed) == Counter(expected)
        checks.append(dict(check=label, passed=ok, rows=len(observed), expected_rows=len(expected)))
        if not ok:
            raise AssertionError(json.dumps(checks[-1], ensure_ascii=False))
    for source, suffix, n in [('words-small.txt', 'small', 2), ('alice.txt', 'alice', 10)]:
        text = (data / source).read_text()
        same('topn-' + suffix, result / ('topn-' + suffix), ranked(Counter(tokens(text)), n), True)
    for source, suffix, threshold in [('phrases-small.txt', 'small', 1), ('alice.txt', 'alice', 2)]:
        sentences = sentence_tokens((data / source).read_text())
        for k in (2, 3):
            counts = Counter(' '.join(words[i:i+k]) for words in sentences for i in range(len(words)-k+1))
            counts = Counter({key: count for key, count in counts.items() if count >= threshold})
            same(f'phrases-{suffix}-{k}', result / ('phrases-' + suffix) / str(k), ranked(counts, 20), True)
    for source, suffix, support in [('phrases-small.txt', 'small', .5), ('alice.txt', 'alice', .02)]:
        transactions = [sorted(set(words)) for words in sentence_tokens((data / source).read_text())]
        t = len(transactions)
        minimum = math.ceil(support*t)
        one = Counter(word for words in transactions for word in words)
        frequent_one = {word for word, count in one.items() if count >= minimum}
        for k in (1, 2, 3):
            counts = Counter(' '.join(combination) for words in transactions
                for combination in combinations([w for w in words if w in frequent_one], k))
            counts = Counter({key: count for key, count in counts.items() if count >= minimum})
            expected = [f'{key}\t{count}\t{count/t:.8f}' for key, count in counts.items()]
            same(f'apriori-{suffix}-l{k}', result / ('apriori-' + suffix) / f'l{k}', expected)
            same(f'apriori-{suffix}-top{k}', result / ('apriori-' + suffix) / f'top{k}', ranked(counts,20), True)
    people = {row.split(',')[0]: row.split(',') for row in lines(data/'customers.txt')}
    expected=[]
    for line in lines(data/'orders.txt'):
        order=line.split(','); customer=people.get(order[1])
        if customer and customer[2]=='M': expected.append('\t'.join(order+[customer[1],customer[2],customer[3]]))
    same('join-small',result/'join-small',expected)
    generated=lines(data/'generated/expected.tsv')
    same('join-early',result/'join-early',generated)
    same('join-late',result/'join-late',generated)
    return dict(passed=all(c['passed'] for c in checks), checks=checks)


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--data',type=Path,default=Path('data'))
    parser.add_argument('--results',type=Path,default=Path('results'))
    args=parser.parse_args()
    print(json.dumps(verify(args.data,args.results),ensure_ascii=False,indent=2))
