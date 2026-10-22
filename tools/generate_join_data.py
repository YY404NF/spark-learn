#!/usr/bin/env python3
"""固定种子生成两表与独立参考结果。只使用标准库，不依赖数据库。"""
import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path


def generate(dest, customers=10000, orders=100000, male_ratio=0.5, seed=20261020, skew=0.0):
    if customers < 1 or orders < 0 or not 0 <= male_ratio <= 1 or not 0 <= skew <= 1:
        raise ValueError('客户数为正，订单数非负，比例在[0,1]')
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    genders = ['M'] * round(customers * male_ratio)
    genders += ['F'] * (customers - len(genders))
    rng.shuffle(genders)
    cities = ['北京', '上海', '广州', '深圳', '杭州', '成都']
    people = []
    with (dest / 'customers.txt').open('w', encoding='utf-8', newline='') as output:
        writer = csv.writer(output, lineterminator='\n')
        for i, gender in enumerate(genders):
            row = [f'C{i:08d}', f'客户{i:08d}', gender, rng.choice(cities)]
            people.append(row)
            writer.writerow(row)
    males = [i for i, row in enumerate(people) if row[2] == 'M']
    if skew and not males:
        raise ValueError('偏斜实验需要至少一名男性客户')
    expected = 0
    with (dest / 'orders.txt').open('w', encoding='utf-8', newline='') as output, \
         (dest / 'expected.tsv').open('w', encoding='utf-8') as reference:
        writer = csv.writer(output, lineterminator='\n')
        for i in range(orders):
            index = males[0] if skew and rng.random() < skew else rng.randrange(customers)
            customer = people[index]
            order = [f'O{i:010d}', customer[0], str(date(2026, 1, 1) + timedelta(days=rng.randrange(293)))]
            writer.writerow(order)
            if customer[2] == 'M':
                reference.write('\t'.join(order + [customer[1], customer[2], customer[3]]) + '\n')
                expected += 1
    summary = dict(customers=customers, orders=orders, male_customers=len(males),
        male_orders=expected, male_ratio=male_ratio, seed=seed, skew=skew,
        bytes={name: (dest / name).stat().st_size for name in ('orders.txt', 'customers.txt')})
    (dest / 'manifest.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--customers', type=int, default=10000)
    parser.add_argument('--orders', type=int, default=100000)
    parser.add_argument('--male-ratio', type=float, default=0.5)
    parser.add_argument('--seed', type=int, default=20261020)
    parser.add_argument('--skew', type=float, default=0)
    parser.add_argument('--output-dir', default='data/generated')
    args = parser.parse_args()
    print(json.dumps(generate(args.output_dir, args.customers, args.orders,
        args.male_ratio, args.seed, args.skew), ensure_ascii=False, indent=2))
