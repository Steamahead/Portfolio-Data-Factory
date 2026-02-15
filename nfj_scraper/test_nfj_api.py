# -*- coding: utf-8 -*-
import requests
import json
import sys
sys.stdout.reconfigure(encoding='utf-8')

# salaryCurrency i salaryPeriod jako query params
url = 'https://nofluffjobs.com/api/search/posting?salaryCurrency=PLN&salaryPeriod=month'
headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Origin': 'https://nofluffjobs.com',
    'Referer': 'https://nofluffjobs.com/pl/data'
}

payload = {
    'criteriaSearch': {'category': ['data']},
    'rawSearch': 'category=data',
    'page': 1
}

print(f"URL: {url}")
print(f"Payload: {json.dumps(payload)}")

try:
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"Total count: {data.get('totalCount', 0)}")
        if 'postings' in data:
            for i, p in enumerate(data['postings'][:3], 1):
                print(f"\n=== Oferta {i} ===")
                print(f"Title: {p.get('title')}")
                print(f"Company: {p.get('name')}")
                print(f"Salary: {json.dumps(p.get('salary'), ensure_ascii=False)}")
                print(f"Tiles: {json.dumps(p.get('tiles'), ensure_ascii=False)}")
    else:
        print(f"Error: {r.text[:500]}")
except Exception as e:
    print(f"Exception: {e}")
