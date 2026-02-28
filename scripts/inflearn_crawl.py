# Inflearn crawler for Statground

import os, re, json, time, random
from datetime import datetime
import requests
from lxml import etree
from tenacity import retry, stop_after_attempt, wait_exponential
import clickhouse_connect
from uuid6 import uuid7

CH_HOST = os.environ["CH_HOST"]
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DATABASE = os.environ.get("CH_DATABASE", "statground_lecture")

SITEMAP_BASE = os.environ.get("SITEMAP_BASE").rstrip("/")
SITEMAP_PREFIX = os.environ.get("SITEMAP_PREFIX")
MAX_SITEMAP_INDEX = int(os.environ.get("MAX_SITEMAP_INDEX"))
MAX_COURSES_PER_RUN = int(os.environ.get("MAX_COURSES_PER_RUN"))

SLEEP_MIN = float(os.environ.get("REQUEST_SLEEP_MIN"))
SLEEP_MAX = float(os.environ.get("REQUEST_SLEEP_MAX"))

UA = "Mozilla/5.0 (compatible; StatgroundCrawler/1.0)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

def now_dt():
    t = datetime.now()
    return t.strftime("%Y-%m-%d %H:%M:%S.000")

def jitter():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

@retry(stop=stop_after_attempt(5), wait=wait_exponential())
def http_get(url):
    r = SESSION.get(url, timeout=30)
    if r.status_code in (429,500,502,503,504):
        raise RuntimeError("Retryable error")
    return r

def parse_sitemap(xml_bytes):
    root = etree.fromstring(xml_bytes)
    return root.xpath(".//sm:loc/text()", namespaces=NS)

def discover():
    urls=[]
    for i in range(MAX_SITEMAP_INDEX):
        sm=f"{SITEMAP_BASE}/{SITEMAP_PREFIX}{i}.xml"
        r=http_get(sm)
        if r.status_code!=200: break
        locs=parse_sitemap(r.content)
        if not locs: break
        urls.extend(locs)
        jitter()
    return list(dict.fromkeys(urls))

def extract_next(html):
    m=re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',html,re.DOTALL)
    return json.loads(m.group(1)) if m else None

def client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE
    )

def main():
    ch=client()
    fetched_at=now_dt()
    urls=discover()[:MAX_COURSES_PER_RUN]

    for url in urls:
        r=http_get(url)
        if r.status_code!=200: continue
        nd=extract_next(r.text)
        if not nd: continue
        page_props=(nd.get("props") or {}).get("pageProps") or {}
        queries=(page_props.get("dehydratedState") or {}).get("queries") or []
        for q in queries:
            qk=json.dumps(q.get("queryKey"),ensure_ascii=False)
            data=(q.get("state") or {}).get("data")
            if not data: continue
            payload=json.dumps(data,ensure_ascii=False)
            ch.insert(
                "inflearn_course_snapshot_raw",
                [[str(uuid7()),fetched_at,0,"en",url,qk,abs(hash(qk)), "OK", None, payload, abs(hash(payload))]],
                column_names=["uuid","fetched_at","course_id","locale","source_url","query_key","query_key_hash","status_code","error_code","payload","payload_hash"]
            )
        jitter()

if __name__=="__main__":
    main()
