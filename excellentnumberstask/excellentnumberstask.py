# filename: harvest_numbers_from_index.py
import os, json, time, glob, random
from typing import Dict, Iterable, Tuple, Optional

from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from .excellentnumbers_extractor import ExcellentNumbersScraper
from .excellentnumbers_state_area import StateAreaCodeScraper  # 引入 StateAreaCodeScraper

DEFAULT_INDEX_LATEST = "/tmp/excellentnumbers_state_area_codes.json"
DEFAULT_INDEX_GLOB   = "/tmp/excellentnumbers_state_area_codes_*.json"

class AreaCodeNumbersHarvester:
    def __init__(
        self,
        mongo_host: str,
        mongo_user: str = "root",
        mongo_password: str = "pp963470667",
        mongo_port: int = 27017,
        mongo_db: str = "extra_numbers",
        mongo_collection: str = "numbers",
        headless: bool = True,
        # ↓↓↓ 节流/停顿配置 ↓↓↓
        min_delay: float = 1.2,     # 每个 URL 之间最小停顿
        max_delay: float = 3.5,     # 每个 URL 之间最大停顿
        long_pause_every: int = 20, # 每处理 N 个 URL 做一次较长停顿（0=关闭）
        long_pause_range: Tuple[float, float] = (8.0, 15.0),  # 长停顿范围
        retries: int = 2,           # 失败重试次数（不含首尝试）
        retry_backoff_base: float = 1.8,  # 重试指数退避底数
        retry_jitter: Tuple[float, float] = (0.3, 0.9),      # 重试抖动
    ):
        self.scraper = ExcellentNumbersScraper(
            mongo_host=mongo_host,
            mongo_user=mongo_user,
            mongo_password=mongo_password,
            mongo_port=mongo_port,
            mongo_db=mongo_db,
            mongo_collection=mongo_collection,
            headless=headless,
        )
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.long_pause_every = long_pause_every
        self.long_pause_range = long_pause_range
        self.retries = retries
        self.retry_backoff_base = retry_backoff_base
        self.retry_jitter = retry_jitter

    # ---------------- 文件加载 ----------------
    def _pick_index_file(self, path_or_dir: Optional[str]) -> str:
        if path_or_dir and os.path.isfile(path_or_dir):
            return path_or_dir
        base_dir = path_or_dir if (path_or_dir and os.path.isdir(path_or_dir)) else "."
        candidates = sorted(
            glob.glob(os.path.join(base_dir, DEFAULT_INDEX_GLOB)),
            key=lambda p: os.path.getmtime(p),
            reverse=True,
        )
        if candidates:
            return candidates[0]
        fallback = os.path.join(base_dir, DEFAULT_INDEX_LATEST)
        if os.path.exists(fallback):
            return fallback
        raise FileNotFoundError(f"未找到索引文件：{DEFAULT_INDEX_GLOB} 或 {DEFAULT_INDEX_LATEST}")

    def _load_index(self, path: str) -> Dict:
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"索引文件损坏或未写完: {path} ({e})")

    def _append_sort_params(self, url: str) -> str:
        """确保 URL 拼上排序参数 ?sort=newest&sortcode="""
        p = urlparse(url)
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        q["sort"] = "newest"
        if "sortcode" not in q:
            q["sortcode"] = ""
        new_query = urlencode(q, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))

    def _iter_state_urls(self, data: Dict) -> Iterable[Tuple[str, str, str]]:
        regions = data.get("regions", {})
        for state, info in regions.items():
            for ac in info.get("area_codes", []):
                code, url = ac.get("code"), ac.get("url")
                if code and url:
                    yield state, code, self._append_sort_params(url)

    # ---------------- 人类化停顿 ----------------
    def _human_pause(self, i: int):
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)
        if self.long_pause_every and i > 0 and i % self.long_pause_every == 0:
            long_delay = random.uniform(*self.long_pause_range)
            print(f"[PAUSE] Long pause ~{long_delay:.1f}s after {i} URLs")
            time.sleep(long_delay)

    def _retry_sleep(self, attempt: int):
        base = self.retry_backoff_base ** (attempt - 1)
        jitter = random.uniform(*self.retry_jitter)
        time.sleep(base + jitter)

    # ---------------- 主流程 ----------------
    def run(self, index_path_or_dir: Optional[str] = None, limit: Optional[int] = None) -> Dict:
        # 先检查索引文件是否存在，如果不存在则生成
        base_dir = index_path_or_dir if (index_path_or_dir and os.path.isdir(index_path_or_dir)) else "."
        fallback = os.path.join(base_dir, DEFAULT_INDEX_LATEST)
        candidates = sorted(
            glob.glob(os.path.join(base_dir, DEFAULT_INDEX_GLOB)),
            key=lambda p: os.path.getmtime(p),
            reverse=True,
        )
        
        # 如果既没有时间戳文件也没有默认文件，则先生成
        if not candidates and not os.path.exists(fallback):
            print(f"[INFO] No index file found in {base_dir}. Running StateAreaCodeScraper first...")
            try:
                scraper = StateAreaCodeScraper()
                scraper.run()  # 执行抓取操作
                print(f"[INFO] StateAreaCodeScraper completed successfully")
            except Exception as e:
                print(f"[ERROR] Failed to generate index file: {e}")
                raise RuntimeError(f"无法生成索引文件: {e}")
        
        # 现在可以安全地获取索引文件
        try:
            index_file = self._pick_index_file(index_path_or_dir)
        except FileNotFoundError as e:
            print(f"[ERROR] Index file still not found after generation attempt: {e}")
            raise
        
        # 加载索引并开始处理
        data = self._load_index(index_file)
        urls = list(self._iter_state_urls(data))
        if limit:
            urls = urls[:limit]

        processed, success, failures, total_numbers = 0, 0, 0, 0
        seen_urls = set()
        print(f"[INDEX] Using: {index_file} | URL count: {len(urls)}")
        start_ts = time.time()

        for idx, (state, code, url) in enumerate(urls, start=1):
            if url in seen_urls:
                continue
            seen_urls.add(url)
            processed += 1

            ok = False
            for attempt in range(1, self.retries + 2):  # 首次 + 重试
                try:
                    print(f"[{processed}/{len(urls)}] {state} {code} -> {url} (try {attempt})")
                    rows = self.scraper.run(url)  # 内部已写入 Mongo
                    total_numbers += len(rows)
                    ok = True
                    break
                except Exception as e:
                    print(f"[WARN] Failed: {url} | {e}")
                    if attempt <= self.retries:
                        self._retry_sleep(attempt)
            if ok:
                success += 1
            else:
                failures += 1

            self._human_pause(idx)

        elapsed = round(time.time() - start_ts, 2)
        summary = {
            "index_file": index_file,
            "processed_urls": processed,
            "success_urls": success,
            "failed_urls": failures,
            "numbers_captured_this_run": total_numbers,
            "elapsed_sec": elapsed,
        }
        print(f"[DONE] {summary}")
        return summary


if __name__ == "__main__":
    job = AreaCodeNumbersHarvester(
        mongo_host="43.159.58.235",
        mongo_user="extra_numbers",
        mongo_password="RsBWd3hTAZeR7kC4",
        mongo_port=27017,
        mongo_db="extra_numbers",
        mongo_collection="numbers",
        headless=True,
        min_delay=1.2,
        max_delay=3.5,
        long_pause_every=20,
        long_pause_range=(8.0, 15.0),
        retries=2,
        retry_backoff_base=1.8,
        retry_jitter=(0.3, 0.9),
    )
    job.run(index_path_or_dir=".", limit=None)
