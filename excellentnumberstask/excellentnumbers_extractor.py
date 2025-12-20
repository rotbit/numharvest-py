# filename: excellentnumbers_scraper_mongo.py
"""
Usage:
  pip install playwright bs4 lxml pymongo
  python -m playwright install

  python excellentnumbers_scraper_mongo.py
"""

import asyncio
import re
import time
import random
from typing import List, Dict, Optional
from urllib.parse import urljoin
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pymongo import MongoClient, ASCENDING, ReplaceOne


class ExcellentNumbersScraper:
    # US/CA 常见电话格式
    PHONE_RE = re.compile(
        r"""
        (?<!\d)
        (?:\+1[\s.\-]?)?
        \(?\d{3}\)?
        [\s.\-]?
        \d{3}
        [\s.\-]?
        \d{4}
        (?!\d)
        """,
        re.VERBOSE,
    )
    # 价格格式 $1,234 或 $99.99
    PRICE_RE = re.compile(r"\$\s?\d{1,3}(?:,\d{3})*(?:\.\d{2})?")
    NEXT_TEXT_CANDIDATES = {"next", ">", "»", "next »", "older", "下一页"}

    def __init__(
        self,
        mongo_host: str,
        mongo_user: str = "root",
        mongo_password: str = "pp963470667",
        mongo_port: int = 27017,
        mongo_db: str = "excellentnumbers",
        mongo_collection: str = "numbers",
        headless: bool = True,
        page_timeout_ms: int = 60_000,
        page_pause_sec: float = 0.8,          # 原有的固定停顿（仍保留）
        user_agent: Optional[str] = None,
        # ↓↓↓ 新增：人类化停顿/滚动参数 ↓↓↓
        min_delay: float = 0.9,               # 每页之间的最小随机停顿（包含翻页）
        max_delay: float = 2.2,               # 每页之间的最大随机停顿
        jitter_ms: int = 400,                 # 每次加载后的轻微随机等待（毫秒）
        scroll_steps_range: tuple = (5, 8),   # 人类式滚动步数范围
        scroll_px_range: tuple = (450, 800),  # 每步滚动像素范围
        long_pause_every: int = 0,            # 每翻 N 页再做一次长停顿（0=关闭）
        long_pause_range: tuple = (6.0, 12.0) # 长停顿范围
    ):
        """
        只需传 Mongo 的 IP、用户、密码（若无认证可留空 user/password）。
        """
        # Playwright 配置
        self.headless = headless
        self.page_timeout_ms = page_timeout_ms
        self.page_pause_sec = page_pause_sec
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )

        # 人类化参数
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.jitter_ms = jitter_ms
        self.scroll_steps_range = scroll_steps_range
        self.scroll_px_range = scroll_px_range
        self.long_pause_every = long_pause_every
        self.long_pause_range = long_pause_range

        # MongoDB 连接（按你原来的写法）
        uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"
        self.mongo = MongoClient(uri)
        self.col = self.mongo[mongo_db][mongo_collection]
        self.html_col = self.mongo[mongo_db]["page_html"]
        # 唯一索引（保持你原来的：仅 phone 唯一）
        self.col.create_index("phone", unique=True)
        # HTML 按 source+url 唯一，便于覆写最新页面
        self.html_col.create_index([("source", ASCENDING), ("url", ASCENDING)], unique=True)
        self.html_col.create_index("fetched_at")

    # ---------- 人类化动作 ----------
    def _human_sleep(self):
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    async def _human_scroll(self, page):
        steps = random.randint(*self.scroll_steps_range)
        for _ in range(steps):
            px = random.randint(*self.scroll_px_range)
            await page.evaluate(f"window.scrollBy(0,{px});")
            # 小停顿模拟阅读
            time.sleep(random.uniform(0.25, 0.7))
        # 回到顶部，避免影响定位
        await page.evaluate("window.scrollTo(0,0);")
        time.sleep(random.uniform(0.2, 0.5))

    # ---------- Playwright 基础 ----------
    async def _get_page_html(self, page, url: str) -> str:
        await page.goto(url, wait_until="load", timeout=self.page_timeout_ms)
        # 原固定等待 + 轻微抖动
        await page.wait_for_timeout(800 + random.randint(0, self.jitter_ms))
        # 人类式滚动触发懒加载
        await self._human_scroll(page)
        return await page.content()

    # ---------- 提取逻辑（先站点特化，失败再通用） ----------
    @classmethod
    def _clean_phone(cls, s: str) -> str:
        digits = re.sub(r"\D", "", s)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10:
            return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
        return s.strip()

    @classmethod
    def _extract_site_specific(cls, soup: BeautifulSoup) -> List[Dict[str, str]]:
        results = []
        containers = soup.select("div, li, article, tr, section")
        for c in containers:
            text = c.get_text(" ", strip=True)
            if not text:
                continue
            phones = cls.PHONE_RE.findall(text)
            prices = cls.PRICE_RE.findall(text)
            if not phones or not prices:
                continue
            results.append({"phone": cls._clean_phone(phones[0]), "price": prices[0].replace(" ", "")})
        dedup = {(r["phone"], r["price"]): r for r in results}
        return list(dedup.values())

    @classmethod
    def _extract_generic(cls, soup: BeautifulSoup) -> List[Dict[str, str]]:
        results = []
        for block in soup.select("div, li, article, tr, section"):
            t = block.get_text(" ", strip=True)
            if not t:
                continue
            phones = cls.PHONE_RE.findall(t)
            prices = cls.PRICE_RE.findall(t)
            if phones and prices:
                results.append({"phone": cls._clean_phone(phones[0]), "price": prices[0].replace(" ", "")})
        dedup = {(r["phone"], r["price"]): r for r in results}
        return list(dedup.values())

    @classmethod
    def _extract_pairs_from_html(cls, html: str) -> List[Dict[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        rows = cls._extract_site_specific(soup)
        if not rows:
            rows = cls._extract_generic(soup)
        return rows

    # ---------- 分页 ----------
    @classmethod
    def _find_next_url(cls, html: str, current_url: str) -> Optional[str]:
        soup = BeautifulSoup(html, "lxml")
        pagers = soup.select('nav, ul.pagination, div.pagination, div.pager, footer, div[role="navigation"]') or [soup]

        def is_next_text(s: str) -> bool:
            s_norm = s.strip().lower()
            return s_norm in cls.NEXT_TEXT_CANDIDATES or "next" in s_norm

        for container in pagers:
            for a in container.select("a[href]"):
                label = a.get_text(" ", strip=True)
                if is_next_text(label):
                    href = a.get("href")
                    if href:
                        return urljoin(current_url, href)

        link = soup.select_one('a[rel="next"][href]')
        return urljoin(current_url, link["href"]) if link else None

    # ---------- MongoDB 批量写入 ----------
    def _bulk_upsert(self, rows: List[Dict[str, str]], source_url: str):
        if not rows:
            return
        now = datetime.now(timezone.utc)
        ops = []
        
        for r in rows:
            phone = r["phone"]
            new_price = r["price"]
            
            # 先查询现有记录
            existing = self.col.find_one({"phone": phone})
            
            if existing is None:
                # 新记录，直接插入
                doc = {
                    "phone": phone,
                    "price": new_price,
                    "source_url": source_url, 
                    "source": "excellent_number", 
                    "crawled_at": now
                }
                ops.append(ReplaceOne({"phone": phone}, doc, upsert=True))
            elif existing.get("price") != new_price:
                # 价格不同，更新记录
                doc = {
                    "phone": phone,
                    "price": new_price,
                    "source_url": source_url, 
                    "source": "excellent_number", 
                    "crawled_at": now
                }
                ops.append(ReplaceOne({"phone": phone}, doc, upsert=True))
            # 如果价格相同，跳过（不添加到ops中）
        
        if ops:
            result = self.col.bulk_write(ops, ordered=False)
            upserted = getattr(result, 'upserted_count', 0) or 0
            modified = getattr(result, 'modified_count', 0) or 0
            skipped = len(rows) - len(ops)
            print(f"[MONGO] upserted={upserted}, modified={modified}, skipped={skipped}")
        else:
            print(f"[MONGO] skipped={len(rows)} (all records identical)")

    # ---------- HTML 原文存档 ----------
    def _save_html_snapshot(self, url: str, html: str, page_no: int) -> None:
        """将原始页面 HTML 保存到 MongoDB，去重键为 source+url。"""
        if not url or not html:
            return
        now = datetime.now(timezone.utc)
        try:
            self.html_col.update_one(
                {"source": "excellent_numbers", "url": url},
                {
                    "$set": {
                        "html": html,
                        "page_no": page_no,
                        "fetched_at": now,
                        "updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        except Exception as exc:
            print(f"[WARN] 保存 HTML 失败 {url}: {exc}")

    # ---------- 抓取主流程 ----------
    async def scrape(self, url: str) -> List[Dict[str, str]]:
        """抓取并返回本轮抓到的 (phone, price) 去重列表（同时已写入 Mongo）。"""
        all_rows: List[Dict[str, str]] = []
        visited = set()
        page_count = 0

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=self.user_agent)
            page = await context.new_page()

            cur = url
            while cur and cur not in visited:
                visited.add(cur)
                page_count += 1
                print(f"[INFO] Fetching: {cur}")

                try:
                    html = await self._get_page_html(page, cur)
                except PlaywrightTimeoutError:
                    print(f"[WARN] Timeout loading {cur}, skip.")
                    break

                self._save_html_snapshot(cur, html, page_count)

                rows = self._extract_pairs_from_html(html)
                print(f"[INFO] Found {len(rows)} items on this page.")
                self._bulk_upsert(rows, source_url=cur)
                all_rows.extend(rows)

                # 找下一页
                nxt = self._find_next_url(html, cur)
                if nxt and nxt not in visited:
                    # 页间随机停顿（人类化）
                    self._human_sleep()
                    # 可选：每 N 页做长停顿
                    if self.long_pause_every and page_count % self.long_pause_every == 0:
                        lp = random.uniform(*self.long_pause_range)
                        print(f"[PAUSE] Long pause ~{lp:.1f}s after {page_count} pages")
                        time.sleep(lp)
                    cur = nxt
                else:
                    cur = None

            await context.close()
            await browser.close()

        # 结果再去重
        dedup = {(r["phone"], r["price"]): r for r in all_rows}
        return list(dedup.values())

    # ---------- 便捷入口 ----------
    def run(self, url: str) -> List[Dict[str, str]]:
        return asyncio.run(self.scrape(url))


if __name__ == "__main__":
    # ✅ 修改为你的 MongoDB 连接信息
    scraper = ExcellentNumbersScraper(
        mongo_host="43.159.58.235",
        mongo_user="root",
        mongo_password="pp963470667",
        mongo_port=27017,
        mongo_db="extra_numbers",
        mongo_collection="numbers",
        headless=True,
        # ↓ 可根据需要微调人类化参数
        min_delay=1.0,
        max_delay=2.8,
        jitter_ms=500,
        scroll_steps_range=(5, 8),
        scroll_px_range=(480, 820),
        long_pause_every=0,            # 设为 0 表示关闭长停顿
        long_pause_range=(6.0, 12.0),
    )

    start_url = "https://excellentnumbers.com/categories/Pennsylvania/582?sort=newest&sortcode="
    data = scraper.run(start_url)
    print(f"[DONE] Got {len(data)} unique rows this run.")
