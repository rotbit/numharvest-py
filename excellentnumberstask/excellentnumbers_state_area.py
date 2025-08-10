# filename: excellentnumbers_state_area_codes_locator_call.py
import asyncio, json, os, time, random
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote
from playwright.async_api import async_playwright

DEFAULT_URL = "https://excellentnumbers.com/"
DEFAULT_OUT = "/tmp/excellentnumbers_state_area_codes.json"
RIGHT_SEL = [
    "aside a[href*='/categories/']",
    "#sidebar a[href*='/categories/']",
    ".sidebar a[href*='/categories/']",
    ".right-sidebar a[href*='/categories/']",
]

class StateAreaCodeScraper:
    def __init__(self, headless=True, wait_ms=800, max_age_days=7):
        self.headless, self.wait_ms, self.max_age_s = headless, wait_ms, max_age_days*86400

    # ------- 人类化停顿/滚动 -------
    def _human_sleep(self, a: float, b: float):
        time.sleep(random.uniform(a, b))

    async def _human_scroll(self, page, steps: int = 6, px_each: int = 600):
        # 分步滚动 + 随机停顿，触发懒加载
        for _ in range(steps):
            await page.evaluate(f"window.scrollBy(0,{px_each});")
            self._human_sleep(0.25, 0.7)
        await page.evaluate("window.scrollTo(0,0);")  # 回到顶部，避免干扰定位
        self._human_sleep(0.2, 0.5)

    # ------- 解析/采集 -------
    def _parse(self, href):
        parts = [p for p in urlparse(href).path.split("/") if p]
        return (unquote(parts[1]), parts[2]) if len(parts)>=3 and parts[0]=="categories" and parts[2].isdigit() else (None,None)

    async def _links(self, page, base):
        links = []
        async def collect(sel):
            loc = page.locator(sel); n = await loc.count()
            # 选择器级随机停顿
            self._human_sleep(0.3, 0.9)
            for i in range(n):
                a = loc.nth(i)
                href = await a.get_attribute("href")
                if href:
                    links.append(href if href.startswith("http") else urljoin(base, href))
                # 元素级随机停顿（更像人）
                self._human_sleep(0.05, 0.18)
        for s in RIGHT_SEL:
            await collect(s)
        if not links:
            await collect("a[href*='/categories/']")
        return links

    async def scrape(self, url):
        async with async_playwright() as p:
            b = await p.chromium.launch(headless=self.headless); pg = await b.new_page()
            await pg.goto(url, wait_until="networkidle", timeout=60_000)
            # 初始固定等待 + 人类式滚动 + 轻微抖动
            await pg.wait_for_timeout(self.wait_ms)
            await self._human_scroll(pg, steps=random.randint(5,8), px_each=random.randint(500,800))

            states, codes, seen = {}, {}, set()
            for link in await self._links(pg, url):
                state, code = self._parse(link)
                if not state or not code or (state, code) in seen: continue
                seen.add((state, code))
                states.setdefault(state, {"name": state, "area_codes": []})["area_codes"].append({"code": code, "url": link})
                codes[code] = {"code": code, "state": state, "url": link}
            for st in states.values():
                st["area_codes"].sort(key=lambda x:int(x["code"]))
                st["total_area_codes"] = len(st["area_codes"])
            await b.close()
        return {
            "regions": dict(sorted(states.items())),
            "area_codes": dict(sorted(codes.items())),
            "summary": {"total_regions": len(states), "total_area_codes": len(codes), "source_url": url},
        }

    # ------- 缓存/落盘 -------
    def _is_fresh(self, path):
        return os.path.exists(path) and (time.time() - os.path.getmtime(path) < self.max_age_s)

    def _ts_name(self, base):
        root, ext = os.path.splitext(base)
        return f"{root}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext or '.json'}"

    def run(self, url=DEFAULT_URL, out=DEFAULT_OUT):
        if self._is_fresh(out):
            with open(out, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"[CACHE] Use fresh file (<=7 days): {out}")
            return data
        data = asyncio.run(self.scrape(url))
        ts_out = self._ts_name(out)
        with open(ts_out, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
        with open(out, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[OK] Saved -> {ts_out} (and updated latest -> {out})")
        return data

if __name__ == "__main__":
    StateAreaCodeScraper().run()
