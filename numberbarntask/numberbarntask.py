import asyncio
import json
import os
import random
import re
import time
from datetime import datetime
from typing import Dict, List

import requests
from playwright.async_api import async_playwright
from pymongo import MongoClient
from progress_tracker import MongoProgressTracker

DEFAULT_JSON_FILE = "/tmp/numberbarn_state_npa_cache.json"  # 本地文件存储路径
API_URL = "https://www.numberbarn.com/api/npas?$limit=1000"  # 获取 combinations 的 API 接口
# 页面内抽取号码与价格的脚本，放在模块级避免函数过长
JS_EXTRACT_SCRIPT = """
() => {
    const numbers = [];
    const phonePattern = /(\\(\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4})/g;
    const pricePattern = /\\$[\\d,]+\\.?\\d*/g;
    const nodes = document.querySelectorAll(
        '.number-item, .phone-number, .listing-item, [data-phone], .search-result, .result-item'
    );

    const pushMatch = (text) => {
        const phones = text.match(phonePattern);
        if (!phones) return;
        const price = text.match(pricePattern)?.[0] ?? '';
        numbers.push({ number: phones[0].trim(), price });
    };

    if (nodes.length) {
        nodes.forEach(el => pushMatch(el.textContent || ''));
    }
    if (!numbers.length) {
        const bodyText = document.body.textContent || '';
        const uniquePhones = [...new Set(bodyText.match(phonePattern) || [])];
        uniquePhones.forEach(phone => {
            const price = bodyText.match(pricePattern)?.[0] ?? '';
            numbers.push({ number: phone.trim(), price });
        });
    }
    return numbers;
}
"""

class NumberbarnNumberExtractor:
    """专门用于从numberbarn.com提取号码和价格的简化爬虫"""

    def __init__(self, mongo_host: str = "43.159.58.235",
                 mongo_password: str = "pp963470667",
                 mongo_db: str = "extra_numbers"):
        self.mongo_host = mongo_host
        self.mongo_password = mongo_password
        self.mongo_db = mongo_db
        self.mongo_client = None
        self.db = None
        self.collection = None

        # 初始化MongoDB连接
        self.init_mongodb()

    def init_mongodb(self):
        """初始化MongoDB连接"""
        try:
            connection_string = f"mongodb://root:{self.mongo_password}@{self.mongo_host}:27017/{self.mongo_db}?authSource=admin"
            self.mongo_client = MongoClient(connection_string)
            self.db = self.mongo_client[self.mongo_db]
            self.collection = self.db['numbers']

            # 测试连接
            self.mongo_client.admin.command('ping')
            print(f"成功连接到MongoDB数据库: {self.mongo_db}")

        except Exception as e:
            print(f"MongoDB连接失败: {e}")
            self.mongo_client = None

    def get_combinations_from_file(self, json_file: str = DEFAULT_JSON_FILE) -> List[Dict]:
        """从本地JSON文件获取state和npa的组合"""
        if not os.path.exists(json_file):
            return []

        try:
            file_timestamp = os.path.getmtime(json_file)
            current_time = time.time()

            # 如果文件超过一周未更新，返回空列表重新获取
            if current_time - file_timestamp > 7 * 86400:
                return []

            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            combinations = []
            if isinstance(data, list):
                combinations = data
            elif isinstance(data, dict) and 'combinations' in data:
                combinations = data['combinations']

            print(f"从本地文件读取到 {len(combinations)} 条state-npa组合")
            return combinations

        except Exception as e:
            print(f"从本地文件读取state-npa组合失败: {e}")
            return []

    def save_combinations_to_file(self, combinations: List[Dict], json_file: str = DEFAULT_JSON_FILE):
        """将组合数据保存到本地JSON文件"""
        try:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump({'combinations': combinations}, f, ensure_ascii=False, indent=2)
            print(f"成功保存 {len(combinations)} 条组合到文件: {json_file}")
        except Exception as e:
            print(f"保存组合到本地文件失败: {e}")

    def get_combinations_from_api(self) -> List[Dict]:
        """从接口获取所有state和npa的组合"""
        try:
            print(f"从API获取state和npa组合: {API_URL}")
            response = requests.get(API_URL, timeout=30)
            response.raise_for_status()  # 如果请求失败，抛出异常

            data = response.json()
            combinations = []

            if isinstance(data, dict) and 'data' in data:
                for entry in data['data']:
                    state = entry.get('state')
                    npa = entry.get('npa')
                    if state and npa and len(str(npa)) == 3:
                        combinations.append({
                            'state': str(state).upper(),
                            'npa': str(npa)
                        })

            deduped = []
            seen = set()
            for combo in combinations:
                key = (combo['state'], combo['npa'])
                if key not in seen:
                    seen.add(key)
                    deduped.append(combo)

            print(f"从API获取到 {len(deduped)} 条有效组合")
            return deduped

        except Exception as e:
            print(f"从API获取state-npa组合失败: {e}")
            return []

    def load_combinations(self, json_file: str = DEFAULT_JSON_FILE) -> List[Dict]:
        """优先使用本地缓存，其次调用API并刷新缓存。"""
        combinations = self.get_combinations_from_file(json_file)
        if combinations:
            return combinations

        combinations = self.get_combinations_from_api()
        if combinations:
            self.save_combinations_to_file(combinations, json_file)
        return combinations

    def save_numbers_to_mongodb(self, numbers: List[Dict]) -> bool:
        """将号码列表保存到MongoDB，每个号码一条记录"""
        if not self.mongo_client or not numbers:
            return False

        current_time = datetime.utcnow()
        documents = [
            {
                'phone': number_data.get('number', ''),
                'price': number_data.get('price', ''),
                'source_url': number_data.get('source_url', ''),
                'source': "numberbarn",
                'crawled_at': current_time
            }
            for number_data in numbers
            if number_data.get('number')
        ]

        if not documents:
            return False

        try:
            result = self.collection.insert_many(documents, ordered=False)
            print(f"  MongoDB: 成功插入 {len(result.inserted_ids)} 条记录")
            return True
        except Exception as e:
            error_msg = str(e).lower()
            if 'duplicate key error' in error_msg or 'e11000' in error_msg:
                upserted = 0
                for doc in documents:
                    phone = doc.get('phone')
                    if not phone:
                        continue
                    self.collection.update_one(
                        {'phone': phone},
                        {'$set': {
                            'price': doc.get('price', ''),
                            'source_url': doc.get('source_url', ''),
                            'source': doc.get('source', "numberbarn"),
                            'crawled_at': doc.get('crawled_at', current_time)
                        }},
                        upsert=True
                    )
                    upserted += 1
                print(f"  MongoDB: 插入/更新 {upserted} 条记录（处理重复键）")
                return True

            print(f"  MongoDB保存失败: {e}")
            return False

    async def extract_numbers_from_url(self, page, url: str, state: str, npa: str, max_numbers: int | None = None) -> List[Dict]:
        """从指定 URL 提取号码与价格，分页上限 10 页。"""
        all_numbers: List[Dict] = []
        page_number = 1
        max_pages = 10

        try:
            print(f"正在处理: {state} - {npa}\\n访问URL: {url}")
            await self._open_search_page(page, url)

            while page_number <= max_pages:
                current_numbers = await self._scrape_current_page(page, state, npa, page_number)
                all_numbers.extend(current_numbers)
                if max_numbers and len(all_numbers) >= max_numbers:
                    print(f"  达到 max_numbers={max_numbers}，提前结束该组合")
                    break
                if not current_numbers or not await self._goto_next_page(page, page_number):
                    break
                page_number += 1

            print(f"  组合 {state}-{npa} 总共提取到 {len(all_numbers)} 个号码（{page_number} 页）")
            return all_numbers

        except Exception as e:
            print(f"提取数据失败 {url}: {e}")
            return []

    async def _open_search_page(self, page, url: str) -> None:
        """首跳并做随机等待，降低反爬概率。"""
        await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        await page.wait_for_timeout(random.uniform(10_000, 30_000))

    async def _scrape_current_page(self, page, state: str, npa: str, page_number: int) -> List[Dict]:
        """抓取当前页、补全元数据并即时写入 Mongo。"""
        print(f"  正在提取第 {page_number} 页数据...")
        raw_numbers = await page.evaluate(JS_EXTRACT_SCRIPT) or []
        annotated = self._annotate_numbers(raw_numbers, state, npa, page_number, page.url)

        print(f"    第 {page_number} 页提取到 {len(annotated)} 个号码")
        self._log_samples(annotated)
        if annotated:
            self.save_numbers_to_mongodb(annotated)
        return annotated

    @staticmethod
    def _annotate_numbers(numbers: List[Dict], state: str, npa: str, page_number: int, url: str) -> List[Dict]:
        """补充州/区号/页码/来源 URL，返回新列表防止副作用。"""
        return [
            {
                "number": num.get("number", ""),
                "price": num.get("price", ""),
                "state": state,
                "npa": npa,
                "page": page_number,
                "source_url": url,
            }
            for num in numbers
        ]

    @staticmethod
    def _log_samples(numbers: List[Dict]) -> None:
        """打印前三条样例，便于人工观察。"""
        if not numbers:
            return
        print("    前3条记录:")
        for idx, num in enumerate(numbers[:3], 1):
            print(
                f"      {idx}. 号码: {num.get('number','')}, 价格: {num.get('price','')}, "
                f"州: {num.get('state','')}, 区号: {num.get('npa','')}"
            )

    async def _goto_next_page(self, page, page_number: int) -> bool:
        """尝试点击下一页，成功返回 True，未找到则 False。"""
        selectors = [
            'a:has-text(">")',
            'button:has-text(">")',
            '.pagination a:has-text(">")',
            '.pager a:has-text(">")',
            '[aria-label*="next"]',
            '[title*="next"]',
            '.next-page',
            '.pagination-next',
        ]

        for selector in selectors:
            try:
                btn = await page.query_selector(selector)
                if btn and await btn.is_visible() and not await btn.is_disabled():
                    print(f"    翻到第 {page_number + 1} 页...")
                    await btn.click()
                    await page.wait_for_timeout(2_000)
                    return True
            except Exception:
                continue

        print("    没有找到下一页按钮，当前组合提取完成")
        return False
        
    async def extract_single_url(self, url: str) -> List[Dict]:
        """从单个URL提取号码数据"""
        # 从URL中提取state和npa参数
        import re
        state_match = re.search(r'state=([^&]+)', url)
        npa_match = re.search(r'npa=([^&]+)', url)
        
        if not state_match or not npa_match:
            print("无法从URL中提取state和npa参数")
            return []
        
        state = state_match.group(1)
        npa = npa_match.group(1)
        
        all_numbers = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                numbers = await self.extract_numbers_from_url(page, url, state, npa)
                all_numbers.extend(numbers)
                        
            finally:
                await browser.close()
        
        return all_numbers
    async def extract_from_combinations(
        self,
        combinations: List[Dict],
        max_numbers: int | None = None,
        tracker: MongoProgressTracker | None = None,
        task_name: str | None = None,
        start_cursor: int = 0,
        prev_summary: Dict | None = None,
    ) -> List[Dict]:
        """从给定的state-npa组合列表提取号码数据，支持断点续跑"""
        all_numbers: List[Dict] = []
        processed = start_cursor
        prev_summary = prev_summary or {}
        success = int(prev_summary.get("success_combos", 0))
        failures = int(prev_summary.get("failed_combos", 0))
        total_numbers = int(prev_summary.get("numbers_captured_this_run", 0))

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                for i, combo in enumerate(combinations):
                    if i < start_cursor:
                        continue

                    state = combo['state']
                    npa = combo['npa']

                    print(f"\n处理进度: {i+1}/{len(combinations)} - {state} {npa}")

                    url = f"https://www.numberbarn.com/search?type=local&state={state}&npa={npa}&moreResults=true&sort=price%2B&limit=24"

                    try:
                        numbers = await self.extract_numbers_from_url(page, url, state, npa, max_numbers=max_numbers)

                        if numbers:
                            all_numbers.extend(numbers)
                            total_numbers += len(numbers)
                            print(f"  完成: 提取到 {len(numbers)} 个号码")
                        else:
                            print(f"  完成: 没有找到号码")
                        success += 1
                        processed += 1

                        if max_numbers and len(all_numbers) >= max_numbers:
                            print(f"已累计 {len(all_numbers)} 个号码，达到上限，停止后续组合")
                            if tracker and task_name:
                                tracker.complete(
                                    task_name,
                                    cursor=processed,
                                    summary={
                                        "success_combos": success,
                                        "failed_combos": failures,
                                        "numbers_captured_this_run": total_numbers,
                                        "stopped_early": True,
                                    },
                                )
                            return all_numbers

                    except Exception as e:
                        print(f"  处理时出错: {e}")
                        failures += 1
                        processed += 1

                    if tracker and task_name:
                        tracker.update(
                            task_name,
                            cursor=processed,
                            summary={
                                "success_combos": success,
                                "failed_combos": failures,
                                "numbers_captured_this_run": total_numbers,
                            },
                        )

                    await page.wait_for_timeout(2000)

                    if i % 5 == 4:
                        print(f"已处理 {i+1} 个组合，暂停3秒...")
                        await page.wait_for_timeout(3000)

            finally:
                await browser.close()

        if tracker and task_name:
            tracker.complete(
                task_name,
                cursor=processed,
                summary={
                    "success_combos": success,
                    "failed_combos": failures,
                    "numbers_captured_this_run": total_numbers,
                },
            )
        return all_numbers

    def run(self, max_numbers: int | None = None) -> List[Dict]:
        """主函数，对外提供run接口，可限定最大号码数，支持断点续跑"""
        combinations = self.load_combinations()

        if not combinations:
            print("未找到有效的组合数据")
            return []

        # 进度跟踪
        tracker = MongoProgressTracker(
            mongo_host=self.mongo_host,
            mongo_user="root",
            mongo_password=self.mongo_password,
            mongo_port=27017,
            mongo_db=self.mongo_db,
        )
        task_name = "numberbarn"
        json_mtime = os.path.getmtime(DEFAULT_JSON_FILE) if os.path.exists(DEFAULT_JSON_FILE) else None
        meta = {"combo_count": len(combinations), "json_file": DEFAULT_JSON_FILE, "json_mtime": json_mtime}
        prev = tracker.load(task_name)
        prev_summary = prev.get("summary", {}) if prev else {}

        if prev and prev.get("meta") == meta and prev.get("status") == "completed" and prev.get("cursor", 0) >= len(combinations):
            print(f"[RESUME] 已完成的 numberbarn 进度（{len(combinations)} 组合），直接返回。")
            return []

        if not prev or prev.get("meta") != meta:
            tracker.start(task_name, total_items=len(combinations), meta=meta)
            start_cursor = 0
            prev_summary = {}
            print(f"[RESUME] 新建进度记录，总计 {len(combinations)} 个组合。")
        else:
            start_cursor = int(prev.get("cursor", 0))
            print(f"[RESUME] 恢复进度，从第 {start_cursor + 1} 个组合继续（总计 {len(combinations)}）。")

        print(f"提取 {len(combinations)} 个 state-npa 组合")
        return asyncio.run(
            self.extract_from_combinations(
                combinations,
                max_numbers=max_numbers,
                tracker=tracker,
                task_name=task_name,
                start_cursor=start_cursor,
                prev_summary=prev_summary,
            )
        )


def main():
    """主函数"""
    extractor = NumberbarnNumberExtractor()

    # 执行数据提取
    numbers = extractor.run()

    print(f"总提取到 {len(numbers)} 个号码")


if __name__ == "__main__":
    main()
