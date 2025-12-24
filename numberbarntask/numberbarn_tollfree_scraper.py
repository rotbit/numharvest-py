import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from playwright.async_api import async_playwright
from pymongo import MongoClient

TOLL_FREE_NPAS = ["800", "888", "877", "866", "855", "844", "833"]
DEFAULT_LIMIT = 24

JS_EXTRACT_SCRIPT = """
() => {
    const numbers = [];

    // 首选 numberbarn 新版组件 search-tn 结构
    document.querySelectorAll('search-tn .tn-number').forEach(numEl => {
        const phone = (numEl.textContent || '').trim();
        if (!phone) return;
        const priceEl = numEl.closest('search-tn')?.querySelector('.tn-price');
        const price = priceEl ? (priceEl.textContent || '').trim() : '';
        numbers.push({ number: phone, price });
    });

    // 兼容旧选择器
    const fallbackNodes = document.querySelectorAll(
        '.number-item, .phone-number, .listing-item, [data-phone], .search-result, .result-item'
    );
    if (fallbackNodes.length) {
        fallbackNodes.forEach(el => {
            const text = el.textContent || '';
            const phonePattern = /(\\(\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4})/g;
            const phoneMatches = text.match(phonePattern);
            if (phoneMatches) {
                const pricePattern = /\\$[\\d,]+\\.?\\d*/g;
                const priceMatch = text.match(pricePattern);
                numbers.push({
                    number: phoneMatches[0].trim(),
                    price: priceMatch ? priceMatch[0] : ''
                });
            }
        });
    }

    return numbers;
}
"""


class NumberbarnTollFreeExtractor:
    """专门用于从 numberbarn.com 提取 toll free 号码和价格的爬虫"""

    def __init__(
        self,
        mongo_host: str = "43.159.58.235",
        mongo_user: str = "root",
        mongo_password: str = "pp963470667",
        mongo_port: int = 27017,
        mongo_auth_source: str = "admin",
        mongo_db: str = "extra_numbers",
        mongo_collection: str = "numbers",
        use_mongodb: bool = True,
        max_pages: Optional[int] = None,
    ):
        self.mongo_host = mongo_host
        self.mongo_user = mongo_user
        self.mongo_password = mongo_password
        self.mongo_port = mongo_port
        self.mongo_auth_source = mongo_auth_source
        self.mongo_db = mongo_db
        self.mongo_collection_name = mongo_collection
        self.use_mongodb = use_mongodb
        self.max_pages = max_pages

        self.mongo_client = None
        self.db = None
        self.collection = None
        self.html_collection = None
        self.error_collection = None

        if self.use_mongodb:
            self.init_mongodb()

    def init_mongodb(self) -> None:
        """初始化MongoDB连接"""
        try:
            connection_string = (
                f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/{self.mongo_db}?authSource={self.mongo_auth_source}"
            )
            self.mongo_client = MongoClient(connection_string)
            self.db = self.mongo_client[self.mongo_db]
            self.collection = self.db[self.mongo_collection_name]
            self.html_collection = self.db["page_html"]
            self.error_collection = self.db["error_page_collect"]

            # 测试连接
            self.mongo_client.admin.command("ping")
            print(f"成功连接到MongoDB数据库: {self.mongo_db}")

            # 创建索引提高查询效率
            self.collection.create_index("phone", unique=True)
            self.collection.create_index("npa")
            self.html_collection.create_index([("source", 1), ("url", 1)], unique=True)
            self.html_collection.create_index("fetched_at")
            self.error_collection.create_index([("source", 1), ("url", 1)], unique=True)
            self.error_collection.create_index("created_at")

        except Exception as exc:
            print(f"MongoDB连接失败: {exc}")
            self.mongo_client = None

    def _save_html_snapshot(self, url: str, html: str, meta: Optional[Dict[str, str]] = None) -> None:
        """将原始页面 HTML 保存到 MongoDB（source=url 唯一）。"""
        if self.html_collection is None or not url or not html:
            return

        now = datetime.utcnow()
        try:
            self.html_collection.update_one(
                {"source": "numberbarn", "url": url},
                {
                    "$set": {
                        "html": html,
                        "meta": meta or {},
                        "fetched_at": now,
                        "updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        except Exception as exc:
            print(f"  [WARN] 保存 HTML 失败 {url}: {exc}")

    def _save_error_page(self, url: str, html: str, meta: Optional[Dict[str, str]] = None) -> None:
        """保存解析失败页面 HTML 到 error_page_collect。"""
        if self.error_collection is None or not url or not html:
            return

        now = datetime.utcnow()
        try:
            self.error_collection.update_one(
                {"source": "numberbarn", "url": url},
                {
                    "$set": {
                        "html": html,
                        "meta": meta or {},
                        "updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        except Exception as exc:
            print(f"  [WARN] 保存 error_page_collect 失败 {url}: {exc}")

    def save_numbers_to_mongodb(self, numbers: List[Dict]) -> bool:
        """将号码列表保存到MongoDB，每个号码一条记录"""
        if not self.mongo_client or not numbers:
            return False

        try:
            current_time = datetime.utcnow()
            inserted_count = 0
            updated_count = 0
            skipped_count = 0

            for number_data in numbers:
                phone = number_data.get("number") or number_data.get("phone", "")
                new_price = number_data.get("price", "")

                if not phone:
                    continue

                existing = self.collection.find_one({"phone": phone})

                if existing is None:
                    doc = {
                        "phone": phone,
                        "price": new_price,
                        "npa": number_data.get("npa", ""),
                        "page": number_data.get("page", 1),
                        "source_url": number_data.get("source_url", ""),
                        "source": "numberbarn",
                        "type": "tollfree",
                        "crawled_at": current_time,
                    }
                    try:
                        self.collection.insert_one(doc)
                        inserted_count += 1
                    except Exception as exc:
                        print(f"    插入记录失败 {phone}: {exc}")

                elif existing.get("price") != new_price:
                    try:
                        self.collection.update_one(
                            {"phone": phone},
                            {
                                "$set": {
                                    "price": new_price,
                                    "npa": number_data.get("npa", ""),
                                    "page": number_data.get("page", 1),
                                    "source_url": number_data.get("source_url", ""),
                                    "source": "numberbarn",
                                    "type": "tollfree",
                                    "crawled_at": current_time,
                                }
                            },
                        )
                        updated_count += 1
                    except Exception as exc:
                        print(f"    更新记录失败 {phone}: {exc}")
                else:
                    skipped_count += 1

            print(
                f"  MongoDB: 插入 {inserted_count} 条新记录，更新 {updated_count} 条记录，"
                f"跳过 {skipped_count} 条相同记录"
            )
            return True

        except Exception as exc:
            print(f"  MongoDB保存失败: {exc}")
            return False

    def close_mongodb(self) -> None:
        """关闭MongoDB连接"""
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB连接已关闭")

    def __del__(self) -> None:
        self.close_mongodb()

    async def extract_numbers_from_url(self, page, url: str, npa: str) -> List[Dict]:
        """从指定URL提取号码和价格数据，支持翻页"""
        all_numbers = []
        page_number = 1

        try:
            print(f"正在处理 toll free NPA: {npa}")
            print(f"访问URL: {url}")

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)

            while True:
                print(f"  正在提取第 {page_number} 页数据...")

                html = await page.content()
                self._save_html_snapshot(page.url, html, meta={"npa": npa, "page": page_number})

                page_numbers = await page.evaluate(JS_EXTRACT_SCRIPT)

                current_page_numbers = []
                for number in page_numbers:
                    number.update(
                        {
                            "npa": npa,
                            "page": page_number,
                            "source_url": page.url,
                        }
                    )
                    current_page_numbers.append(number)
                    all_numbers.append(number)

                print(f"    第 {page_number} 页提取到 {len(current_page_numbers)} 个号码")

                if not current_page_numbers:
                    self._save_error_page(
                        page.url,
                        html,
                        meta={
                            "npa": npa,
                            "page": page_number,
                            "reason": "no_numbers_extracted",
                        },
                    )

                if current_page_numbers:
                    print("    前3条记录:")
                    for i, number in enumerate(current_page_numbers[:3]):
                        print(
                            f"      {i+1}. 号码: {number.get('number', '')}, 价格: {number.get('price', '')}, "
                            f"区号: {number.get('npa', '')}"
                        )

                if current_page_numbers:
                    self.save_numbers_to_mongodb(current_page_numbers)

                if self.max_pages is not None and page_number >= self.max_pages:
                    print(f"    达到最大页数 {self.max_pages}，停止翻页")
                    break

                try:
                    next_button_selectors = [
                        'a:has-text(">")',
                        'button:has-text(">")',
                        '.pagination a:has-text(">")',
                        '.pager a:has-text(">")',
                        '[aria-label*="next"]',
                        '[title*="next"]',
                        '.next-page',
                        '.pagination-next',
                    ]

                    next_button = None
                    for selector in next_button_selectors:
                        try:
                            next_button = await page.query_selector(selector)
                            if next_button:
                                is_disabled = await next_button.is_disabled()
                                is_visible = await next_button.is_visible()
                                if not is_disabled and is_visible:
                                    break
                                next_button = None
                        except Exception:
                            continue

                    if next_button:
                        print(f"    找到下一页按钮，正在翻到第 {page_number + 1} 页...")
                        await next_button.click()
                        await page.wait_for_timeout(2000)
                        page_number += 1
                    else:
                        print("    没有找到下一页按钮，当前组合提取完成")
                        break

                except Exception as exc:
                    print(f"    翻页时出错: {exc}")
                    break

            print(f"  NPA {npa} 总共提取到 {len(all_numbers)} 个号码（{page_number} 页）")
            return all_numbers

        except Exception as exc:
            print(f"提取数据失败 {url}: {exc}")
            return []

    async def extract_from_npas(self, npas: List[str], limit: int = DEFAULT_LIMIT) -> List[Dict]:
        """从给定的 toll free NPA 列表提取号码数据"""
        all_numbers: List[Dict] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                for i, npa in enumerate(npas):
                    npa = str(npa)
                    print(f"\n处理进度: {i+1}/{len(npas)} - NPA {npa}")

                    url = (
                        "https://www.numberbarn.com/search?type=tollfree"
                        f"&npa={npa}&moreResults=true&sort=price%2B&limit={limit}"
                    )

                    try:
                        numbers = await self.extract_numbers_from_url(page, url, npa)

                        if numbers:
                            all_numbers.extend(numbers)
                            print(f"  完成: 提取到 {len(numbers)} 个号码")
                        else:
                            print("  完成: 没有找到号码")

                    except Exception as exc:
                        print(f"  处理时出错: {exc}")

                    await page.wait_for_timeout(2000)

                    if i % 5 == 4:
                        print(f"已处理 {i+1} 个 NPA，暂停3秒...")
                        await page.wait_for_timeout(3000)

            finally:
                await browser.close()

        return all_numbers

    async def extract_single_npa(self, npa: str, limit: int = DEFAULT_LIMIT) -> List[Dict]:
        """从单个 NPA 提取号码数据"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                url = (
                    "https://www.numberbarn.com/search?type=tollfree"
                    f"&npa={npa}&moreResults=true&sort=price%2B&limit={limit}"
                )
                return await self.extract_numbers_from_url(page, url, npa)
            finally:
                await browser.close()

    def run(self, npas: Optional[List[str]] = None, limit: int = DEFAULT_LIMIT) -> List[Dict]:
        """主函数，对外提供 run 接口。"""
        return asyncio.run(self.extract_from_npas(npas or TOLL_FREE_NPAS, limit=limit))


def extract_from_single_npa(
    npa: str,
    limit: int = DEFAULT_LIMIT,
    max_pages: Optional[int] = None,
    use_mongodb: bool = False,
) -> List[Dict]:
    """从单个 NPA 提取号码的便捷函数"""

    async def _extract():
        extractor = NumberbarnTollFreeExtractor(use_mongodb=use_mongodb, max_pages=max_pages)
        try:
            return await extractor.extract_single_npa(npa, limit=limit)
        finally:
            extractor.close_mongodb()

    return asyncio.run(_extract())


def extract_from_all_tollfree(
    npas: Optional[List[str]] = None,
    limit: int = DEFAULT_LIMIT,
    max_pages: Optional[int] = None,
    use_mongodb: bool = True,
) -> List[Dict]:
    """从所有 toll free NPAs 提取号码的便捷函数"""

    async def _extract():
        extractor = NumberbarnTollFreeExtractor(use_mongodb=use_mongodb, max_pages=max_pages)
        try:
            return await extractor.extract_from_npas(npas or TOLL_FREE_NPAS, limit=limit)
        finally:
            extractor.close_mongodb()

    return asyncio.run(_extract())


async def main():
    """主函数 - 执行 toll free 号码提取"""
    extractor = NumberbarnTollFreeExtractor()

    try:
        print("开始执行 toll free 号码提取任务...")
        all_numbers = await extractor.extract_from_npas(TOLL_FREE_NPAS)

        print("\n=== 提取任务完成 ===")
        print(f"总提取号码数: {len(all_numbers)}")

        if all_numbers:
            print("\n前3条记录:")
            for i, number in enumerate(all_numbers[:3]):
                print(
                    f"  {i+1}. 号码: {number.get('number', '')}, 价格: {number.get('price', '')}, "
                    f"区号: {number.get('npa', '')}"
                )

        return all_numbers

    except Exception as exc:
        print(f"提取任务失败: {exc}")
        return None
    finally:
        extractor.close_mongodb()


if __name__ == "__main__":
    asyncio.run(main())
