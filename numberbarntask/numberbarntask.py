import asyncio
import json
import re
import os
import time
import random
from typing import List, Dict
from playwright.async_api import async_playwright
from pymongo import MongoClient
from datetime import datetime

DEFAULT_JSON_FILE = "/tmp/numberbarn_state_npa_cache.json"  # 本地文件存储路径
API_URL = "https://www.numberbarn.com/api/npas?$limit=1000"  # 获取 combinations 的 API 接口

class NumberbarnNumberExtractor:
    """专门用于从numberbarn.com提取号码和价格的简化爬虫"""

    def __init__(self, mongo_host: str = "43.159.58.235",
                 mongo_password: str = "RsBWd3hTAZeR7kC4",
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
            connection_string = f"mongodb://extra_numbers:{self.mongo_password}@{self.mongo_host}:27017/{self.mongo_db}?authSource=extra_numbers"
            self.mongo_client = MongoClient(connection_string)
            self.db = self.mongo_client[self.mongo_db]
            self.collection = self.db['numbers']

            # 测试连接
            self.mongo_client.admin.command('ping')
            print(f"成功连接到MongoDB数据库: {self.mongo_db}")

        except Exception as e:
            print(f"MongoDB连接失败: {e}")
            self.mongo_client = None
            
    def get_combinations_from_api(self) -> List[Dict]:
        """从接口获取所有state和npa的组合"""
        try:
            print(f"从API获取state和npa组合: {API_URL}")
            response = requests.get(API_URL)
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

            print(f"从API获取到 {len(combinations)} 条有效组合")
            return combinations

        except Exception as e:
            print(f"从API获取state-npa组合失败: {e}")
            return []

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
        import requests
        API_URL = "https://www.numberbarn.com/api/npas?$limit=1000"
        try:
            print(f"从API获取state和npa组合: {API_URL}")
            response = requests.get(API_URL)
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

            print(f"从API获取到 {len(combinations)} 条有效组合")
            return combinations

        except Exception as e:
            print(f"从API获取state-npa组合失败: {e}")
            return []

    def save_numbers_to_mongodb(self, numbers: List[Dict]) -> bool:
        """将号码列表保存到MongoDB，每个号码一条记录"""
        if not self.mongo_client or not numbers:
            return False

        try:
            documents = []
            current_time = datetime.utcnow()

            for number_data in numbers:
                doc = {
                    'phone': number_data.get('number', ''),
                    'price': number_data.get('price', ''),
                    'source_url': number_data.get('source_url', ''),
                    'source':"numberbarn",
                    'crawled_at': current_time
                }
                documents.append(doc)

            # 批量插入，忽略重复记录
            if documents:
                try:
                    result = self.collection.insert_many(documents, ordered=False)
                    print(f"  MongoDB: 成功插入 {len(result.inserted_ids)} 条记录")
                    return True
                except Exception as e:
                    # 处理重复键错误
                    if 'duplicate key error' in str(e).lower() or 'E11000' in str(e):
                        inserted_count = 0
                        for doc in documents:
                            try:
                                self.collection.insert_one(doc)
                                inserted_count += 1
                            except Exception:
                                try:
                                    self.collection.update_one(
                                        {'number': doc['number']},
                                        {'$set': {'updated_at': current_time}}
                                    )
                                except Exception:
                                    pass
                        print(f"插入 {inserted_count} 条新记录，跳过重复记录")
                        return True
                    else:
                        raise e

        except Exception as e:
            print(f"  MongoDB保存失败: {e}")
            return False

        return False

    async def extract_numbers_from_url(self, page, url: str, state: str, npa: str) -> List[Dict]:
        """从指定URL提取号码和价格数据，支持翻页"""
        all_numbers = []
        page_number = 1
        max_pages = 10  # 最大翻页数，防止无限循环
        
        try:
            print(f"正在处理: {state} - {npa}")
            print(f"访问URL: {url}")
            
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(random.uniform(10000, 30000))
            
            while page_number <= max_pages:
                print(f"  正在提取第 {page_number} 页数据...")
                
                # 提取当前页面的号码数据
                page_numbers = await page.evaluate("""
                    () => {
                        const numbers = [];
                        
                        // 查找包含电话号码和价格的元素
                        const numberElements = document.querySelectorAll('.number-item, .phone-number, .listing-item, [data-phone], .search-result, .result-item');
                        
                        if (numberElements.length > 0) {
                            numberElements.forEach(el => {
                                const text = el.textContent || '';
                                
                                // 提取电话号码 - 支持多种格式
                                const phonePattern = /(\\(\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4})/g;
                                const phoneMatches = text.match(phonePattern);
                                
                                if (phoneMatches) {
                                    // 提取价格
                                    const pricePattern = /\\$[\\d,]+\\.?\\d*/g;
                                    const priceMatch = text.match(pricePattern);
                                    
                                    numbers.push({
                                        number: phoneMatches[0].trim(),
                                        price: priceMatch ? priceMatch[0] : ''
                                    });
                                }
                            });
                        }
                        
                        // 如果没有找到专门的号码元素，进行全局搜索
                        if (numbers.length === 0) {
                            const bodyText = document.body.textContent || '';
                            const phonePattern = /(\\(\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4})/g;
                            const phoneMatches = bodyText.match(phonePattern);
                            
                            if (phoneMatches) {
                                // 去重
                                const uniquePhones = [...new Set(phoneMatches)];
                                
                                uniquePhones.forEach(phone => {
                                    // 尝试在附近找到价格
                                    const pricePattern = /\\$[\\d,]+\\.?\\d*/g;
                                    const priceMatch = bodyText.match(pricePattern);
                                    
                                    numbers.push({
                                        number: phone.trim(),
                                        price: priceMatch ? priceMatch[0] : ''
                                    });
                                });
                            }
                        }
                        
                        return numbers;
                    }
                """)
                
                # 添加state和npa信息
                current_page_numbers = []
                for number in page_numbers:
                    number.update({
                        'state': state,
                        'npa': npa,
                        'page': page_number,
                        'source_url': page.url
                    })
                    current_page_numbers.append(number)
                    all_numbers.append(number)
                
                print(f"    第 {page_number} 页提取到 {len(current_page_numbers)} 个号码")
                
                # 打印当前页的前3条记录（如果是第一页）
                if page_number == 1 and current_page_numbers:
                    print("    前3条记录:")
                    for i, number in enumerate(current_page_numbers[:3]):
                        print(f"      {i+1}. 号码: {number.get('number', '')}, 价格: {number.get('price', '')}, 州: {number.get('state', '')}, 区号: {number.get('npa', '')}")
                
                # 立即保存当前页数据到MongoDB
                if current_page_numbers:
                    self.save_numbers_to_mongodb(current_page_numbers)
                
                # 检查是否有下一页（查找 '>' 翻页按钮）
                try:
                    # 查找翻页按钮的多种可能选择器
                    next_button_selectors = [
                        'a:has-text(">")',
                        'button:has-text(">")',
                        '.pagination a:has-text(">")',
                        '.pager a:has-text(">")',
                        '[aria-label*="next"]',
                        '[title*="next"]',
                        '.next-page',
                        '.pagination-next'
                    ]
                    
                    next_button = None
                    for selector in next_button_selectors:
                        try:
                            next_button = await page.query_selector(selector)
                            if next_button:
                                # 检查按钮是否可以点击（不是禁用状态）
                                is_disabled = await next_button.is_disabled()
                                is_visible = await next_button.is_visible()
                                if not is_disabled and is_visible:
                                    break
                                else:
                                    next_button = None
                        except:
                            continue
                    
                    if next_button:
                        print(f"    找到下一页按钮，正在翻到第 {page_number + 1} 页...")
                        await next_button.click()
                        await page.wait_for_timeout(2000)  # 等待页面加载
                        page_number += 1
                    else:
                        print(f"    没有找到下一页按钮，当前组合提取完成")
                        break
                        
                except Exception as e:
                    print(f"    翻页时出错: {e}")
                    break
                    
            print(f"  组合 {state}-{npa} 总共提取到 {len(all_numbers)} 个号码（{page_number} 页）")
            
            return all_numbers
            
        except Exception as e:
            print(f"提取数据失败 {url}: {e}")
            return []
        
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
    async def extract_from_combinations(self, combinations: List[Dict]) -> List[Dict]:
        """从给定的state-npa组合列表提取号码数据"""
        all_numbers = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                for i, combo in enumerate(combinations):
                    state = combo['state']
                    npa = combo['npa']

                    print(f"\n处理进度: {i+1}/{len(combinations)} - {state} {npa}")

                    url = f"https://www.numberbarn.com/search?type=local&state={state}&npa={npa}&moreResults=true&sort=price%2B&limit=24"

                    try:
                        numbers = await self.extract_numbers_from_url(page, url, state, npa)

                        if numbers:
                            all_numbers.extend(numbers)
                            print(f"  完成: 提取到 {len(numbers)} 个号码")
                        else:
                            print(f"  完成: 没有找到号码")

                    except Exception as e:
                        print(f"  处理时出错: {e}")

                    await page.wait_for_timeout(2000)

                    if i % 5 == 4:
                        print(f"已处理 {i+1} 个组合，暂停3秒...")
                        await page.wait_for_timeout(3000)

            finally:
                await browser.close()

        return all_numbers

    def run(self) -> List[Dict]:
        """主函数，对外提供run接口"""
        combinations = self.get_combinations_from_file()

        if not combinations:
            combinations = self.get_combinations_from_api()

        if combinations:
            print(f"提取 {len(combinations)} 个 state-npa 组合")
            return asyncio.run(self.extract_from_combinations(combinations))
        else:
            print("未找到有效的组合数据")
            return []


def main():
    """主函数"""
    extractor = NumberbarnNumberExtractor()

    # 执行数据提取
    numbers = extractor.run()

    print(f"总提取到 {len(numbers)} 个号码")


if __name__ == "__main__":
    main()
