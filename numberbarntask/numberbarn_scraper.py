import asyncio
import json
import re
from typing import Dict, List, Optional
from playwright.async_api import async_playwright
from pymongo import MongoClient
from datetime import datetime



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
        self.html_collection = None
        self.error_collection = None
        
        # 初始化MongoDB连接
        self.init_mongodb()
    
    def init_mongodb(self):
        """初始化MongoDB连接"""
        try:
            connection_string = f"mongodb://extra_numbers:{self.mongo_password}@{self.mongo_host}:27017/{self.mongo_db}?authSource=extra_numbers"
            
            self.mongo_client = MongoClient(connection_string)
            self.db = self.mongo_client[self.mongo_db]
            self.collection = self.db['numberbarn_numbers']
            self.html_collection = self.db['page_html']
            self.error_collection = self.db['error_page_collect']
            
            # 测试连接
            self.mongo_client.admin.command('ping')
            print(f"成功连接到MongoDB数据库: {self.mongo_db}")
            
            # 创建索引提高查询效率
            self.collection.create_index("number", unique=True)
            self.collection.create_index([("state", 1), ("npa", 1)])
            self.html_collection.create_index([("source", 1), ("url", 1)], unique=True)
            self.html_collection.create_index("fetched_at")
            self.error_collection.create_index([("source", 1), ("url", 1)], unique=True)
            self.error_collection.create_index("created_at")
            
        except Exception as e:
            print(f"MongoDB连接失败: {e}")
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

    def get_all_state_npa_combinations(self, json_file: str = "numberbarn_state_npa_cache.json") -> List[Dict]:
        """从JSON文件获取所有state和npa的组合"""
        combinations = []
        
        try:
            print(f"从JSON文件读取state和npa组合: {json_file}")
            
            # 检查文件是否存在
            import os
            if not os.path.exists(json_file):
                print(f"错误：JSON文件不存在: {json_file}")
                return []
            
            # 读取JSON文件
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"JSON文件数据结构: {type(data)}")
            
            # 解析JSON数据结构
            if isinstance(data, dict):
                # 如果是包含combinations字段的结构
                if 'combinations' in data:
                    combinations = data['combinations']
                    print(f"从combinations字段获取到 {len(combinations)} 条记录")
                else:
                    # 如果是直接按州分组的结构
                    print("处理按州分组格式")
                    for state, npas in data.items():
                        if state == 'data':  # 跳过data字段
                            continue
                        if isinstance(npas, list):
                            for npa in npas:
                                combinations.append({
                                    'state': str(state).upper(),
                                    'npa': str(npa)
                                })
                        else:
                            combinations.append({
                                'state': str(state).upper(),
                                'npa': str(npas)
                            })
            
            elif isinstance(data, list):
                # 如果是直接的组合列表
                combinations = data
                print(f"从列表格式获取到 {len(combinations)} 条记录")
            
            # 验证和清理数据
            valid_combinations = []
            for item in combinations:
                if isinstance(item, dict):
                    state = item.get('state', item.get('stateCode', ''))
                    npa = item.get('npa', item.get('areaCode', ''))
                    
                    # 确保state和npa都存在且有效
                    if state and npa and len(str(npa)) == 3:
                        valid_combinations.append({
                            'state': str(state).upper(),
                            'npa': str(npa)
                        })
                elif isinstance(item, list) and len(item) == 2:
                    # 处理[state, npa]格式
                    state, npa = item
                    if state and npa and len(str(npa)) == 3:
                        valid_combinations.append({
                            'state': str(state).upper(),
                            'npa': str(npa)
                        })
            
            print(f"验证后得到 {len(valid_combinations)} 个有效的state-npa组合")
            
            # 显示前几个组合样本
            if valid_combinations:
                print("前几个组合样本:")
                for i, combo in enumerate(valid_combinations[:5]):
                    print(f"  {i+1}. {combo['state']} - {combo['npa']}")
            
            return valid_combinations
            
        except Exception as e:
            print(f"从JSON文件读取state-npa组合失败: {e}")
            return []
    
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
                number = number_data.get('number', '')
                new_price = number_data.get('price', '')
                
                if not number:
                    continue
                
                # 查询现有记录
                existing = self.collection.find_one({'number': number})
                
                if existing is None:
                    # 新记录，直接插入
                    doc = {
                        'number': number,
                        'price': new_price,
                        'state': number_data.get('state', ''),
                        'npa': number_data.get('npa', ''),
                        'page': number_data.get('page', 1),
                        'source_url': number_data.get('source_url', ''),
                        'created_at': current_time,
                        'updated_at': current_time
                    }
                    try:
                        self.collection.insert_one(doc)
                        inserted_count += 1
                    except Exception as e:
                        print(f"    插入记录失败 {number}: {e}")
                        
                elif existing.get('price') != new_price:
                    # 价格不同，更新记录
                    try:
                        self.collection.update_one(
                            {'number': number},
                            {'$set': {
                                'price': new_price,
                                'state': number_data.get('state', ''),
                                'npa': number_data.get('npa', ''),
                                'page': number_data.get('page', 1),
                                'source_url': number_data.get('source_url', ''),
                                'updated_at': current_time
                            }}
                        )
                        updated_count += 1
                    except Exception as e:
                        print(f"    更新记录失败 {number}: {e}")
                else:
                    # 价格相同，跳过
                    skipped_count += 1
            
            print(f"  MongoDB: 插入 {inserted_count} 条新记录，更新 {updated_count} 条记录，跳过 {skipped_count} 条相同记录")
            return True
                        
        except Exception as e:
            print(f"  MongoDB保存失败: {e}")
            return False
    
    def close_mongodb(self):
        """关闭MongoDB连接"""
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB连接已关闭")
    
    def __del__(self):
        """析构函数，确保关闭MongoDB连接"""
        self.close_mongodb()

    async def extract_numbers_from_url(self, page, url: str, state: str, npa: str) -> List[Dict]:
        """从指定URL提取号码和价格数据，支持翻页"""
        all_numbers = []
        page_number = 1

        try:
            print(f"正在处理: {state} - {npa}")
            print(f"访问URL: {url}")
            
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)

            while True:
                print(f"  正在提取第 {page_number} 页数据...")
                
                # 提取当前页面的号码数据
                # 先抓 HTML 供库存档
                html = await page.content()
                self._save_html_snapshot(page.url, html, meta={"state": state, "npa": npa, "page": page_number})

                page_numbers = await page.evaluate("""
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
                        const fallbackNodes = document.querySelectorAll('.number-item, .phone-number, .listing-item, [data-phone], .search-result, .result-item');
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

                if not current_page_numbers:
                    self._save_error_page(
                        page.url,
                        html,
                        meta={
                            "state": state,
                            "npa": npa,
                            "page": page_number,
                            "reason": "no_numbers_extracted",
                        },
                    )
                
                # 打印当前页的前3条记录
                if current_page_numbers:
                    print("    前3条记录:")
                    for i, number in enumerate(current_page_numbers[:3]):
                        print(
                            f"      {i+1}. 号码: {number.get('number', '')}, 价格: {number.get('price', '')}, "
                            f"州: {number.get('state', '')}, 区号: {number.get('npa', '')}"
                        )
                
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
                    
                    # 构建URL，包含更多结果和限制参数
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
                    
                    # 添加延迟避免被限制
                    await page.wait_for_timeout(2000)
                    
                    # 每处理几个组合就暂停一下
                    if i % 5 == 4:
                        print(f"已处理 {i+1} 个组合，暂停3秒...")
                        await page.wait_for_timeout(3000)
                        
            finally:
                await browser.close()
        
        return all_numbers
    
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

async def main():
    """主函数 - 生产环境执行号码提取"""
    extractor = NumberbarnNumberExtractor()
    
    try:
        print("开始执行号码提取任务...")
        
        # 获取所有可用的state-npa组合
        combinations = extractor.get_all_state_npa_combinations()
        
        if not combinations:
            print("错误：无法获取state-npa组合数据")
            return None
        
        print(f"获取到 {len(combinations)} 个state-npa组合")
        
        # 执行批量号码提取
        all_numbers = await extractor.extract_from_combinations(combinations)
        
        print(f"\n=== 提取任务完成 ===")
        print(f"总提取号码数: {len(all_numbers)}")
        
        # 打印前3条记录
        if all_numbers:
            print(f"\n前3条记录:")
            for i, number in enumerate(all_numbers[:3]):
                print(f"  {i+1}. 号码: {number.get('number', '')}, 价格: {number.get('price', '')}, 州: {number.get('state', '')}, 区号: {number.get('npa', '')}")
        
        # 统计各州的号码数量
        state_stats = {}
        for number in all_numbers:
            state = number.get('state', 'Unknown')
            if state not in state_stats:
                state_stats[state] = 0
            state_stats[state] += 1
        
        print(f"覆盖州数: {len(state_stats)}")
        
        # 按号码数量排序显示前10个州
        sorted_states = sorted(state_stats.items(), key=lambda x: x[1], reverse=True)
        print(f"\n前10个州的号码数量:")
        for i, (state, count) in enumerate(sorted_states[:10]):
            print(f"{i+1}. {state}: {count} 个号码")
        
        if len(sorted_states) > 10:
            print(f"... 还有 {len(sorted_states) - 10} 个州")
        
        return all_numbers
        
    except Exception as e:
        print(f"提取任务失败: {e}")
        return None
    finally:
        # 确保关闭MongoDB连接
        extractor.close_mongodb()


def extract_from_single_url(url: str) -> List[Dict]:
    """从单个URL提取号码的便捷函数"""
    """用法示例：
        from numberbarn_scraper import extract_from_single_url
        numbers = asyncio.run(extract_from_single_url(
            "https://www.numberbarn.com/search?type=local&state=NJ&npa=201&moreResults=true&sort=price%2B&limit=24"
        ))
        print(f"提取到 {len(numbers)} 个号码")
    """
    async def _extract():
        extractor = NumberbarnNumberExtractor()
        try:
            return await extractor.extract_single_url(url)
        finally:
            extractor.close_mongodb()
    
    return asyncio.run(_extract())


def extract_from_all_combinations(json_file: str = "numberbarn_state_npa_cache.json") -> List[Dict]:
    """从所有state-npa组合提取号码的便捷函数"""
    """用法示例：
        from numberbarn_scraper import extract_from_all_combinations
        numbers = asyncio.run(extract_from_all_combinations())
        print(f"提取到 {len(numbers)} 个号码")
        
        # 或者指定特定的JSON文件
        numbers = asyncio.run(extract_from_all_combinations("custom_combinations.json"))
    """
    async def _extract():
        extractor = NumberbarnNumberExtractor()
        try:
            combinations = extractor.get_all_state_npa_combinations(json_file)
            return await extractor.extract_from_combinations(combinations)
        finally:
            extractor.close_mongodb()
    
    return asyncio.run(_extract())

if __name__ == "__main__":
    asyncio.run(main())
