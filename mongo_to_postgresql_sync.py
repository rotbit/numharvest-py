import asyncio
import logging
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import DatabaseError
from psycopg2.extras import execute_values
from pymongo import MongoClient

from state_codes import normalize_state, state_name


class MongoToPostgreSQLSync:
    """MongoDB到PostgreSQL数据同步器"""
    
    def __init__(self, 
                 mongo_host: str = "43.159.58.235",
                 mongo_user: str = "root",
                 mongo_password: str = "pp963470667",
                 mongo_port: int = 27017,
                 mongo_db: str = "extra_numbers",
                 
                 postgres_host: str = "43.159.58.235",
                 postgres_port: int = 4088,
                 postgres_db: str = "numharvest",
                 postgres_user: str = "numharvest",
                 postgres_password: str = "pP963470667",
                 
                 batch_size: int = 1000,
                 dry_run: bool = False):
        
        # MongoDB配置
        self.mongo_host = mongo_host
        self.mongo_user = mongo_user
        self.mongo_password = mongo_password
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        
        # PostgreSQL配置
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_db = postgres_db
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        
        # 同步配置
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        # 初始化连接
        self.mongo_client = None
        self.postgres_conn = None
        
        # 设置日志
        self.setup_logging()
    
    def setup_logging(self):
        """设置日志配置"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('mongo_postgresql_sync.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def price_str_to_int(self, price_str: str) -> Optional[float]:
        """
        将价格字符串转换为数字（保留两位小数）
        支持格式: $1,234, $99.99, $1234, $1,234.56 等
        """
        if not price_str:
            return None
            
        try:
            # 移除货币符号与非数字/小数点字符，避免把“$9.9/mo”解析成 9.9 以外的值
            clean_price = re.sub(r"[^\d.,]", "", price_str)
            if not clean_price:
                return None

            # 仅保留第一个小数点，去掉千分位逗号
            if clean_price.count(".") > 1:
                first, *rest = clean_price.split(".")
                clean_price = first + "." + "".join(rest)
            clean_price = clean_price.replace(",", "")

            # 解析为浮点，保留两位小数
            value = float(clean_price)
            return round(value, 2)
            
        except (ValueError, AttributeError):
            self.logger.warning(f"无法解析价格字符串: {price_str}")
            return None
    
    def connect_mongodb(self) -> bool:
        """连接MongoDB"""
        try:
            connection_string = (
                f"mongodb://{self.mongo_user}:{self.mongo_password}"
                f"@{self.mongo_host}:{self.mongo_port}/?authSource=admin"
            )
            self.mongo_client = MongoClient(connection_string)
            
            # 测试连接
            self.mongo_client.admin.command('ping')
            self.logger.info(f"成功连接到MongoDB: {self.mongo_host}:{self.mongo_port}")
            return True
            
        except Exception as e:
            self.logger.error(f"MongoDB连接失败: {e}")
            return False
    
    def connect_postgresql(self) -> bool:
        """连接PostgreSQL"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                dbname=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password,
            )
            with self.postgres_conn.cursor() as cur:
                cur.execute("SELECT 1")
            self.logger.info("成功连接到 PostgreSQL: %s:%s", self.postgres_host, self.postgres_port)
            return True
        except DatabaseError as e:
            self.logger.error("PostgreSQL连接失败: %s", e)
            return False
    
    def get_mongodb_collections(self) -> List[str]:
        """获取MongoDB中的所有集合名称"""
        collections = []
        try:
            db = self.mongo_client[self.mongo_db]
            collection_names = db.list_collection_names()
            
            # 过滤出包含电话号码数据的集合
            phone_collections = [name for name in collection_names if 'number' in name.lower()]
            
            if not phone_collections:
                self.logger.warning("未找到包含电话号码数据的集合，使用所有集合")
                phone_collections = collection_names
                
            self.logger.info(f"找到 {len(phone_collections)} 个集合: {phone_collections}")
            return phone_collections
            
        except Exception as e:
            self.logger.error(f"获取MongoDB集合失败: {e}")
            return []
    
    def get_recent_mongo_data(self, collection_name: str, days: int = 5) -> List[Dict]:
        """
        获取最近指定天数内的MongoDB数据（默认5天）
        
        MongoDB文档结构:
        - excellentnumbers: {phone, price, source_url, source, crawled_at}
        - numberbarn_numbers: {number, price, state, npa, page, source_url, created_at, updated_at}
        """
        try:
            db = self.mongo_client[self.mongo_db]
            collection = db[collection_name]

            # 时间范围：最近 days 天（含今天）到现在（UTC）
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=days)
            
            # 构建查询条件
            query = {}
            
            # 根据不同的集合类型调整查询
            if collection_name == 'numbers':  # excellentnumbers数据
                query = {"crawled_at": {"$gte": start_time, "$lt": end_time}}
            elif collection_name == 'numberbarn_numbers':
                query = {"created_at": {"$gte": start_time, "$lt": end_time}}
            else:
                # 尝试常见的时间字段
                time_fields = ['created_at', 'updated_at', 'crawled_at', 'timestamp']
                for field in time_fields:
                    try:
                        # 检查字段是否存在
                        sample_doc = collection.find_one({field: {"$exists": True}})
                        if sample_doc:
                            query = {field: {"$gte": start_time, "$lt": end_time}}
                            break
                    except:
                        continue
                
                if not query:
                    self.logger.warning(f"集合 {collection_name} 中未找到时间字段，获取所有数据")
                    query = {}
            
            # 执行查询
            documents = list(collection.find(query))
            self.logger.info(f"集合 {collection_name} 最近{days}天内找到 {len(documents)} 条数据")
            
            return documents
            
        except Exception as e:
            self.logger.error(f"从集合 {collection_name} 获取数据失败: {e}")
            return []
    
    def normalize_mongo_data(self, documents: List[Dict], collection_name: str) -> List[Dict]:
        """
        标准化MongoDB数据为 phone_numbers 表的字段：
        country_code, area_code, local_number, country, state_code, state_name, price_str, price, source_url, source, updated_at
        """
        normalized: List[Dict[str, Any]] = []
        for doc in documents:
            base = self._extract_fields(doc, collection_name)
            phone_raw = base["phone"]
            if not phone_raw:
                continue

            country_code, area_code, local_number = self._split_phone(phone_raw)
            # 优先使用文档里的区号提示（如 numberbarn 的 npa）
            area_code = base.get("area_code_hint") or area_code

            if not area_code or not local_number:
                continue

            state_code, state_name = self._normalize_state(base.get("state"))
            price_int = self.price_str_to_int(base.get("price_str", ""))

            normalized.append(
                {
                    "country_code": country_code or "1",
                    "area_code": area_code,
                    "local_number": local_number,
                    "country": base.get("country") or "USA",
                    "state_code": state_code,
                    "state_name": state_name,
                    "price_str": base.get("price_str"),
                    "price": price_int,
                    "source_url": base.get("source_url"),
                    "source": base.get("source"),
                    "updated_at": self._extract_timestamp(doc),
                }
            )

        self.logger.info("标准化后得到 %d 条有效记录", len(normalized))
        return normalized

    def _extract_fields(self, doc: Dict, collection_name: str) -> Dict[str, Any]:
        """根据来源集合提取基础字段，兼容不同源字段名。"""
        if collection_name == "numbers":  # excellentnumbers
            phone = doc.get("phone", "")
            price = doc.get("price", "")
            url = doc.get("source_url", "")
            source = doc.get("source", "excellent_number")
            country = doc.get("country", "USA")
            state = doc.get("region") or self._infer_state_from_url(url, collection_name)
            area_hint = None
        elif collection_name == "numberbarn_numbers":
            phone = doc.get("number", "")
            price = doc.get("price", "")
            url = doc.get("source_url", "")
            source = "numberbarn"
            country = doc.get("country", "USA")
            state = doc.get("state") or self._infer_state_from_url(url, collection_name)
            area_hint = doc.get("npa")
        else:
            phone = doc.get("phone", doc.get("number", ""))
            price = doc.get("price", "")
            url = doc.get("source_url", doc.get("url", ""))
            source = doc.get("source", collection_name)
            country = doc.get("country", "USA")
            state = doc.get("region") or self._infer_state_from_url(url, collection_name)
            area_hint = doc.get("area_code") or doc.get("npa")

        return {
            "phone": phone,
            "price_str": price,
            "source_url": url,
            "source": source,
            "country": country,
            "state": state,
            "area_code_hint": area_hint,
        }

    def _split_phone(self, phone: str) -> tuple[str, str, str]:
        """拆分电话号码 -> (country_code, area_code, local_number)。仅处理常见的10位北美号码。"""
        digits = re.sub(r"\D", "", phone or "")
        if not digits:
            return "", "", ""

        country_code = "1"
        national = digits

        if len(digits) > 10:
            national = digits[-10:]
            country_code = digits[:-10] or "1"
        elif len(digits) == 11 and digits.startswith("1"):
            national = digits[1:]
        elif len(digits) == 10:
            national = digits
        else:
            return country_code, "", ""

        if len(national) < 10:
            return country_code, "", ""

        area_code = national[:3]
        local_number = national[3:]
        return country_code, area_code, local_number

    def _infer_state_from_url(self, url: str, collection_name: str) -> str:
        """从 URL 提取州/地区：excellentnumbers 用路径，numberbarn 用 state 参数。"""
        if not url:
            return ""
        try:
            if collection_name == "numbers":
                m = re.search(r"/categories/([^/]+)/", url)
                if m:
                    return m.group(1)
            m = re.search(r"[?&]state=([^&]+)", url)
            if m:
                return m.group(1)
        except Exception:
            return ""
        return ""

    def _extract_timestamp(self, doc: Dict[str, Any]) -> datetime:
        """从文档里优先取时间字段，缺省用当前UTC时间。"""
        for field in ("updated_at", "crawled_at", "created_at", "timestamp"):
            ts = doc.get(field)
            if isinstance(ts, datetime):
                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc)

    def _normalize_state(self, state_value: Optional[str]) -> tuple[str, str]:
        """标准化州字段，返回 (state_code, state_name)，依赖 state_codes 模块。"""
        if not state_value:
            return "", ""
        s = str(state_value).strip()
        if not s:
            return "", ""

        try:
            code = normalize_state(s)
            return code, state_name(code)
        except ValueError:
            # 未识别时返回空，让上游保持空值而非写入非法代码
            return "", ""

    def insert_to_postgresql(self, data: List[Dict]) -> bool:
        """将数据插入PostgreSQL，拆分小步骤以便维护。"""
        if not data:
            return True

        try:
            with self.postgres_conn.cursor() as cursor:
                unique_data = self._deduplicate_input(data)
                stats = {"inserted": 0, "updated": 0, "skipped": 0}

                for batch in self._iter_batches(unique_data):
                    existing = self._fetch_existing_records(cursor, batch)
                    to_insert, to_update, skipped = self._classify_records(batch, existing)
                    stats["skipped"] += skipped

                    if not self.dry_run:
                        self._insert_batch(cursor, to_insert)
                        self._update_batch(cursor, to_update)

                    stats["inserted"] += len(to_insert)
                    stats["updated"] += len(to_update)

                if not self.dry_run:
                    self.postgres_conn.commit()

                mode = "干运行" if self.dry_run else "实际同步"
                self.logger.info(
                    "%s: 插入 %d 条，更新 %d 条，跳过 %d 条",
                    mode,
                    stats["inserted"],
                    stats["updated"],
                    stats["skipped"],
                )
                return True

        except DatabaseError as e:
            self.postgres_conn.rollback()
            # 打印具体错误栈，便于定位插入失败原因
            self.logger.exception("插入PostgreSQL失败: %s", e)
            return False
        except Exception as e:
            self.postgres_conn.rollback()
            # 捕获未知异常并打印详细栈
            self.logger.exception("插入PostgreSQL时发生错误: %s", e)
            return False

    # -------- Helper methods for PostgreSQL upsert pipeline --------
    def _deduplicate_input(self, data: List[Dict]) -> List[Dict]:
        """对输入列表按 area_code+local_number 去重，保留最新 updated_at 的记录。"""
        unique: Dict[str, Dict] = {}
        for record in data:
            area = record.get("area_code")
            local = record.get("local_number")
            if not area or not local:
                continue
            key = f"{area}:{local}"
            if key not in unique or record["updated_at"] > unique[key]["updated_at"]:
                unique[key] = record
        if len(unique) < len(data):
            self.logger.info("输入数据去重: %d -> %d 条记录", len(data), len(unique))
        return list(unique.values())

    def _iter_batches(self, data: List[Dict]):
        """生成器：按 batch_size 切分批次。"""
        for i in range(0, len(data), self.batch_size):
            yield data[i : i + self.batch_size]

    def _fetch_existing_records(self, cursor, batch: List[Dict]) -> Dict[str, tuple]:
        """一次查询批次中已有的号码记录，键为 area_code:local_number。"""
        keys = [(r["area_code"], r["local_number"]) for r in batch]
        if not keys:
            return {}

        # 使用显式 VALUES 列表，避免 ANY(array) 触发 unknown 类型的哈希错误
        values_sql = ",".join(cursor.mogrify("(%s,%s)", k).decode() for k in keys)
        query = f"""
            SELECT
                area_code,
                local_number,
                price_str,
                price,
                source_url,
                source,
                country_code,
                country,
                state_code,
                state_name
            FROM phone_numbers
            WHERE (area_code, local_number) IN ({values_sql})
        """
        cursor.execute(query)
        return {f"{row[0]}:{row[1]}": row for row in cursor.fetchall()}

    def _classify_records(
        self, batch: List[Dict], existing: Dict[str, tuple]
    ) -> tuple[List[Dict], List[Dict], int]:
        """将批次数据分为需插入、需更新和可跳过三类。"""
        to_insert: List[Dict] = []
        to_update: List[Dict] = []
        skipped = 0

        for record in batch:
            key = f"{record['area_code']}:{record['local_number']}"
            if key not in existing:
                to_insert.append(record)
                continue

            (
                _,
                _,
                price_str,
                price,
                source_url,
                source,
                existing_cc,
                existing_country,
                existing_state_code,
                existing_state_name,
            ) = existing[key]
            if (
                record.get("price_str") != price_str
                or record.get("price") != price
                or record.get("source_url") != source_url
                or record.get("source") != source
                or record.get("country_code") != existing_cc
                or record.get("country") != existing_country
                or (record.get("state_code") or "") != (existing_state_code or "")
                or (record.get("state_name") or "") != (existing_state_name or "")
            ):
                to_update.append(record)
            else:
                skipped += 1
        return to_insert, to_update, skipped

    def _insert_batch(self, cursor, records: List[Dict]) -> None:
        """批量插入新记录，使用 ON CONFLICT upsert。"""
        if not records:
            return
        query = """
            INSERT INTO phone_numbers
            (country_code, area_code, local_number, country, state_code, state_name, price_str, price, source_url, source, updated_at)
            VALUES %s
            ON CONFLICT (area_code, local_number) DO UPDATE SET
                country_code = EXCLUDED.country_code,
                country = EXCLUDED.country,
                state_code = EXCLUDED.state_code,
                state_name = EXCLUDED.state_name,
                price_str = EXCLUDED.price_str,
                price = EXCLUDED.price,
                source_url = EXCLUDED.source_url,
                source = EXCLUDED.source,
                updated_at = EXCLUDED.updated_at
        """
        values = [
            (
                r["country_code"],
                r["area_code"],
                r["local_number"],
                r["country"],
                r["state_code"],
                r["state_name"],
                r["price_str"],
                r["price"],
                r["source_url"],
                r["source"],
                r["updated_at"],
            )
            for r in records
        ]
        execute_values(cursor, query, values)

    def _update_batch(self, cursor, records: List[Dict]) -> None:
        """兼容接口；PostgreSQL 已在 insert 中 upsert，这里留空。"""
        return

    def close_connections(self):
        """关闭所有连接"""
        if self.mongo_client:
            self.mongo_client.close()
            self.logger.info("MongoDB连接已关闭")
        
        if self.postgres_conn:
            self.postgres_conn.close()
            self.logger.info("PostgreSQL连接已关闭")
            
    def sync_collection(self, collection_name: str) -> bool:
        """同步最近5天的数据，避免长周期全量扫描"""
        self.logger.info(f"开始同步集合: {collection_name}")

        mongo_data = self.get_recent_mongo_data(collection_name, days=5)
        if not mongo_data:
            self.logger.info(f"{collection_name} 最近5天无数据，跳过")
            return True

        normalized_data = self.normalize_mongo_data(mongo_data, collection_name)
        if not normalized_data:
            self.logger.info(f"{collection_name} 最近5天标准化后无有效数据，跳过")
            return True

        total_records = 0
        for i in range(0, len(normalized_data), self.batch_size):
            batch = normalized_data[i:i + self.batch_size]
            if self.insert_to_postgresql(batch):
                total_records += len(batch)
            else:
                self.logger.error(f"{collection_name} 最近5天批次 {i//self.batch_size + 1} 插入失败")
                return False

        self.logger.info(f"{collection_name} 最近5天同步完成，处理 {total_records} 条记录")
        return True
    def sync_all_collections(self) -> bool:
        """同步所有集合的数据"""
        self.logger.info("开始同步所有集合的数据")
        
        # 获取所有集合
        collections = self.get_mongodb_collections()
        if not collections:
            self.logger.error("没有找到任何MongoDB集合")
            return False
        
        # 同步每个集合
        success_count = 0
        for collection_name in collections:
            if self.sync_collection(collection_name):
                success_count += 1
            else:
                self.logger.error(f"同步集合 {collection_name} 失败")
        
        total_collections = len(collections)
        self.logger.info(f"同步完成: {success_count}/{total_collections} 个集合成功")
        
        return success_count == total_collections
    
    def run(self) -> bool:
        """执行同步任务"""
        self.logger.info("开始执行MongoDB到PostgreSQL同步任务")
        
        try:
            # 连接数据库
            if not self.connect_mongodb():
                return False
            
            if not self.connect_postgresql():
                return False
            
            # 执行同步
            return self.sync_all_collections()
            
        except Exception as e:
            self.logger.error(f"同步任务失败: {e}")
            return False
        finally:
            self.close_connections()


async def main():
    """主函数"""
    sync = MongoToPostgreSQLSync(
        # MongoDB配置
        mongo_host="43.159.58.235",
        mongo_user="extra_numbers",
        mongo_password="RsBWd3hTAZeR7kC4",
        mongo_port=27017,
        mongo_db="extra_numbers",
        
        # PostgreSQL配置（请根据实际情况修改）
        postgres_host="43.159.58.235",
        postgres_port=4088,
        postgres_db="numharvest",
        postgres_user="numharvest",
        postgres_password="pP963470667",
        
        # 同步配置
        batch_size=1000,  # 批量处理的大小
        dry_run=False  # 设置为True进行测试运行
    )
    
    success = sync.run()
    
    if success:
        print("同步任务完成")
        sys.exit(0)
    else:
        print("同步任务失败")
        sys.exit(1)

if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())
