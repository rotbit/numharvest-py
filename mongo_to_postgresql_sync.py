import asyncio
import re
import sys
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import mysql.connector
from mysql.connector import Error as MySQLError
from pymongo import MongoClient


class MongoToMySQLSync:
    """MongoDB到MySQL数据同步器"""
    
    def __init__(self, 
                 mongo_host: str = "43.159.58.235",
                 mongo_user: str = "extra_numbers",
                 mongo_password: str = "RsBWd3hTAZeR7kC4",
                 mongo_port: int = 27017,
                 mongo_db: str = "extra_numbers",
                 
                 mysql_host: str = "localhost",
                 mysql_port: int = 3306,
                 mysql_db: str = "phone_numbers_db",
                 mysql_user: str = "root",
                 mysql_password: str = "",
                 
                 batch_size: int = 1000,
                 dry_run: bool = False):
        
        # MongoDB配置
        self.mongo_host = mongo_host
        self.mongo_user = mongo_user
        self.mongo_password = mongo_password
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        
        # MySQL配置
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_db = mysql_db
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        
        # 同步配置
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        # 初始化连接
        self.mongo_client = None
        self.mysql_conn = None
        
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
    
    def price_str_to_int(self, price_str: str) -> Optional[int]:
        """
        将价格字符串转换为整数
        支持格式: $1,234, $99.99, $1234, $1,234.56等
        """
        if not price_str:
            return None
            
        try:
            # 移除$符号和所有空格
            clean_price = re.sub(r'[$\s]', '', price_str)
            
            # 处理包含小数的情况（向下取整）
            if '.' in clean_price:
                clean_price = clean_price.split('.')[0]
            
            # 移除所有逗号
            clean_price = clean_price.replace(',', '')
            
            # 转换为整数
            return int(clean_price) if clean_price else None
            
        except (ValueError, AttributeError):
            self.logger.warning(f"无法解析价格字符串: {price_str}")
            return None
    
    def connect_mongodb(self) -> bool:
        """连接MongoDB"""
        try:
            connection_string = f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/?authSource=extra_numbers"
            self.mongo_client = MongoClient(connection_string)
            
            # 测试连接
            self.mongo_client.admin.command('ping')
            self.logger.info(f"成功连接到MongoDB: {self.mongo_host}:{self.mongo_port}")
            return True
            
        except Exception as e:
            self.logger.error(f"MongoDB连接失败: {e}")
            return False
    
    def connect_mysql(self) -> bool:
        """连接MySQL"""
        try:
            self.mysql_conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                database=self.mysql_db,
                user=self.mysql_user,
                password=self.mysql_password,
                autocommit=False,
            )
            cur = self.mysql_conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            self.logger.info("成功连接到 MySQL: %s:%s", self.mysql_host, self.mysql_port)
            return True
        except MySQLError as e:
            self.logger.error("MySQL连接失败: %s", e)
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
    
    def get_today_mongo_data(self, collection_name: str, target_date: Optional[datetime] = None) -> List[Dict]:
        """
        按天获取MongoDB某日的数据（用于逐日同步，直到今天）
        
        MongoDB文档结构:
        - excellentnumbers: {phone, price, source_url, source, crawled_at}
        - numberbarn_numbers: {number, price, state, npa, page, source_url, created_at, updated_at}
        """
        try:
            db = self.mongo_client[self.mongo_db]
            collection = db[collection_name]

            # 目标日期的起止时间（UTC）
            target_date = target_date or datetime.now(timezone.utc)
            start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            end_time = start_time + timedelta(days=1)
            
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
            self.logger.info(f"集合 {collection_name} 在 {start_time.date()} 找到 {len(documents)} 条数据")
            
            return documents
            
        except Exception as e:
            self.logger.error(f"从集合 {collection_name} 获取数据失败: {e}")
            return []
    
    def normalize_mongo_data(self, documents: List[Dict], collection_name: str) -> List[Dict]:
        """
        标准化MongoDB数据为MySQL phone_numbers 结构：
        country_code, national_number, country, region, price_str, original_price, adjusted_price, source_url, source, updated_at
        """
        normalized = []
        for doc in documents:
            phone_raw, price_str, source_url, source, country, region = self._extract_fields(doc, collection_name)
            if not phone_raw:
                continue

            country_code, national_number = self._split_phone(phone_raw)
            original_price = self.price_str_to_int(price_str)
            adjusted_price = int(original_price * 1.2) if original_price is not None else None

            normalized.append(
                {
                    "country_code": country_code,
                    "national_number": national_number,
                    "country": country,
                    "region": region,
                    "price_str": price_str,
                    "original_price": original_price,
                    "adjusted_price": adjusted_price,
                    "source_url": source_url,
                    "source": source,
                    "updated_at": datetime.now(timezone.utc),
                }
            )

        self.logger.info("标准化后得到 %d 条有效记录", len(normalized))
        return normalized

    def _extract_fields(self, doc: Dict, collection_name: str) -> tuple:
        """根据来源集合提取基础字段。"""
        if collection_name == "numbers":  # excellentnumbers
            phone = doc.get("phone", "")
            price = doc.get("price", "")
            url = doc.get("source_url", "")
            source = doc.get("source", "excellent_number")
            country = doc.get("country", "USA")
            region = doc.get("region", "")
        elif collection_name == "numberbarn_numbers":
            phone = doc.get("number", "")
            price = doc.get("price", "")
            url = doc.get("source_url", "")
            source = "numberbarn"
            country = doc.get("country", "USA")
            region = doc.get("state", "")
        else:
            phone = doc.get("phone", doc.get("number", ""))
            price = doc.get("price", "")
            url = doc.get("source_url", doc.get("url", ""))
            source = doc.get("source", collection_name)
            country = doc.get("country", "USA")
            region = doc.get("region", "")
        return phone, price, url, source, country, region

    def _split_phone(self, phone: str) -> tuple[str, str]:
        """将原始号码拆为国家码和本机号；默认美国1。"""
        digits = re.sub(r"\\D", "", phone)
        if len(digits) == 11 and digits.startswith("1"):
            return "1", digits[1:]
        if len(digits) > 11:
            return digits[:-10], digits[-10:]
        if len(digits) == 10:
            return "1", digits
        return "1", digits  # fallback

    def insert_to_mysql(self, data: List[Dict]) -> bool:
        """将数据插入MySQL，拆分小步骤以便维护。"""
        if not data:
            return True

        try:
            with self.mysql_conn.cursor() as cursor:
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
                    self.mysql_conn.commit()

                mode = "干运行" if self.dry_run else "实际同步"
                self.logger.info(
                    "%s: 插入 %d 条，更新 %d 条，跳过 %d 条",
                    mode,
                    stats["inserted"],
                    stats["updated"],
                    stats["skipped"],
                )
                return True

        except MySQLError as e:
            self.mysql_conn.rollback()
            self.logger.error(f"插入MySQL失败: {e}")
            return False
        except Exception as e:
            self.mysql_conn.rollback()
            self.logger.error(f"插入MySQL时发生错误: {e}")
            return False

    # -------- Helper methods for MySQL upsert pipeline --------
    def _deduplicate_input(self, data: List[Dict]) -> List[Dict]:
        """对输入列表按 phone 去重，保留最新 updated_at 的记录。"""
        unique: Dict[str, Dict] = {}
        for record in data:
            phone = record.get("phone")
            if not phone:
                continue
            if phone not in unique or record["updated_at"] > unique[phone]["updated_at"]:
                unique[phone] = record
        if len(unique) < len(data):
            self.logger.info("输入数据去重: %d -> %d 条记录", len(data), len(unique))
        return list(unique.values())

    def _iter_batches(self, data: List[Dict]):
        """生成器：按 batch_size 切分批次。"""
        for i in range(0, len(data), self.batch_size):
            yield data[i : i + self.batch_size]

    def _fetch_existing_records(self, cursor, batch: List[Dict]) -> Dict[str, tuple]:
        """一次查询批次中已有的号码记录，键为 country_code+national_number。"""
        keys = [(r["country_code"], r["national_number"]) for r in batch]
        placeholders = ",".join(["(%s,%s)"] * len(keys))
        flat_params = [item for pair in keys for item in pair]
        query = f"""
            SELECT country_code, national_number, price_str, original_price, source_url, source 
            FROM phone_numbers 
            WHERE (country_code, national_number) IN ({placeholders})
        """
        cursor.execute(query, flat_params)
        return {f"{row[0]}:{row[1]}": row for row in cursor.fetchall()}

    def _classify_records(
        self, batch: List[Dict], existing: Dict[str, tuple]
    ) -> tuple[List[Dict], List[Dict], int]:
        """将批次数据分为需插入、需更新和可跳过三类。"""
        to_insert: List[Dict] = []
        to_update: List[Dict] = []
        skipped = 0

        for record in batch:
            key = f"{record['country_code']}:{record['national_number']}"
            if key not in existing:
                to_insert.append(record)
                continue

            _, _, price_str, original_price, source_url, source = existing[key]
            if (
                record["price_str"] != price_str
                or record["original_price"] != original_price
                or record["source_url"] != source_url
                or record["source"] != source
            ):
                to_update.append(record)
            else:
                skipped += 1
        return to_insert, to_update, skipped

    def _insert_batch(self, cursor, records: List[Dict]) -> None:
        """批量插入新记录，使用 ON DUPLICATE KEY 更新。"""
        if not records:
            return
        query = """
            INSERT INTO phone_numbers
            (country_code, national_number, country, region, price_str, original_price, adjusted_price, source_url, source, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                country = VALUES(country),
                region = VALUES(region),
                price_str = VALUES(price_str),
                original_price = VALUES(original_price),
                adjusted_price = VALUES(adjusted_price),
                source_url = VALUES(source_url),
                source = VALUES(source),
                updated_at = VALUES(updated_at)
        """
        params = [
            (
                r["country_code"],
                r["national_number"],
                r["country"],
                r["region"],
                r["price_str"],
                r["original_price"],
                r["adjusted_price"],
                r["source_url"],
                r["source"],
                r["updated_at"],
            )
            for r in records
        ]
        cursor.executemany(query, params)

    def _update_batch(self, cursor, records: List[Dict]) -> None:
        """兼容接口；MySQL 插入已用 upsert，这里留空即可。"""
        return

    def close_connections(self):
        """关闭所有连接"""
        if self.mongo_client:
            self.mongo_client.close()
            self.logger.info("MongoDB连接已关闭")
        
        if self.mysql_conn:
            self.mysql_conn.close()
            self.logger.info("MySQL连接已关闭")
            
    def sync_collection(self, collection_name: str) -> bool:
        """按天同步单个集合的数据，逐日跑到今天"""
        self.logger.info(f"开始同步集合: {collection_name}")

        # 从5个月前开始逐日同步（约150天），避免一次性大查询
        today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = today_utc - timedelta(days=150)

        total_records = 0

        current_date = start_date
        while current_date <= today_utc:
            mongo_data = self.get_today_mongo_data(collection_name, current_date)
            if not mongo_data:
                self.logger.info(f"{collection_name} {current_date.date()} 无数据，跳过")
                current_date += timedelta(days=1)
                continue

            normalized_data = self.normalize_mongo_data(mongo_data, collection_name)
            if not normalized_data:
                self.logger.info(f"{collection_name} {current_date.date()} 标准化后无有效数据，跳过")
                current_date += timedelta(days=1)
                continue

            # 分批插入PostgreSQL
            for i in range(0, len(normalized_data), self.batch_size):
                batch = normalized_data[i:i + self.batch_size]
                if self.insert_to_postgresql(batch):
                    total_records += len(batch)
                else:
                    self.logger.error(f"{collection_name} {current_date.date()} 批次 {i//self.batch_size + 1} 插入失败")
                    return False

            self.logger.info(f"{collection_name} {current_date.date()} 同步完成，处理 {len(normalized_data)} 条记录")
            current_date += timedelta(days=1)

        self.logger.info(f"集合 {collection_name} 同步完成，最近约5个月共处理 {total_records} 条记录")
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
        self.logger.info("开始执行MongoDB到MySQL同步任务")
        
        try:
            # 连接数据库
            if not self.connect_mongodb():
                return False
            
            if not self.connect_mysql():
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
    sync = MongoToMySQLSync(
        # MongoDB配置
        mongo_host="43.159.58.235",
        mongo_user="extra_numbers",
        mongo_password="RsBWd3hTAZeR7kC4",
        mongo_port=27017,
        mongo_db="extra_numbers",
        
        # MySQL配置（请根据实际情况修改）
        mysql_host="43.159.58.235",
        mysql_port=3306,
        mysql_db="numbers",
        mysql_user="root",
        mysql_password="axad3M3MJN57NWzr",
        
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
