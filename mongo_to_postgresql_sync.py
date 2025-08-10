import asyncio
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import logging

from pymongo import MongoClient
import psycopg2
from psycopg2 import sql, DatabaseError
from psycopg2.extras import execute_values


class MongoToPostgreSQLSync:
    """MongoDB到PostgreSQL数据同步器"""
    
    def __init__(self, 
                 mongo_host: str = "43.159.58.235",
                 mongo_user: str = "extra_numbers",
                 mongo_password: str = "RsBWd3hTAZeR7kC4",
                 mongo_port: int = 27017,
                 mongo_db: str = "extra_numbers",
                 
                 postgres_host: str = "localhost",
                 postgres_port: int = 5432,
                 postgres_db: str = "phone_numbers_db",
                 postgres_user: str = "postgres",
                 postgres_password: str = "",
                 
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
    
    def connect_postgresql(self) -> bool:
        """连接PostgreSQL"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                database=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password
            )
            
            # 测试连接
            with self.postgres_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            
            self.logger.info(f"成功连接到PostgreSQL: {self.postgres_host}:{self.postgres_port}")
            return True
            
        except DatabaseError as e:
            self.logger.error(f"PostgreSQL连接失败: {e}")
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
    
    def get_today_mongo_data(self, collection_name: str) -> List[Dict]:
        """
        从MongoDB获取今天的数据
        
        MongoDB文档结构:
        - excellentnumbers: {phone, price, source_url, source, crawled_at}
        - numberbarn_numbers: {number, price, state, npa, page, source_url, created_at, updated_at}
        """
        try:
            db = self.mongo_client[self.mongo_db]
            collection = db[collection_name]
            
            # 获取今天的开始时间（UTC）
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow = today + timedelta(days=1)
            
            # 构建查询条件
            query = {}
            
            # 根据不同的集合类型调整查询
            if collection_name == 'numbers':  # excellentnumbers数据
                query = {"crawled_at": {"$gte": today, "$lt": tomorrow}}
            elif collection_name == 'numberbarn_numbers':
                query = {"created_at": {"$gte": today, "$lt": tomorrow}}
            else:
                # 尝试常见的时间字段
                time_fields = ['created_at', 'updated_at', 'crawled_at', 'timestamp']
                for field in time_fields:
                    try:
                        # 检查字段是否存在
                        sample_doc = collection.find_one({field: {"$exists": True}})
                        if sample_doc:
                            query = {field: {"$gte": today, "$lt": tomorrow}}
                            break
                    except:
                        continue
                
                if not query:
                    self.logger.warning(f"集合 {collection_name} 中未找到时间字段，获取所有数据")
                    query = {}
            
            # 执行查询
            documents = list(collection.find(query))
            self.logger.info(f"集合 {collection_name} 中找到 {len(documents)} 条今天的数据")
            
            return documents
            
        except Exception as e:
            self.logger.error(f"从集合 {collection_name} 获取数据失败: {e}")
            return []
    
    def normalize_mongo_data(self, documents: List[Dict], collection_name: str) -> List[Dict]:
        """
        标准化MongoDB数据为PostgreSQL格式
        
        PostgreSQL表结构:
        CREATE TABLE phone_numbers (
            "id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            "phone" VARCHAR(20) NOT NULL,
            "price_str" VARCHAR(50),
            "original_price" integer,
            "adjusted_price" integer,
            "source_url" TEXT,
            "source" VARCHAR(50),
            "updated_at" timestamp with time zone
        );
        """
        normalized_data = []
        
        for doc in documents:
            try:
                # 根据集合类型提取不同的字段
                if collection_name == 'numbers':  # excellentnumbers数据
                    phone = doc.get('phone', '')
                    price_str = doc.get('price', '')
                    source_url = doc.get('source_url', '')
                    source = doc.get('source', 'excellent_number')
                    
                elif collection_name == 'numberbarn_numbers':
                    phone = doc.get('number', '')
                    price_str = doc.get('price', '')
                    source_url = doc.get('source_url', '')
                    source = 'numberbarn'
                    
                else:
                    # 通用处理
                    phone = doc.get('phone', doc.get('number', ''))
                    price_str = doc.get('price', '')
                    source_url = doc.get('source_url', doc.get('url', ''))
                    source = doc.get('source', collection_name)
                
                if not phone:
                    continue
                
                # 转换价格
                original_price = self.price_str_to_int(price_str)
                adjusted_price = original_price  # 可以根据需要调整
                
                # 构建PostgreSQL记录
                postgres_record = {
                    'phone': phone,
                    'price_str': price_str,
                    'original_price': original_price,
                    'adjusted_price': adjusted_price * 1.2,
                    'source_url': source_url,
                    'source': source,
                    'updated_at': datetime.now(timezone.utc)
                }
                
                normalized_data.append(postgres_record)
                
            except Exception as e:
                self.logger.warning(f"标准化文档失败: {e}, 文档: {doc}")
                continue
        
        self.logger.info(f"标准化后得到 {len(normalized_data)} 条有效记录")
        return normalized_data
    
    def insert_to_postgresql(self, data: List[Dict]) -> bool:
        """将数据插入PostgreSQL"""
        if not data:
            return True
            
        try:
            with self.postgres_conn.cursor() as cursor:
                # 先对数据进行去重，以phone为键，保留最后一条记录
                unique_data = {}
                for record in data:
                    phone = record['phone']
                    # 如果已存在，比较更新时间，保留最新的
                    if phone in unique_data:
                        existing_time = unique_data[phone]['updated_at']
                        new_time = record['updated_at']
                        if new_time > existing_time:
                            unique_data[phone] = record
                    else:
                        unique_data[phone] = record
                
                # 转换为列表
                deduplicated_data = list(unique_data.values())
                
                if len(deduplicated_data) < len(data):
                    self.logger.info(f"数据去重: {len(data)} -> {len(deduplicated_data)} 条记录")
                
                # 分批处理，避免单批次数据量过大
                total_inserted = 0
                for i in range(0, len(deduplicated_data), self.batch_size):
                    batch = deduplicated_data[i:i + self.batch_size]
                    
                    # 构建插入语句
                    insert_query = sql.SQL("""
                        INSERT INTO phone_numbers 
                        (phone, price_str, original_price, adjusted_price, source_url, source, updated_at)
                        VALUES %s
                        ON CONFLICT (phone) DO UPDATE SET
                            price_str = EXCLUDED.price_str,
                            original_price = EXCLUDED.original_price,
                            adjusted_price = EXCLUDED.adjusted_price,
                            source_url = EXCLUDED.source_url,
                            source = EXCLUDED.source,
                            updated_at = EXCLUDED.updated_at
                    """)
                    
                    # 准备数据
                    values = []
                    for record in batch:
                        values.append((
                            record['phone'],
                            record['price_str'],
                            record['original_price'],
                            record['adjusted_price'],
                            record['source_url'],
                            record['source'],
                            record['updated_at']
                        ))
                    
                    # 执行批量插入
                    if self.dry_run:
                        self.logger.info(f"干运行模式：将插入 {len(values)} 条记录")
                        continue
                    
                    execute_values(cursor, insert_query, values)
                    total_inserted += len(values)
                    
                    if self.dry_run:
                        continue
                
                if not self.dry_run:
                    self.postgres_conn.commit()
                    self.logger.info(f"成功插入/更新 {total_inserted} 条记录到PostgreSQL")
                else:
                    self.logger.info(f"干运行模式：将插入/更新 {len(deduplicated_data)} 条记录到PostgreSQL")
                
                return True
                
        except DatabaseError as e:
            self.postgres_conn.rollback()
            self.logger.error(f"插入PostgreSQL失败: {e}")
            return False
        except Exception as e:
            self.postgres_conn.rollback()
            self.logger.error(f"插入PostgreSQL时发生错误: {e}")
            return False

    def close_connections(self):
        """关闭所有连接"""
        if self.mongo_client:
            self.mongo_client.close()
            self.logger.info("MongoDB连接已关闭")
        
        if self.postgres_conn:
            self.postgres_conn.close()
            self.logger.info("PostgreSQL连接已关闭")
            
    def sync_collection(self, collection_name: str) -> bool:
        """同步单个集合的数据"""
        self.logger.info(f"开始同步集合: {collection_name}")
        
        # 获取MongoDB数据
        mongo_data = self.get_today_mongo_data(collection_name)
        if not mongo_data:
            self.logger.info(f"集合 {collection_name} 没有今天的数据")
            return True
        
        # 标准化数据
        normalized_data = self.normalize_mongo_data(mongo_data, collection_name)
        if not normalized_data:
            self.logger.info(f"集合 {collection_name} 标准化后没有有效数据")
            return True
        
        # 分批插入PostgreSQL
        total_inserted = 0
        for i in range(0, len(normalized_data), self.batch_size):
            batch = normalized_data[i:i + self.batch_size]
            
            if self.insert_to_postgresql(batch):
                total_inserted += len(batch)
            else:
                self.logger.error(f"插入批次 {i//self.batch_size + 1} 失败")
                return False
        
        self.logger.info(f"集合 {collection_name} 同步完成，共处理 {total_inserted} 条记录")
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
        postgres_port=5432,
        postgres_db="numbers",  # 你的 PostgreSQL 数据库名
        postgres_user="postgres",
        postgres_password="axad3M3MJN57NWzr",  # 请替换为实际的密码
        
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
