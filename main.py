#!/usr/bin/env python3
"""
主调度程序 - 定时执行任务并同步数据
"""
import time
import schedule
import logging
from datetime import datetime
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# 添加模块路径
sys.path.extend([
    os.path.join(os.path.dirname(__file__), 'excellentnumberstask'),
    os.path.join(os.path.dirname(__file__), 'numberbarntask')
])

from excellentnumberstask import AreaCodeNumbersHarvester
from numberbarntask import NumberbarnNumberExtractor  
from mongo_to_postgresql_sync import MongoToPostgreSQLSync

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('numharvest_scheduler.log'),
        logging.StreamHandler()
    ]
)

class NumberHarvestScheduler:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.is_running = False
        
        # MongoDB配置
        self.mongo_config = {
            "host": "43.159.58.235",
            "user": "extra_numbers",
            "password": "RsBWd3hTAZeR7kC4",
            "port": 27017,
            "db": "extra_numbers"
        }
        
        # PostgreSQL配置
        self.postgres_config = {
            "host": "43.159.58.235",
            "port": 5432,
            "db": "numbers", 
            "user": "postgres",
            "password": "axad3M3MJN57NWzr"
        }
            
    def _run_task(self, task_name: str, task_func, *args) -> tuple[bool, str, Any]:
        """统一的任务执行方法"""
        start_time = datetime.now()
        self.logger.info(f"开始执行{task_name}任务")
        
        try:
            result = task_func(*args)
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"{task_name}任务完成，耗时: {duration:.2f}秒")
            return True, f"{task_name}任务成功完成", result
        except Exception as e:
            self.logger.error(f"执行{task_name}任务时出错: {e}")
            return False, f"执行{task_name}任务时出错: {e}", None
    
    def run_parallel_scraping_and_sync(self):
        """并行执行抓取任务，完成后同步数据"""
        if self.is_running:
            self.logger.warning("有任务正在运行，跳过本次任务")
            return
            
        self.is_running = True
        start_time = datetime.now()
        
        try:
            # 创建任务实例
            excellentnumbers_harvester = AreaCodeNumbersHarvester(
                mongo_host=self.mongo_config["host"],
                mongo_user=self.mongo_config["user"],
                mongo_password=self.mongo_config["password"],
                mongo_port=self.mongo_config["port"],
                mongo_db=self.mongo_config["db"],
                mongo_collection="numbers",
                headless=True
            )
            
            numberbarn_extractor = NumberbarnNumberExtractor(
                mongo_host=self.mongo_config["host"],
                mongo_password=self.mongo_config["password"],
                mongo_db=self.mongo_config["db"]
            )
            
            # 并行执行抓取任务
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(self._run_task, "excellentnumbers", 
                                   excellentnumbers_harvester.run, ".", None),
                    executor.submit(self._run_task, "numberbarn", 
                                   numberbarn_extractor.run)
                ]
                
                results = [f.result() for f in futures]
                
                # 检查所有任务是否成功
                if all(result[0] for result in results):
                    self.logger.info("抓取任务完成，开始数据同步")
                    
                    # 执行数据同步
                    sync_processor = MongoToPostgreSQLSync(
                        mongo_host=self.mongo_config["host"],
                        mongo_user=self.mongo_config["user"],
                        mongo_password=self.mongo_config["password"],
                        mongo_port=self.mongo_config["port"],
                        mongo_db=self.mongo_config["db"],
                        postgres_host=self.postgres_config["host"],
                        postgres_port=self.postgres_config["port"],
                        postgres_db=self.postgres_config["db"],
                        postgres_user=self.postgres_config["user"],
                        postgres_password=self.postgres_config["password"],
                        batch_size=1000,
                        dry_run=False
                    )
                    
                    sync_result = self._run_task("数据同步", sync_processor.run)
                    
                    duration = (datetime.now() - start_time).total_seconds()
                    self.logger.info(f"所有任务完成，总耗时: {duration:.2f}秒")
                else:
                    self.logger.error("部分任务失败，跳过数据同步")
                    
        except Exception as e:
            self.logger.error(f"执行任务时出错: {e}")
        finally:
            self.is_running = False
            
    def setup_schedule(self):
        """设置定时任务调度"""
        schedule.every().day.at("08:00").do(self.run_parallel_scraping_and_sync)
        self.logger.info("定时任务调度设置完成：每天8点执行")
        
    def run_scheduler(self):
        """运行调度器主循环"""
        self.setup_schedule()
        self.logger.info("数字收获调度器启动")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("调度器停止")
    
    def run_single_task(self, task_type: str):
        """执行单个任务"""
        if self.is_running:
            self.logger.warning("有任务正在运行，跳过本次任务")
            return
            
        self.is_running = True
        try:
            if task_type == "excellentnumbers":
                harvester = AreaCodeNumbersHarvester(
                    mongo_host=self.mongo_config["host"],
                    mongo_user=self.mongo_config["user"],
                    mongo_password=self.mongo_config["password"],
                    mongo_port=self.mongo_config["port"],
                    mongo_db=self.mongo_config["db"],
                    mongo_collection="numbers",
                    headless=True
                )
                result = self._run_task("excellentnumbers", harvester.run, ".", None)
                
            elif task_type == "numberbarn":
                extractor = NumberbarnNumberExtractor(
                    mongo_host=self.mongo_config["host"],
                    mongo_password=self.mongo_config["password"],
                    mongo_db=self.mongo_config["db"]
                )
                result = self._run_task("numberbarn", extractor.run)
                
            elif task_type == "sync":
                sync_processor = MongoToPostgreSQLSync(
                    mongo_host=self.mongo_config["host"],
                    mongo_user=self.mongo_config["user"],
                    mongo_password=self.mongo_config["password"],
                    mongo_port=self.mongo_config["port"],
                    mongo_db=self.mongo_config["db"],
                    postgres_host=self.postgres_config["host"],
                    postgres_port=self.postgres_config["port"],
                    postgres_db=self.postgres_config["db"],
                    postgres_user=self.postgres_config["user"],
                    postgres_password=self.postgres_config["password"],
                    batch_size=1000,
                    dry_run=False
                )
                result = self._run_task("数据同步", sync_processor.run)
                
            self.logger.info(f"单独执行{task_type}任务结果: {result[1]}")
        finally:
            self.is_running = False


def main():
    """主函数"""
    scheduler = NumberHarvestScheduler()
    
    if len(sys.argv) == 1:
        scheduler.run_scheduler()
        return
    
    command = sys.argv[1]
    
    if command == "--parallel":
        # 立即执行一次，然后启动定时调度器
        scheduler.logger.info("立即执行一次并行任务，然后启动定时调度器")
        scheduler.run_parallel_scraping_and_sync()
        scheduler.run_scheduler()
    elif command == "--test":
        # 只执行一次测试
        scheduler.run_parallel_scraping_and_sync()
    elif command == "--excellentnumbers":
        scheduler.run_single_task("excellentnumbers")
    elif command == "--numberbarn":
        scheduler.run_single_task("numberbarn")
    elif command == "--sync":
        scheduler.run_single_task("sync")
    else:
        print("用法:")
        print("  python main.py                    # 启动定时调度器")
        print("  python main.py --parallel         # 立即执行一次，然后定时执行") 
        print("  python main.py --test             # 只执行一次测试")
        print("  python main.py --excellentnumbers # 只执行excellentnumbers")
        print("  python main.py --numberbarn       # 只执行numberbarn")
        print("  python main.py --sync             # 只执行数据同步")


if __name__ == "__main__":
    main()