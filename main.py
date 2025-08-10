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
from task_lock import TaskLock, HeartbeatManager

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
        
        # 任务锁配置 (2小时超时，30秒心跳)
        self.task_lock = TaskLock(
            lock_file="numharvest_task.lock",
            timeout_minutes=120,
            heartbeat_interval=30
        )
        
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
        # 检查锁状态
        lock_status = self.task_lock.get_lock_status()
        if lock_status['locked']:
            self.logger.warning(f"任务已在运行，跳过本次任务: {lock_status['message']}")
            return
        
        # 使用文件锁确保任务不重复执行
        try:
            with self.task_lock:
                self.logger.info(f"获取任务锁成功 (PID: {os.getpid()})")
                
                # 启动心跳管理器
                heartbeat = HeartbeatManager(self.task_lock)
                heartbeat.start()
                
                try:
                    self._execute_main_tasks()
                finally:
                    heartbeat.stop()
                    
        except RuntimeError as e:
            # 锁获取失败
            lock_status = self.task_lock.get_lock_status()
            self.logger.warning(f"无法获取任务锁: {lock_status.get('message', str(e))}")
        except Exception as e:
            self.logger.error(f"任务执行过程中发生未预期错误: {e}", exc_info=True)
    
    def _execute_main_tasks(self):
        """执行主要任务逻辑"""
        start_time = datetime.now()
        self.logger.info("开始执行数据抓取和同步任务")
        
        try:
            # 创建任务实例
            self.logger.info("初始化任务实例...")
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
            self.logger.info("开始并行执行抓取任务...")
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(self._run_task, "excellentnumbers", 
                                   excellentnumbers_harvester.run, ".", None),
                    executor.submit(self._run_task, "numberbarn", 
                                   numberbarn_extractor.run)
                ]
                
                # 等待所有任务完成
                results = []
                for i, future in enumerate(futures):
                    try:
                        result = future.result(timeout=3600)  # 1小时超时
                        results.append(result)
                    except Exception as e:
                        self.logger.error(f"并行任务 {i} 执行失败: {e}")
                        results.append((False, f"任务执行超时或失败: {e}", None))
                
                # 检查所有任务是否成功
                if all(result[0] for result in results):
                    self.logger.info("所有抓取任务完成，开始数据同步")
                    
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
                    if sync_result[0]:
                        self.logger.info(f"✅ 所有任务成功完成，总耗时: {duration:.2f}秒")
                    else:
                        self.logger.error(f"❌ 数据同步失败，总耗时: {duration:.2f}秒")
                else:
                    failed_count = sum(1 for result in results if not result[0])
                    self.logger.error(f"❌ {failed_count} 个抓取任务失败，跳过数据同步")
                    
        except Exception as e:
            self.logger.error(f"任务执行过程中出现错误: {e}", exc_info=True)
            raise
            
    def setup_schedule(self):
        """设置定时任务调度"""
        schedule.every().day.at("08:00").do(self.run_parallel_scraping_and_sync)
        self.logger.info("定时任务调度设置完成：每天8点执行")
        
        # 创建健康检查文件
        import os
        with open("/tmp/healthcheck", "w") as f:
            f.write("healthy")
        
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
        # 检查锁状态
        lock_status = self.task_lock.get_lock_status()
        if lock_status['locked']:
            self.logger.warning(f"任务已在运行，跳过本次任务: {lock_status['message']}")
            return
        
        # 使用文件锁确保任务不重复执行
        try:
            with self.task_lock:
                self.logger.info(f"获取任务锁成功，开始执行{task_type}任务 (PID: {os.getpid()})")
                
                # 启动心跳管理器
                heartbeat = HeartbeatManager(self.task_lock)
                heartbeat.start()
                
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
                    else:
                        result = (False, f"未知任务类型: {task_type}", None)
                        
                    status = "✅" if result[0] else "❌"
                    self.logger.info(f"{status} 单独执行{task_type}任务结果: {result[1]}")
                    
                finally:
                    heartbeat.stop()
                    
        except RuntimeError as e:
            lock_status = self.task_lock.get_lock_status()
            self.logger.warning(f"无法获取任务锁: {lock_status.get('message', str(e))}")
        except Exception as e:
            self.logger.error(f"执行{task_type}任务时出现未预期错误: {e}", exc_info=True)
    
    def get_task_status(self):
        """获取任务状态"""
        lock_status = self.task_lock.get_lock_status()
        
        if lock_status['locked']:
            self.logger.info(f"📍 {lock_status['message']}")
            self.logger.info(f"   开始时间: {lock_status.get('start_time', '未知')}")
            self.logger.info(f"   最后心跳: {lock_status.get('last_heartbeat', '未知')}")
        else:
            self.logger.info("📍 当前没有任务在运行")
            if lock_status.get('stale'):
                self.logger.info(f"   发现过期锁: {lock_status['message']}")
        
        return lock_status
    
    def force_unlock(self):
        """强制解锁（用于清理卡死的任务）"""
        lock_status = self.task_lock.get_lock_status()
        
        if not lock_status['locked']:
            self.logger.info("📍 当前没有活跃的锁")
            return True
        
        self.logger.warning(f"⚠️ 强制清理任务锁: {lock_status['message']}")
        
        try:
            # 删除锁文件
            if os.path.exists(self.task_lock.lock_file):
                os.unlink(self.task_lock.lock_file)
                self.logger.info("✅ 锁文件已删除")
                return True
        except Exception as e:
            self.logger.error(f"❌ 删除锁文件失败: {e}")
            return False


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
    elif command == "--status":
        # 查看任务状态
        scheduler.get_task_status()
    elif command == "--unlock":
        # 强制解锁
        scheduler.force_unlock()
    elif command == "--help" or command == "-h":
        print("NumHarvest 任务调度器")
        print("")
        print("用法:")
        print("  python main.py                    # 启动定时调度器")
        print("  python main.py --parallel         # 立即执行一次，然后定时执行") 
        print("  python main.py --test             # 只执行一次测试")
        print("  python main.py --excellentnumbers # 只执行excellentnumbers")
        print("  python main.py --numberbarn       # 只执行numberbarn")
        print("  python main.py --sync             # 只执行数据同步")
        print("  python main.py --status           # 查看任务状态")
        print("  python main.py --unlock           # 强制解锁卡死的任务")
        print("")
        print("任务安全机制:")
        print("  - 使用文件锁防止重复执行")
        print("  - 任务超时时间: 2小时")
        print("  - 心跳检测间隔: 30秒")
        print("  - 支持跨进程互斥")
    else:
        print("未知命令:", command)
        print("使用 'python main.py --help' 查看帮助")


if __name__ == "__main__":
    main()