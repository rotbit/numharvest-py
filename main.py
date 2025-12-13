#!/usr/bin/env python3
"""
主调度程序 - 定时执行任务并同步数据
"""
from __future__ import annotations

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List

import schedule

from excellentnumberstask import AreaCodeNumbersHarvester
from mongo_to_postgresql_sync import MongoToPostgreSQLSync
from numberbarntask import NumberbarnNumberExtractor
from settings import MongoSettings, PostgresSettings
from task_lock import HeartbeatManager, TaskLock

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("numharvest_scheduler.log"), logging.StreamHandler()],
)


@dataclass(frozen=True)
class TaskDefinition:
    key: str
    label: str
    runner: Callable[[], Any]
    timeout_seconds: int = 3600


@dataclass(frozen=True)
class TaskResult:
    key: str
    label: str
    success: bool
    message: str
    payload: Any = None


class NumberHarvestScheduler:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

        # 任务锁配置 (2小时超时，30秒心跳)
        self.task_lock = TaskLock(
            lock_file="numharvest_task.lock", timeout_minutes=120, heartbeat_interval=30
        )

        self.mongo_settings = MongoSettings()
        self.postgres_settings = PostgresSettings()
        self.scrape_timeout_seconds = 3600

    def _build_scrape_tasks(self) -> List[TaskDefinition]:
        """构建两个抓取任务的定义。"""
        mongo = self.mongo_settings
        return [
            TaskDefinition(
                key="excellentnumbers",
                label="excellentnumbers",
                runner=lambda: AreaCodeNumbersHarvester(
                    mongo_host=mongo.host,
                    mongo_user=mongo.user,
                    mongo_password=mongo.password,
                    mongo_port=mongo.port,
                    mongo_db=mongo.db,
                    mongo_collection=mongo.collection,
                    headless=True,
                ).run(".", None),
                timeout_seconds=self.scrape_timeout_seconds,
            ),
            TaskDefinition(
                key="numberbarn",
                label="numberbarn",
                runner=lambda: NumberbarnNumberExtractor(
                    mongo_host=mongo.host,
                    mongo_password=mongo.password,
                    mongo_db=mongo.db,
                ).run(),
                timeout_seconds=self.scrape_timeout_seconds,
            ),
        ]

    def _build_sync_task(self) -> TaskDefinition:
        """构建数据同步任务。"""
        mongo = self.mongo_settings
        postgres = self.postgres_settings
        return TaskDefinition(
            key="sync",
            label="数据同步",
            runner=lambda: MongoToPostgreSQLSync(
                mongo_host=mongo.host,
                mongo_user=mongo.user,
                mongo_password=mongo.password,
                mongo_port=mongo.port,
                mongo_db=mongo.db,
                postgres_host=postgres.host,
                postgres_port=postgres.port,
                postgres_db=postgres.db,
                postgres_user=postgres.user,
                postgres_password=postgres.password,
                batch_size=1000,
                dry_run=False,
            ).run(),
            timeout_seconds=self.scrape_timeout_seconds,
        )

    def _task_map(self) -> Dict[str, TaskDefinition]:
        tasks = {task.key: task for task in self._build_scrape_tasks()}
        sync_task = self._build_sync_task()
        tasks[sync_task.key] = sync_task
        return tasks

    def _run_task(self, task: TaskDefinition) -> TaskResult:
        """统一的任务执行方法。"""
        start_time = datetime.now()
        self.logger.info("开始执行%s任务", task.label)

        try:
            result = task.runner()
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info("%s任务完成，耗时: %.2f秒", task.label, duration)
            return TaskResult(task.key, task.label, True, f"{task.label}任务成功完成", result)
        except Exception as exc:  # noqa: B902
            self.logger.error("执行%s任务时出错: %s", task.label, exc, exc_info=True)
            return TaskResult(task.key, task.label, False, f"执行{task.label}任务时出错: {exc}", None)

    def _run_tasks_in_parallel(self, tasks: List[TaskDefinition]) -> List[TaskResult]:
        """并行执行任务并收集结果。"""
        if not tasks:
            return []

        results: List[TaskResult] = []
        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            future_map = {executor.submit(self._run_task, task): task for task in tasks}

            for future, task in future_map.items():
                try:
                    results.append(future.result(timeout=task.timeout_seconds))
                except FuturesTimeout as exc:
                    self.logger.error("并行任务 %s 超时: %s", task.label, exc)
                    results.append(TaskResult(task.key, task.label, False, f"任务执行超时: {exc}", None))
                except Exception as exc:  # noqa: B902
                    self.logger.error("并行任务 %s 执行失败: %s", task.label, exc)
                    results.append(TaskResult(task.key, task.label, False, f"任务执行失败: {exc}", None))

        return results

    def _with_task_lock(self, action: Callable[[], None]) -> bool:
        """获取锁并执行任务体，处理心跳和异常。"""
        lock_status = self.task_lock.get_lock_status()
        if lock_status["locked"]:
            self.logger.warning("任务已在运行，跳过本次任务: %s", lock_status["message"])
            return False

        try:
            with self.task_lock:
                self.logger.info("获取任务锁成功 (PID: %s)", os.getpid())

                heartbeat = HeartbeatManager(self.task_lock)
                heartbeat.start()

                try:
                    action()
                    return True
                finally:
                    heartbeat.stop()

        except RuntimeError as exc:
            lock_status = self.task_lock.get_lock_status()
            self.logger.warning("无法获取任务锁: %s", lock_status.get("message", str(exc)))
        except Exception as exc:  # noqa: B902
            self.logger.error("任务执行过程中发生未预期错误: %s", exc, exc_info=True)

        return False

    def _execute_main_tasks(self) -> None:
        """执行抓取+同步的主逻辑。"""
        start_time = datetime.now()
        self.logger.info("开始执行数据抓取和同步任务")

        scrape_results = self._run_tasks_in_parallel(self._build_scrape_tasks())
        success_count = sum(1 for result in scrape_results if result.success)
        failed_count = len(scrape_results) - success_count

        for result in scrape_results:
            status = "✅" if result.success else "❌"
            self.logger.info("%s 任务 %s 结果: %s", status, result.label, result.message)

        if success_count > 0:
            self.logger.info("有 %d 个抓取任务成功，%d 个失败，开始数据同步", success_count, failed_count)

            sync_result = self._run_task(self._build_sync_task())
            duration = (datetime.now() - start_time).total_seconds()
            if sync_result.success:
                self.logger.info("✅ 数据同步成功完成，总耗时: %.2f秒", duration)
            else:
                self.logger.error("❌ 数据同步失败，总耗时: %.2f秒", duration)
        else:
            self.logger.error("❌ 所有 %d 个抓取任务均失败，跳过数据同步", len(scrape_results))
    
    def run_test_flow(self, max_numbers: int = 10) -> None:
        """测试流程：先excellentnumbers抓10条，再numberbarn抓10条，最后同步。"""
        def _task_body() -> None:
            mongo = self.mongo_settings
            self.logger.info("开始测试抓取 excellentnumbers (最多 %d 条)", max_numbers)
            excel_result = AreaCodeNumbersHarvester(
                mongo_host=mongo.host,
                mongo_user=mongo.user,
                mongo_password=mongo.password,
                mongo_port=mongo.port,
                mongo_db=mongo.db,
                mongo_collection=mongo.collection,
                headless=True,
            ).run(max_numbers=max_numbers)
            self.logger.info("excellentnumbers 抓取完成: %s", excel_result)

            self.logger.info("开始测试抓取 numberbarn (最多 %d 条)", max_numbers)
            nb_result = NumberbarnNumberExtractor(
                mongo_host=mongo.host,
                mongo_password=mongo.password,
                mongo_db=mongo.db,
            ).run(max_numbers=max_numbers)
            self.logger.info("numberbarn 抓取完成，数量: %d", len(nb_result) if nb_result else 0)

            self.logger.info("开始执行数据同步")
            self._run_task(self._build_sync_task())

        self._with_task_lock(_task_body)

    def run_parallel_scraping_and_sync(self) -> None:
        """并行执行抓取任务，完成后同步数据。"""
        self._with_task_lock(self._execute_main_tasks)

    def setup_schedule(self) -> None:
        """设置定时任务调度。"""
        schedule.every().day.at("08:00").do(self.run_parallel_scraping_and_sync)
        self.logger.info("定时任务调度设置完成：每天8点执行")

        # 创建健康检查文件
        with open("/tmp/healthcheck", "w") as health_file:
            health_file.write("healthy")

    def run_scheduler(self) -> None:
        """运行调度器主循环。"""
        self.setup_schedule()
        self.logger.info("数字收获调度器启动")

        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("调度器停止")

    def run_single_task(self, task_type: str) -> None:
        """执行单个任务。"""

        def _task_body() -> None:
            task = self._task_map().get(task_type)
            if not task:
                self.logger.error("未知任务类型: %s", task_type)
                return

            result = self._run_task(task)
            status = "✅" if result.success else "❌"
            self.logger.info("%s 单独执行%s任务结果: %s", status, task_type, result.message)

        self._with_task_lock(_task_body)

    def get_task_status(self) -> Dict[str, Any]:
        """获取任务状态。"""
        lock_status = self.task_lock.get_lock_status()

        if lock_status["locked"]:
            self.logger.info("📍 %s", lock_status["message"])
            self.logger.info("   开始时间: %s", lock_status.get("start_time", "未知"))
            self.logger.info("   最后心跳: %s", lock_status.get("last_heartbeat", "未知"))
        else:
            self.logger.info("📍 当前没有任务在运行")
            if lock_status.get("stale"):
                self.logger.info("   发现过期锁: %s", lock_status["message"])

        return lock_status

    def force_unlock(self) -> bool:
        """强制解锁（用于清理卡死的任务）。"""
        lock_status = self.task_lock.get_lock_status()

        if not lock_status["locked"]:
            self.logger.info("📍 当前没有活跃的锁")
            return True

        self.logger.warning("⚠️ 强制清理任务锁: %s", lock_status["message"])

        try:
            if os.path.exists(self.task_lock.lock_file):
                os.unlink(self.task_lock.lock_file)
                self.logger.info("✅ 锁文件已删除")
                return True
        except Exception as exc:  # noqa: B902
            self.logger.error("❌ 删除锁文件失败: %s", exc)
            return False
        return False


def main() -> None:
    """主函数。"""
    scheduler = NumberHarvestScheduler()

    if len(sys.argv) == 1:
        scheduler.run_scheduler()
        return

    command = sys.argv[1]

    if command == "--parallel":
        scheduler.logger.info("立即执行一次并行任务，然后启动定时调度器")
        scheduler.run_parallel_scraping_and_sync()
        scheduler.run_scheduler()
    elif command == "--test":
        scheduler.run_test_flow(max_numbers=1)
    elif command == "--excellentnumbers":
        scheduler.run_single_task("excellentnumbers")
    elif command == "--numberbarn":
        scheduler.run_single_task("numberbarn")
    elif command == "--sync":
        scheduler.run_single_task("sync")
    elif command == "--status":
        scheduler.get_task_status()
    elif command == "--unlock":
        scheduler.force_unlock()
    elif command in ("--help", "-h"):
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
