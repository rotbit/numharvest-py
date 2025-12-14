#!/usr/bin/env python3
"""
主调度程序 - 定时执行任务并同步数据
"""
from __future__ import annotations

import logging
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List

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

        self.mongo_settings = MongoSettings()
        self.postgres_settings = PostgresSettings()
        self.scrape_timeout_seconds = 3600

    def _build_scrape_tasks(self) -> List[TaskDefinition]:
        """构建抓取任务列表。"""
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
        """统一的任务执行方法，按单任务独立锁互斥。"""
        lock = TaskLock(
            lock_file=f"numharvest_{task.key}.lock",
            timeout_minutes=120,
            heartbeat_interval=30,
        )
        lock_status = lock.get_lock_status()
        if lock_status["locked"]:
            self.logger.warning("任务[%s]已在运行，跳过本次执行: %s", task.label, lock_status["message"])
            return TaskResult(task.key, task.label, False, f"{task.label} 正在运行，跳过", None)

        start_time = datetime.now()
        self.logger.info("开始执行%s任务", task.label)

        try:
            with lock:
                heartbeat = HeartbeatManager(lock)
                heartbeat.start()
                try:
                    result = task.runner()
                    success = True
                    message = f"{task.label}任务成功完成"
                except Exception as exc:  # noqa: B902
                    success = False
                    result = None
                    message = f"执行{task.label}任务时出错: {exc}"
                    self.logger.error(message, exc_info=True)
                finally:
                    heartbeat.stop()
        except RuntimeError as exc:
            self.logger.warning("获取任务[%s]锁失败: %s", task.label, exc)
            return TaskResult(task.key, task.label, False, f"无法获取锁: {exc}", None)

        duration = (datetime.now() - start_time).total_seconds()
        if success:
            self.logger.info("%s任务完成，耗时: %.2f秒", task.label, duration)
        else:
            self.logger.error("%s任务失败，耗时: %.2f秒", task.label, duration)
        return TaskResult(task.key, task.label, success, message, result)

    def _run_tasks_in_parallel(self, tasks: List[TaskDefinition]) -> List[TaskResult]:
        """并行执行任务并收集结果，不做超时终止控制。"""
        if not tasks:
            return []

        results: List[TaskResult] = []
        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            future_map = {executor.submit(self._run_task, task): task for task in tasks}

            for future, task in future_map.items():
                try:
                    results.append(future.result())
                except Exception as exc:  # noqa: B902
                    self.logger.error("并行任务 %s 执行失败: %s", task.label, exc, exc_info=True)
                    results.append(TaskResult(task.key, task.label, False, f"任务执行失败: {exc}", None))

        return results

    def _execute_main_tasks(self) -> None:
        """并行执行抓取任务和数据同步任务（同一批次同时启动）。"""
        start_time = datetime.now()
        self.logger.info("开始并行执行：抓取 + 数据同步")

        tasks = self._build_scrape_tasks()
        tasks.append(self._build_sync_task())

        results = self._run_tasks_in_parallel(tasks)

        for result in results:
            status = "✅" if result.success else "❌"
            self.logger.info("%s 任务 %s 结果: %s", status, result.label, result.message)

        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info("本轮并行任务完成，总耗时: %.2f秒", duration)
    
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

    def run_scrapers_only(self) -> None:
        """仅并行抓取，不做同步。"""
        def _task_body() -> None:
            self.logger.info("开始仅抓取任务（excellentnumbers + numberbarn）")
            self._run_tasks_in_parallel(self._build_scrape_tasks())
        self._with_task_lock(_task_body)

    def run_scheduler(self) -> None:
        """简单循环：执行一轮抓取+同步，完成后sleep 10 分钟再执行。"""
        try:
            while True:
                self.logger.info("启动一轮抓取+同步")
                self._execute_main_tasks()
                self.logger.info("本轮结束，休眠600秒")
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("调度器停止（收到Ctrl+C）")

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
