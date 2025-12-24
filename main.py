#!/usr/bin/env python3
"""
主调度程序 - 多进程定时执行抓取与同步。

需求：
1) 每个爬虫任务单独进程，跑完后休眠5天再跑。
2) 同步任务单独进程，跑完后休眠1分钟再跑。
3) 不搞复杂参数，保证抓取和同步能长期跑即可。
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

from excellentnumberstask import AreaCodeNumbersHarvester
from mongo_to_postgresql_sync import MongoToPostgreSQLSync
from numberbarntask import NumberbarnNumberExtractor, NumberbarnTollFreeExtractor, NumberbarnGlobalExtractor
from settings import MongoSettings, PostgresSettings
from task_lock import HeartbeatManager, TaskLock

# ----- 调度参数 -----
SCRAPER_INTERVAL_SECONDS = 5 * 24 * 60 * 60  # 5天
SYNC_INTERVAL_SECONDS = 60 * 30  # 半小时
LOCK_TIMEOUT_MINUTES = 180

TASK_LABELS: Dict[str, str] = {
    "excellentnumbers": "excellentnumbers 爬虫",
    "numberbarn": "numberbarn 爬虫",
    "numberbarn_tollfree": "numberbarn tollfree 爬虫",
    "numberbarn_global": "numberbarn global 爬虫",
    "sync": "Mongo -> PostgreSQL 同步",
}


def configure_logging(level: int = logging.INFO) -> None:
    """为当前进程设置统一日志格式。"""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(processName)s] %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("numharvest_scheduler.log"), logging.StreamHandler()],
    )


@dataclass(frozen=True)
class TaskResult:
    key: str
    label: str
    success: bool
    message: str
    payload: Any = None


def _run_task_payload(task_key: str, mongo: MongoSettings, postgres: PostgresSettings) -> Any:
    """实际执行单个任务的主体，保持简单可pickle。"""
    if task_key == "excellentnumbers":
        return AreaCodeNumbersHarvester(
            mongo_host=mongo.host,
            mongo_user=mongo.user,
            mongo_password=mongo.password,
            mongo_port=mongo.port,
            mongo_db=mongo.db,
            mongo_collection=mongo.collection,
            headless=True,
        ).run(index_path_or_dir=".", limit=None, max_numbers=None)

    if task_key == "numberbarn":
        return NumberbarnNumberExtractor(
            mongo_host=mongo.host,
            mongo_password=mongo.password,
            mongo_db=mongo.db,
        ).run()

    if task_key == "numberbarn_tollfree":
        return NumberbarnTollFreeExtractor(
            mongo_host=mongo.host,
            mongo_db=mongo.db,
        ).run()

    if task_key == "numberbarn_global":
        return NumberbarnGlobalExtractor(
            mongo_host=mongo.host,
            mongo_db=mongo.db,
        ).run()

    if task_key == "sync":
        return MongoToPostgreSQLSync(
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
        ).run()

    raise ValueError(f"未知任务类型: {task_key}")


def run_task_once(
    task_key: str,
    mongo: MongoSettings,
    postgres: PostgresSettings,
    log: logging.Logger,
) -> TaskResult:
    """带锁执行一次任务。"""
    label = TASK_LABELS.get(task_key, task_key)
    lock = TaskLock(
        lock_file=f"numharvest_{task_key}.lock",
        timeout_minutes=LOCK_TIMEOUT_MINUTES,
        heartbeat_interval=30,
    )
    lock_status = lock.get_lock_status()
    if lock_status["locked"]:
        msg = f"{label} 已在运行，跳过本次执行"
        log.warning("%s: %s", label, lock_status.get("message", msg))
        return TaskResult(task_key, label, False, msg, None)

    start = datetime.now()
    success = False
    payload = None
    message = ""

    try:
        with lock:
            hb = HeartbeatManager(lock)
            hb.start()
            try:
                payload = _run_task_payload(task_key, mongo, postgres)
                success = True
                message = f"{label} 完成"
            finally:
                hb.stop()
    except RuntimeError as exc:
        message = f"无法获取锁: {exc}"
        log.warning("%s", message)
        return TaskResult(task_key, label, False, message, None)
    except Exception as exc:  # noqa: B902
        message = f"{label} 执行出错: {exc}"
        log.exception(message)
        success = False

    duration = (datetime.now() - start).total_seconds()
    message = f"{message}，耗时 {duration:.1f} 秒"
    if success:
        log.info("%s", message)
    else:
        log.error("%s", message)
    return TaskResult(task_key, label, success, message, payload)


def task_worker(
    task_key: str,
    interval_seconds: int,
    stop_event: mp.Event,
    mongo: MongoSettings,
    postgres: PostgresSettings,
) -> None:
    """子进程工作循环：执行一次，休眠固定时间，再重复。"""
    configure_logging()
    logger = logging.getLogger(f"worker.{task_key}")
    label = TASK_LABELS.get(task_key, task_key)
    logger.info("启动 %s 进程，间隔 %d 秒", label, interval_seconds)

    while not stop_event.is_set():
        result = run_task_once(task_key, mongo, postgres, logger)
        logger.info("%s 结果: %s", label, result.message)
        logger.info("%s 下一次执行将在 %d 秒后", label, interval_seconds)
        if stop_event.wait(interval_seconds):
            break

    logger.info("%s 收到停止信号，退出", label)


def main() -> None:
    configure_logging()
    mongo = MongoSettings()
    postgres = PostgresSettings()
    stop_event = mp.Event()

    if len(sys.argv) == 3 and sys.argv[1] == "--once":
        task_key = sys.argv[2]
        logger = logging.getLogger(f"once.{task_key}")
        result = run_task_once(task_key, mongo, postgres, logger)
        status = "✅" if result.success else "❌"
        print(f"{status} {result.label}: {result.message}")
        return

    if len(sys.argv) > 1:
        print("用法: python main.py            # 启动常驻进程")
        print("     python main.py --once <task>")
        return

    # 启动三个简单的子进程
    processes: Dict[str, mp.Process] = {}
    specs = {
        "excellentnumbers": SCRAPER_INTERVAL_SECONDS,
        "numberbarn": SCRAPER_INTERVAL_SECONDS,
        "numberbarn_tollfree": SCRAPER_INTERVAL_SECONDS,
        "numberbarn_global": SCRAPER_INTERVAL_SECONDS,
        "sync": SYNC_INTERVAL_SECONDS,
    }
    for key, interval in specs.items():
        p = mp.Process(
            target=task_worker,
            name=f"{key}_worker",
            args=(key, interval, stop_event, mongo, postgres),
            daemon=False,
        )
        p.start()
        processes[key] = p
        logging.getLogger("main").info("启动 %s (pid=%s)", key, p.pid)

    try:
        # 主进程保持阻塞，等待 Ctrl+C
        for p in processes.values():
            p.join()
    except KeyboardInterrupt:
        logging.getLogger("main").info("收到退出信号，准备关闭所有任务")
        stop_event.set()
        for p in processes.values():
            p.join(timeout=20)
    logging.getLogger("main").info("调度器已退出")


if __name__ == "__main__":
    # macOS / Windows 默认 spawn，Linux 使用 fork 即可；spawn 最安全可移植。
    try:
        mp.set_start_method("spawn", force=True)
    except RuntimeError:
        # start method 已设置时忽略
        pass
    main()
