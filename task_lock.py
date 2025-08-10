#!/usr/bin/env python3
"""
任务锁管理器 - 防止任务重复执行的robust锁机制
"""
import os
import time
import fcntl
import signal
import psutil
from datetime import datetime, timedelta
from typing import Optional

class TaskLock:
    """任务锁管理器 - 跨进程、带超时的文件锁"""
    
    def __init__(self, 
                 lock_file: str = "numharvest_task.lock",
                 timeout_minutes: int = 120,  # 2小时超时
                 heartbeat_interval: int = 30):  # 30秒心跳间隔
        self.lock_file = lock_file
        self.timeout_seconds = timeout_minutes * 60
        self.heartbeat_interval = heartbeat_interval
        self.lock_fd: Optional[int] = None
        self.pid = os.getpid()
        
    def _write_lock_info(self):
        """写入锁信息"""
        lock_info = {
            'pid': self.pid,
            'start_time': datetime.now().isoformat(),
            'last_heartbeat': datetime.now().isoformat(),
            'timeout_seconds': self.timeout_seconds
        }
        
        try:
            with open(self.lock_file, 'w') as f:
                import json
                json.dump(lock_info, f, indent=2)
        except Exception:
            pass  # 忽略写入错误，锁仍然有效
    
    def _read_lock_info(self) -> Optional[dict]:
        """读取锁信息"""
        try:
            if not os.path.exists(self.lock_file):
                return None
                
            with open(self.lock_file, 'r') as f:
                import json
                return json.load(f)
        except Exception:
            return None
    
    def _is_process_running(self, pid: int) -> bool:
        """检查进程是否还在运行"""
        try:
            return psutil.pid_exists(pid)
        except Exception:
            # psutil不可用时的fallback
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False
    
    def _is_lock_expired(self, lock_info: dict) -> bool:
        """检查锁是否已过期"""
        try:
            start_time = datetime.fromisoformat(lock_info['start_time'])
            timeout = lock_info.get('timeout_seconds', self.timeout_seconds)
            
            if datetime.now() - start_time > timedelta(seconds=timeout):
                return True
                
            # 检查心跳
            last_heartbeat = datetime.fromisoformat(lock_info['last_heartbeat'])
            if datetime.now() - last_heartbeat > timedelta(seconds=self.heartbeat_interval * 2):
                return True
                
            return False
        except Exception:
            return True  # 解析错误时认为已过期
    
    def _cleanup_stale_lock(self) -> bool:
        """清理过期的锁"""
        lock_info = self._read_lock_info()
        if not lock_info:
            return True
            
        pid = lock_info.get('pid')
        if not pid:
            self._remove_lock_file()
            return True
            
        # 检查进程是否存在
        if not self._is_process_running(pid):
            self._remove_lock_file()
            return True
            
        # 检查是否过期
        if self._is_lock_expired(lock_info):
            self._remove_lock_file()
            return True
            
        return False
    
    def _remove_lock_file(self):
        """删除锁文件"""
        try:
            if os.path.exists(self.lock_file):
                os.unlink(self.lock_file)
        except Exception:
            pass
    
    def acquire(self) -> bool:
        """获取锁"""
        try:
            # 清理过期锁
            if not self._cleanup_stale_lock():
                return False  # 锁被其他活跃进程持有
            
            # 尝试创建锁文件
            self.lock_fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            
            # 写入锁信息
            self._write_lock_info()
            
            return True
            
        except FileExistsError:
            # 文件已存在，检查是否为过期锁
            if self._cleanup_stale_lock():
                # 重试一次
                try:
                    self.lock_fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                    self._write_lock_info()
                    return True
                except FileExistsError:
                    return False
            return False
        except Exception:
            return False
    
    def release(self):
        """释放锁"""
        try:
            if self.lock_fd is not None:
                os.close(self.lock_fd)
                self.lock_fd = None
            
            self._remove_lock_file()
        except Exception:
            pass
    
    def update_heartbeat(self):
        """更新心跳"""
        if self.lock_fd is not None:
            lock_info = self._read_lock_info()
            if lock_info and lock_info.get('pid') == self.pid:
                lock_info['last_heartbeat'] = datetime.now().isoformat()
                try:
                    with open(self.lock_file, 'w') as f:
                        import json
                        json.dump(lock_info, f, indent=2)
                except Exception:
                    pass
    
    def get_lock_status(self) -> dict:
        """获取锁状态信息"""
        lock_info = self._read_lock_info()
        if not lock_info:
            return {'locked': False, 'message': '无活跃锁'}
        
        pid = lock_info.get('pid')
        start_time = lock_info.get('start_time', '')
        
        if not self._is_process_running(pid):
            return {
                'locked': False, 
                'message': f'锁文件存在但进程 {pid} 已停止',
                'stale': True
            }
        
        if self._is_lock_expired(lock_info):
            return {
                'locked': False,
                'message': f'锁已过期 (PID: {pid}, 开始时间: {start_time})',
                'stale': True
            }
        
        return {
            'locked': True,
            'pid': pid,
            'start_time': start_time,
            'last_heartbeat': lock_info.get('last_heartbeat', ''),
            'message': f'任务正在运行 (PID: {pid})'
        }
    
    def __enter__(self):
        """上下文管理器入口"""
        if self.acquire():
            return self
        else:
            raise RuntimeError("无法获取任务锁，可能有其他任务正在运行")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.release()

class HeartbeatManager:
    """心跳管理器 - 定期更新锁的心跳"""
    
    def __init__(self, task_lock: TaskLock):
        self.task_lock = task_lock
        self.running = False
        
    def start(self):
        """启动心跳"""
        self.running = True
        
        def heartbeat_handler(signum, frame):
            if self.running:
                self.task_lock.update_heartbeat()
                signal.alarm(self.task_lock.heartbeat_interval)
        
        signal.signal(signal.SIGALRM, heartbeat_handler)
        signal.alarm(self.task_lock.heartbeat_interval)
    
    def stop(self):
        """停止心跳"""
        self.running = False
        signal.alarm(0)  # 取消定时器