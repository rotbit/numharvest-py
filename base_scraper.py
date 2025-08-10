import json
import time
from typing import Any, Dict, List

class BaseScraper:
    """简化的爬虫基类"""
    
    def __init__(self, base_url: str = ""):
        self.base_url = base_url
        self.data: List[Dict] = []
    
    def run(self) -> Any:
        """运行爬虫 - 子类重写此方法"""
        raise NotImplementedError("Subclasses must implement run method")
    
    def save_json(self, data: Any, filename: str) -> str:
        """保存数据为JSON文件"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"数据已保存到: {filename}")
            return filename
        except Exception as e:
            print(f"保存文件失败: {e}")
            raise