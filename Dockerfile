# NumHarvest Docker Image
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libdrm2 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
    libu2f-udev \
    libvulkan1 \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 升级pip并安装Python依赖
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 设置Playwright环境变量
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 安装 Playwright 浏览器 (在root用户下)
RUN playwright install chromium && \
    playwright install-deps chromium && \
    playwright install chromium --force

# 验证Playwright安装
RUN python -c "from playwright.sync_api import sync_playwright; print('Playwright安装成功')" && \
    echo "浏览器文件:" && \
    find /ms-playwright -name "*chromium*" | head -5

# 复制应用代码
COPY . .

# 创建非root用户并设置权限
RUN useradd -m -u 1000 numharvest && \
    mkdir -p /app/logs && \
    chown -R numharvest:numharvest /app

# 切换到非root用户
USER numharvest

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 默认命令
CMD ["python", "main.py"]
