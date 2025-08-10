# NumHarvest Docker 运行指南

本文档介绍如何使用 Docker 运行 NumHarvest 项目。

## 🚀 快速开始

### 1. 环境要求
- Docker 20.10+
- Docker Compose 2.0+
- 至少 2GB 可用内存

### 2. 一键启动
```bash
# 构建并启动服务
./docker-run.sh build
./docker-run.sh start

# 或者合并命令
./docker-run.sh build && ./docker-run.sh start
```

### 3. 查看状态
```bash
# 查看容器状态
./docker-run.sh status

# 查看实时日志
./docker-run.sh logs -f
```

## 📁 文件结构

```
numharvest/
├── Dockerfile                    # Docker 镜像定义
├── docker-compose.yml           # 完整版（含数据库）
├── docker-compose.simple.yml    # 简化版（仅主应用）
├── docker-run.sh               # Docker 管理脚本
├── .dockerignore               # Docker 构建忽略文件
├── logs/                       # 日志目录（挂载）
├── data/                       # 数据目录（挂载）
└── docker/                     # 数据库初始化脚本
    ├── mongodb-init/
    └── postgresql-init/
```

## 🛠️ 管理命令

### 基本操作
```bash
# 构建镜像
./docker-run.sh build

# 启动服务
./docker-run.sh start

# 停止服务
./docker-run.sh stop

# 重启服务
./docker-run.sh restart

# 查看状态
./docker-run.sh status
```

### 日志管理
```bash
# 查看日志（最后100行）
./docker-run.sh logs

# 实时查看日志
./docker-run.sh logs -f

# 查看特定服务日志
./docker-run.sh logs numharvest
./docker-run.sh logs mongodb
```

### 容器管理
```bash
# 进入容器
./docker-run.sh shell

# 进入特定服务容器
./docker-run.sh shell numharvest
./docker-run.sh shell mongodb
```

### 数据管理
```bash
# 备份数据
./docker-run.sh backup

# 更新服务（含备份）
./docker-run.sh update

# 清理资源
./docker-run.sh cleanup
```

## 🔧 配置选项

### 运行模式选择

#### 1. 简化模式（推荐）
仅运行主应用，使用外部数据库：
```bash
# 使用简化配置
cp docker-compose.simple.yml docker-compose.yml
./docker-run.sh start
```

#### 2. 完整模式
包含本地数据库服务：
```bash
# 启动所有服务
./docker-run.sh start numharvest mongodb postgresql

# 或分别启动
./docker-run.sh start numharvest
./docker-run.sh start mongodb
./docker-run.sh start postgresql
```

### 环境变量配置

在 `docker-compose.yml` 中修改环境变量：
```yaml
environment:
  - TZ=Asia/Shanghai              # 时区
  - PYTHONUNBUFFERED=1           # Python 输出缓冲
  - MONGO_HOST=43.159.58.235     # MongoDB 地址（如需修改）
  - POSTGRES_HOST=43.159.58.235  # PostgreSQL 地址（如需修改）
```

### 资源限制调整

在 `docker-compose.yml` 中调整资源限制：
```yaml
deploy:
  resources:
    limits:
      memory: 2G        # 内存限制
      cpus: '1.0'       # CPU 限制
    reservations:
      memory: 512M      # 内存预留
      cpus: '0.5'       # CPU 预留
```

## 📊 监控和排错

### 健康检查
```bash
# 查看容器健康状态
docker ps

# 查看详细健康检查信息
docker inspect numharvest-app | grep -A 20 Health
```

### 日志分析
```bash
# 查看错误日志
./docker-run.sh logs | grep -i error

# 查看最近的日志
./docker-run.sh logs --tail=50

# 查看特定时间段的日志
docker logs numharvest-app --since="2024-01-01T00:00:00" --until="2024-01-02T00:00:00"
```

### 性能监控
```bash
# 查看资源使用情况
docker stats numharvest-app

# 查看容器进程
docker exec numharvest-app ps aux
```

## 🔒 安全配置

### 1. 非 root 用户
Dockerfile 中已配置非 root 用户运行：
```dockerfile
RUN useradd -m -u 1000 numharvest
USER numharvest
```

### 2. 网络隔离
使用专用网络：
```yaml
networks:
  numharvest-network:
    driver: bridge
```

### 3. 数据卷权限
确保宿主机目录权限正确：
```bash
# 设置目录权限
sudo chown -R 1000:1000 logs/ data/
chmod 755 logs/ data/
```

## 🚨 故障排除

### 常见问题

1. **端口冲突**
   ```bash
   # 检查端口占用
   sudo lsof -i :27017
   sudo lsof -i :5432
   ```

2. **磁盘空间不足**
   ```bash
   # 清理 Docker 资源
   docker system prune -f
   docker volume prune -f
   ```

3. **内存不足**
   ```bash
   # 调整内存限制
   # 编辑 docker-compose.yml 中的 memory 配置
   ```

4. **权限问题**
   ```bash
   # 修复文件权限
   sudo chown -R $(whoami):$(whoami) .
   ```

### 完全重置
```bash
# 停止所有服务
./docker-run.sh stop

# 清理所有资源
./docker-run.sh cleanup

# 重新构建和启动
./docker-run.sh build
./docker-run.sh start
```

## 📈 生产环境部署

### 1. 资源配置
```yaml
# 生产环境建议配置
deploy:
  resources:
    limits:
      memory: 4G
      cpus: '2.0'
    reservations:
      memory: 1G
      cpus: '1.0'
```

### 2. 日志轮转
```yaml
logging:
  driver: "json-file"
  options:
    max-size: "100m"
    max-file: "10"
```

### 3. 自动重启
```yaml
restart: unless-stopped
```

### 4. 健康检查
```yaml
healthcheck:
  interval: 30s
  timeout: 10s
  retries: 5
  start_period: 60s
```

## 📞 支持

如果遇到问题，请：
1. 查看容器日志：`./docker-run.sh logs -f`
2. 检查容器状态：`./docker-run.sh status`
3. 进入容器调试：`./docker-run.sh shell`
4. 查看系统资源：`docker stats`

---

**注意**：首次运行时会自动下载 Playwright 浏览器，可能需要几分钟时间。