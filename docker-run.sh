#!/bin/bash

# NumHarvest Docker 管理脚本
# 功能：构建、运行、管理 Docker 容器

set -e  # 遇到错误立即退出

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="numharvest"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
IMAGE_NAME="numharvest:latest"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# 检查 Docker 是否安装
check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker 未安装，请先安装 Docker"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        error "Docker Compose 未安装，请先安装 Docker Compose"
        exit 1
    fi
    
    # 检查 Docker 是否运行
    if ! docker info &> /dev/null; then
        error "Docker 服务未运行，请启动 Docker"
        exit 1
    fi
    
    log "Docker 环境检查通过"
}

# 创建必要的目录
create_directories() {
    log "创建必要的目录..."
    mkdir -p "$SCRIPT_DIR/logs"
    mkdir -p "$SCRIPT_DIR/data"
    mkdir -p "$SCRIPT_DIR/docker/mongodb-init"
    mkdir -p "$SCRIPT_DIR/docker/postgresql-init"
    
    # 创建数据库初始化脚本（如果不存在）
    if [ ! -f "$SCRIPT_DIR/docker/mongodb-init/01-init.js" ]; then
        cat > "$SCRIPT_DIR/docker/mongodb-init/01-init.js" << 'EOF'
// MongoDB 初始化脚本
db = db.getSiblingDB('extra_numbers');

// 创建用户
db.createUser({
    user: 'extra_numbers',
    pwd: 'RsBWd3hTAZeR7kC4',
    roles: [
        {
            role: 'readWrite',
            db: 'extra_numbers'
        }
    ]
});

// 创建集合和索引
db.createCollection('numbers');
db.numbers.createIndex({ "number": 1 }, { unique: true });
db.numbers.createIndex({ "region": 1, "area_code": 1 });

print('MongoDB 初始化完成');
EOF
    fi
    
    if [ ! -f "$SCRIPT_DIR/docker/postgresql-init/01-init.sql" ]; then
        cat > "$SCRIPT_DIR/docker/postgresql-init/01-init.sql" << 'EOF'
-- PostgreSQL 初始化脚本
CREATE TABLE IF NOT EXISTS phone_numbers (
    id SERIAL PRIMARY KEY,
    number VARCHAR(20) UNIQUE NOT NULL,
    price VARCHAR(20),
    region VARCHAR(100),
    area_code VARCHAR(10),
    page INTEGER,
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_phone_numbers_region_area ON phone_numbers(region, area_code);
CREATE INDEX IF NOT EXISTS idx_phone_numbers_created_at ON phone_numbers(created_at);

-- 插入测试数据（可选）
-- INSERT INTO phone_numbers (number, price, region, area_code) VALUES ('(555) 123-4567', '$10.00', 'Test Region', '555');
EOF
    fi
}

# 构建镜像
build_image() {
    log "构建 Docker 镜像..."
    cd "$SCRIPT_DIR"
    
    # 使用 docker-compose 构建
    if docker compose version &> /dev/null; then
        docker compose build --no-cache
    else
        docker-compose build --no-cache
    fi
    
    log "镜像构建完成"
}

# 检查服务是否已运行
check_running() {
    local service="${1:-numharvest}"
    local container_name="${PROJECT_NAME}-app"
    
    if [ "$service" != "numharvest" ]; then
        container_name="${PROJECT_NAME}-${service}"
    fi
    
    if docker ps --filter "name=$container_name" --filter "status=running" | grep -q "$container_name"; then
        return 0  # 正在运行
    else
        return 1  # 未运行
    fi
}

# 启动服务
start_services() {
    local services="${1:-numharvest}"
    
    # 检查是否已运行
    if check_running "$services"; then
        warn "服务 $services 已在运行"
        read -p "是否要重启服务？[Y/n]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            log "重启服务: $services"
            stop_services "$services"
            sleep 3
        else
            info "保持现有服务运行"
            show_status
            return 0
        fi
    fi
    
    log "启动服务: $services"
    cd "$SCRIPT_DIR"
    
    create_directories
    
    if docker compose version &> /dev/null; then
        docker compose up -d $services
    else
        docker-compose up -d $services
    fi
    
    log "服务启动完成"
    
    # 显示服务状态
    sleep 5
    show_status
}

# 停止服务
stop_services() {
    local services="${1:-}"
    
    if [ -n "$services" ]; then
        log "停止服务: $services"
        if docker compose version &> /dev/null; then
            docker compose stop $services
        else
            docker-compose stop $services
        fi
    else
        log "停止所有服务..."
        if docker compose version &> /dev/null; then
            docker compose down
        else
            docker-compose down
        fi
    fi
    
    log "服务已停止"
}

# 重启服务
restart_services() {
    local services="${1:-numharvest}"
    
    log "重启服务: $services"
    stop_services "$services"
    sleep 2
    start_services "$services"
}

# 查看服务状态
show_status() {
    log "服务状态:"
    
    if docker compose version &> /dev/null; then
        docker compose ps
    else
        docker-compose ps
    fi
    
    echo ""
    info "容器健康状态:"
    docker ps --filter "name=${PROJECT_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

# 查看日志
view_logs() {
    local service="${1:-numharvest}"
    local follow="${2:-}"
    
    cd "$SCRIPT_DIR"
    
    if [ "$follow" = "-f" ] || [ "$follow" = "follow" ]; then
        info "实时查看 $service 日志 (Ctrl+C 退出):"
        if docker compose version &> /dev/null; then
            docker compose logs -f "$service"
        else
            docker-compose logs -f "$service"
        fi
    else
        info "查看 $service 日志 (最后100行):"
        if docker compose version &> /dev/null; then
            docker compose logs --tail=100 "$service"
        else
            docker-compose logs --tail=100 "$service"
        fi
    fi
}

# 进入容器
enter_container() {
    local service="${1:-numharvest}"
    local container_name="${PROJECT_NAME}-${service}"
    
    if [ "$service" = "numharvest" ]; then
        container_name="${PROJECT_NAME}-app"
    fi
    
    log "进入容器: $container_name"
    docker exec -it "$container_name" /bin/bash || docker exec -it "$container_name" /bin/sh
}

# 清理资源
cleanup() {
    warn "清理所有相关资源..."
    
    # 停止并删除容器
    if docker compose version &> /dev/null; then
        docker compose down -v --remove-orphans
    else
        docker-compose down -v --remove-orphans
    fi
    
    # 删除镜像（可选）
    read -p "是否删除 Docker 镜像? [y/N]: " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker rmi "$IMAGE_NAME" 2>/dev/null || true
        docker image prune -f
    fi
    
    # 删除数据卷（可选）
    read -p "是否删除数据卷（会丢失数据）? [y/N]: " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker volume rm ${PROJECT_NAME}-cache ${PROJECT_NAME}-mongodb-data ${PROJECT_NAME}-postgresql-data 2>/dev/null || true
    fi
    
    log "清理完成"
}

# 备份数据
backup_data() {
    local backup_dir="$SCRIPT_DIR/backups/$(date +%Y%m%d_%H%M%S)"
    
    log "备份数据到: $backup_dir"
    mkdir -p "$backup_dir"
    
    # 备份日志
    if [ -d "$SCRIPT_DIR/logs" ]; then
        cp -r "$SCRIPT_DIR/logs" "$backup_dir/"
    fi
    
    # 备份数据
    if [ -d "$SCRIPT_DIR/data" ]; then
        cp -r "$SCRIPT_DIR/data" "$backup_dir/"
    fi
    
    # 备份数据库（如果本地运行）
    if docker ps --filter "name=${PROJECT_NAME}-mongodb" --filter "status=running" | grep -q mongodb; then
        log "备份 MongoDB 数据..."
        docker exec "${PROJECT_NAME}-mongodb" mongodump --db extra_numbers --out /tmp/backup
        docker cp "${PROJECT_NAME}-mongodb:/tmp/backup" "$backup_dir/mongodb-backup"
    fi
    
    if docker ps --filter "name=${PROJECT_NAME}-postgresql" --filter "status=running" | grep -q postgresql; then
        log "备份 PostgreSQL 数据..."
        docker exec "${PROJECT_NAME}-postgresql" pg_dump -U postgres numbers > "$backup_dir/postgresql-backup.sql"
    fi
    
    log "备份完成: $backup_dir"
}

# 更新服务
update() {
    log "更新服务..."
    
    # 备份数据
    backup_data
    
    # 重新构建并启动
    build_image
    restart_services
    
    log "更新完成"
}

# 显示帮助信息
show_help() {
    echo "NumHarvest Docker 管理脚本"
    echo ""
    echo "用法: $0 {command} [options]"
    echo ""
    echo "基本命令:"
    echo "  build              构建 Docker 镜像"
    echo "  start [service]    启动服务 (默认: numharvest)"
    echo "  stop [service]     停止服务 (空=停止所有)"
    echo "  restart [service]  重启服务 (默认: numharvest)"
    echo "  status             查看服务状态"
    echo ""
    echo "管理命令:"
    echo "  logs [service] [-f]  查看日志 (-f=实时查看)"
    echo "  shell [service]      进入容器 (默认: numharvest)"
    echo "  task-status         检查任务运行状态"
    echo "  unlock              强制解锁卡死的任务"
    echo "  task-test           执行一次测试任务"
    echo "  update              更新服务"
    echo "  cleanup             清理资源"
    echo "  backup              备份数据"
    echo ""
    echo "服务选项:"
    echo "  numharvest         主应用"
    echo "  mongodb            MongoDB 数据库"
    echo "  postgresql         PostgreSQL 数据库"
    echo ""
    echo "示例:"
    echo "  $0 start                    # 启动主应用"
    echo "  $0 start mongodb            # 启动 MongoDB"
    echo "  $0 logs numharvest -f       # 实时查看应用日志"
    echo "  $0 shell numharvest         # 进入应用容器"
    echo ""
    echo "文件位置:"
    echo "  项目目录: $SCRIPT_DIR"
    echo "  日志目录: $SCRIPT_DIR/logs"
    echo "  数据目录: $SCRIPT_DIR/data"
}

# 主函数
main() {
    # 切换到脚本目录
    cd "$SCRIPT_DIR"
    
    case "${1:-}" in
        build)
            check_docker
            build_image
            ;;
        start)
            check_docker
            start_services "${2:-numharvest}"
            ;;
        stop)
            stop_services "$2"
            ;;
        restart)
            check_docker
            restart_services "${2:-numharvest}"
            ;;
        status)
            show_status
            ;;
        logs)
            view_logs "$2" "$3"
            ;;
        shell|exec|bash)
            enter_container "$2"
            ;;
        task-status)
            info "检查任务状态..."
            docker exec "${PROJECT_NAME}-app" python main.py --status 2>/dev/null || error "无法获取任务状态，容器可能未运行"
            ;;
        unlock)
            info "强制解锁任务..."
            docker exec "${PROJECT_NAME}-app" python main.py --unlock 2>/dev/null || error "无法解锁，容器可能未运行"
            ;;
        task-test)
            info "执行一次测试任务..."
            docker exec "${PROJECT_NAME}-app" python main.py --test 2>/dev/null || error "无法执行测试任务，容器可能未运行"
            ;;
        update)
            check_docker
            update
            ;;
        cleanup|clean)
            cleanup
            ;;
        backup)
            backup_data
            ;;
        help|--help|-h)
            show_help
            ;;
        "")
            show_help
            ;;
        *)
            error "未知命令: $1"
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"