#!/bin/bash

# NumHarvest Docker 完全卸载和清理脚本
# 功能：安全地卸载所有相关容器、镜像、网络和数据卷

set -e

# 配置
PROJECT_NAME="numharvest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
warn() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
info() { echo -e "${BLUE}[INFO]${NC} $1"; }

# 检查 Docker 是否运行
check_docker() {
    if ! docker info &> /dev/null; then
        error "Docker 服务未运行，请启动 Docker"
        exit 1
    fi
}

# 显示当前状态
show_current_status() {
    log "当前 NumHarvest 相关资源状态："
    echo ""
    
    info "=== 容器状态 ==="
    docker ps -a --filter "name=${PROJECT_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.CreatedAt}}" || echo "无相关容器"
    echo ""
    
    info "=== 镜像状态 ==="
    docker images --filter "reference=${PROJECT_NAME}*" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}" || echo "无相关镜像"
    echo ""
    
    info "=== 网络状态 ==="
    docker network ls --filter "name=${PROJECT_NAME}" --format "table {{.Name}}\t{{.Driver}}\t{{.Scope}}" || echo "无相关网络"
    echo ""
    
    info "=== 数据卷状态 ==="
    docker volume ls --filter "name=${PROJECT_NAME}" --format "table {{.Name}}\t{{.Driver}}\t{{.Mountpoint}}" || echo "无相关数据卷"
    echo ""
}

# 停止所有相关容器
stop_containers() {
    log "停止所有 NumHarvest 容器..."
    
    local containers=$(docker ps -a --filter "name=${PROJECT_NAME}" --format "{{.Names}}" 2>/dev/null || true)
    
    if [ -z "$containers" ]; then
        info "没有找到相关容器"
        return 0
    fi
    
    for container in $containers; do
        info "停止容器: $container"
        docker stop "$container" 2>/dev/null || warn "容器 $container 可能已经停止"
        docker rm "$container" 2>/dev/null || warn "无法删除容器 $container"
    done
    
    log "容器清理完成"
}

# 删除相关镜像
remove_images() {
    log "删除 NumHarvest 相关镜像..."
    
    local images=$(docker images --filter "reference=${PROJECT_NAME}*" --format "{{.Repository}}:{{.Tag}}" 2>/dev/null || true)
    
    if [ -z "$images" ]; then
        info "没有找到相关镜像"
        return 0
    fi
    
    for image in $images; do
        info "删除镜像: $image"
        docker rmi "$image" 2>/dev/null || warn "无法删除镜像 $image（可能被其他容器使用）"
    done
    
    # 清理悬空镜像
    info "清理悬空镜像..."
    docker image prune -f 2>/dev/null || true
    
    log "镜像清理完成"
}

# 删除相关网络
remove_networks() {
    log "删除 NumHarvest 相关网络..."
    
    local networks=$(docker network ls --filter "name=${PROJECT_NAME}" --format "{{.Name}}" 2>/dev/null || true)
    
    if [ -z "$networks" ]; then
        info "没有找到相关网络"
        return 0
    fi
    
    for network in $networks; do
        info "删除网络: $network"
        docker network rm "$network" 2>/dev/null || warn "无法删除网络 $network（可能被使用中）"
    done
    
    log "网络清理完成"
}

# 删除相关数据卷
remove_volumes() {
    local keep_data="$1"
    
    if [ "$keep_data" = "keep" ]; then
        warn "保留数据卷（根据用户选择）"
        return 0
    fi
    
    log "删除 NumHarvest 相关数据卷..."
    
    local volumes=$(docker volume ls --filter "name=${PROJECT_NAME}" --format "{{.Name}}" 2>/dev/null || true)
    
    if [ -z "$volumes" ]; then
        info "没有找到相关数据卷"
        return 0
    fi
    
    for volume in $volumes; do
        info "删除数据卷: $volume"
        docker volume rm "$volume" 2>/dev/null || warn "无法删除数据卷 $volume（可能被使用中）"
    done
    
    log "数据卷清理完成"
}

# 清理本地文件
clean_local_files() {
    local keep_data="$1"
    
    if [ "$keep_data" = "keep" ]; then
        warn "保留本地数据文件（根据用户选择）"
        return 0
    fi
    
    log "清理本地数据文件..."
    
    cd "$SCRIPT_DIR"
    
    # 清理日志文件
    if [ -d "logs" ]; then
        info "清理日志目录..."
        rm -rf logs/*
    fi
    
    # 清理数据文件
    if [ -d "data" ]; then
        info "清理数据目录..."
        rm -rf data/*
    fi
    
    # 清理备份文件
    if [ -d "backups" ]; then
        info "清理备份目录..."
        rm -rf backups
    fi
    
    # 清理PID文件
    rm -f *.pid *.log
    
    log "本地文件清理完成"
}

# 完全卸载
complete_uninstall() {
    local keep_data="$1"
    
    warn "开始完全卸载 NumHarvest Docker 环境..."
    echo ""
    
    show_current_status
    
    if [ "$keep_data" != "force" ]; then
        read -p "确认要完全卸载吗？这将删除所有容器、镜像和网络 [y/N]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            info "取消卸载操作"
            exit 0
        fi
        
        if [ "$keep_data" != "keep" ]; then
            echo ""
            read -p "是否保留数据卷和本地数据文件？[Y/n]: " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                keep_data="keep"
            fi
        fi
    fi
    
    echo ""
    log "开始卸载过程..."
    
    # 执行清理步骤
    stop_containers
    remove_images
    remove_networks
    remove_volumes "$keep_data"
    clean_local_files "$keep_data"
    
    # 额外清理
    info "执行额外清理..."
    docker system prune -f 2>/dev/null || true
    
    echo ""
    log "✅ NumHarvest 完全卸载完成！"
    
    if [ "$keep_data" = "keep" ]; then
        info "数据文件已保留在 logs/ 和 data/ 目录中"
    fi
}

# 安全重启（解决重复运行问题）
safe_restart() {
    log "执行安全重启..."
    
    # 先停止现有容器
    stop_containers
    
    # 等待一下确保容器完全停止
    sleep 3
    
    # 启动新容器
    info "启动新容器..."
    cd "$SCRIPT_DIR"
    
    if [ -f "./docker-run.sh" ]; then
        ./docker-run.sh start
    else
        error "找不到 docker-run.sh 脚本"
        exit 1
    fi
    
    log "✅ 安全重启完成！"
}

# 检查重复运行
check_duplicate_running() {
    local running_containers=$(docker ps --filter "name=${PROJECT_NAME}" --format "{{.Names}}" 2>/dev/null || true)
    
    if [ -n "$running_containers" ]; then
        warn "发现正在运行的 NumHarvest 容器："
        echo "$running_containers"
        echo ""
        
        read -p "是否要停止现有容器并重新启动？[Y/n]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            safe_restart
        else
            info "保持现有容器运行"
        fi
    else
        info "没有发现重复运行的容器"
    fi
}

# 显示帮助
show_help() {
    echo "NumHarvest Docker 清理和卸载脚本"
    echo ""
    echo "用法: $0 {command} [options]"
    echo ""
    echo "清理命令:"
    echo "  status              显示当前状态"
    echo "  check               检查重复运行问题"
    echo "  restart             安全重启（解决重复运行）"
    echo "  stop                仅停止和删除容器"
    echo "  clean               清理镜像和网络（保留数据）"
    echo "  uninstall           完全卸载（交互式）"
    echo "  uninstall-keep      完全卸载但保留数据"
    echo "  uninstall-force     强制完全卸载（无确认）"
    echo ""
    echo "示例:"
    echo "  $0 status           # 查看当前状态"
    echo "  $0 check            # 检查重复运行"
    echo "  $0 restart          # 安全重启"
    echo "  $0 uninstall        # 交互式完全卸载"
    echo ""
    echo "注意："
    echo "  - uninstall 会删除所有容器、镜像、网络和数据"
    echo "  - uninstall-keep 会保留数据卷和本地文件"
    echo "  - 建议在卸载前先执行 backup 备份数据"
}

# 主函数
main() {
    check_docker
    
    case "${1:-}" in
        status)
            show_current_status
            ;;
        check)
            check_duplicate_running
            ;;
        restart)
            safe_restart
            ;;
        stop)
            stop_containers
            ;;
        clean)
            stop_containers
            remove_images
            remove_networks
            ;;
        uninstall)
            complete_uninstall
            ;;
        uninstall-keep)
            complete_uninstall "keep"
            ;;
        uninstall-force)
            complete_uninstall "force"
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