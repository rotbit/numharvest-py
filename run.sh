#!/bin/bash

# NumHarvest 后台运行脚本
# 功能：自动安装依赖、管理进程、后台运行

set -e  # 遇到错误立即退出

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_SCRIPT="main.py"
PID_FILE="$SCRIPT_DIR/numharvest.pid"
LOG_FILE="$SCRIPT_DIR/numharvest.log"
VENV_DIR="$SCRIPT_DIR/venv"
REQUIREMENTS_FILE="$SCRIPT_DIR/requirements.txt"

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

# 检查Python是否安装
check_python() {
    if ! command -v python3 &> /dev/null; then
        error "Python3 未安装，请先安装 Python3"
        exit 1
    fi
    log "Python3 已安装: $(python3 --version)"
}

# 检查并创建虚拟环境
setup_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        log "创建虚拟环境..."
        python3 -m venv "$VENV_DIR"
    fi
    
    log "激活虚拟环境..."
    source "$VENV_DIR/bin/activate"
}

# 安装依赖
install_dependencies() {
    log "检查并安装依赖..."
    
    if [ -f "$REQUIREMENTS_FILE" ]; then
        log "从 requirements.txt 安装依赖..."
        pip install -r "$REQUIREMENTS_FILE"
    else
        warn "未找到 requirements.txt，安装基本依赖..."
        pip install pymongo playwright schedule
    fi
    
    # 安装 Playwright 浏览器
    log "安装 Playwright 浏览器..."
    playwright install
}

# 检查进程是否运行
is_running() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            return 0  # 进程正在运行
        else
            rm -f "$PID_FILE"  # 清理无效的PID文件
            return 1  # 进程未运行
        fi
    fi
    return 1  # PID文件不存在
}

# 获取进程PID
get_pid() {
    if [ -f "$PID_FILE" ]; then
        cat "$PID_FILE"
    fi
}

# 启动服务
start_service() {
    if is_running; then
        warn "服务已在运行 (PID: $(get_pid))"
        return 0
    fi
    
    log "启动 NumHarvest 服务..."
    
    # 切换到脚本目录
    cd "$SCRIPT_DIR"
    
    # 激活虚拟环境
    source "$VENV_DIR/bin/activate"
    
    # 后台启动服务
    nohup python3 "$MAIN_SCRIPT" --parallel > "$LOG_FILE" 2>&1 &
    local pid=$!
    
    # 保存PID
    echo "$pid" > "$PID_FILE"
    
    # 等待一下确保进程启动
    sleep 2
    
    if is_running; then
        log "服务启动成功 (PID: $pid)"
        log "日志文件: $LOG_FILE"
    else
        error "服务启动失败，请检查日志: $LOG_FILE"
        exit 1
    fi
}

# 停止服务
stop_service() {
    if ! is_running; then
        warn "服务未运行"
        return 0
    fi
    
    local pid=$(get_pid)
    log "停止服务 (PID: $pid)..."
    
    # 尝试优雅停止
    if kill "$pid" 2>/dev/null; then
        # 等待进程结束
        local count=0
        while kill -0 "$pid" 2>/dev/null && [ $count -lt 10 ]; do
            sleep 1
            count=$((count + 1))
        done
        
        # 如果还在运行，强制杀死
        if kill -0 "$pid" 2>/dev/null; then
            warn "强制停止服务..."
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
    
    rm -f "$PID_FILE"
    log "服务已停止"
}

# 重启服务
restart_service() {
    log "重启服务..."
    stop_service
    sleep 1
    start_service
}

# 查看服务状态
status_service() {
    if is_running; then
        local pid=$(get_pid)
        log "服务正在运行 (PID: $pid)"
        
        # 显示进程信息
        if command -v ps &> /dev/null; then
            info "进程信息:"
            ps -p "$pid" -o pid,ppid,cmd,etime,pcpu,pmem 2>/dev/null || true
        fi
        
        # 显示最近日志
        if [ -f "$LOG_FILE" ]; then
            info "最近日志 (最后10行):"
            tail -n 10 "$LOG_FILE"
        fi
    else
        warn "服务未运行"
        return 1
    fi
}

# 查看日志
view_logs() {
    if [ -f "$LOG_FILE" ]; then
        if [ "$1" = "follow" ] || [ "$1" = "-f" ]; then
            info "实时查看日志 (Ctrl+C 退出):"
            tail -f "$LOG_FILE"
        else
            info "查看日志:"
            tail -n 50 "$LOG_FILE"
        fi
    else
        warn "日志文件不存在: $LOG_FILE"
    fi
}

# 初始化设置
init_setup() {
    log "初始化 NumHarvest 环境..."
    
    check_python
    setup_venv
    install_dependencies
    
    log "初始化完成！"
    info "现在可以使用以下命令:"
    info "  $0 start    # 启动服务"
    info "  $0 status   # 查看状态"
    info "  $0 logs     # 查看日志"
}

# 显示帮助信息
show_help() {
    echo "NumHarvest 后台运行管理脚本"
    echo ""
    echo "用法: $0 {start|stop|restart|status|logs|init|help}"
    echo ""
    echo "命令:"
    echo "  start    启动服务 (如果已运行会先停止再启动)"
    echo "  stop     停止服务"
    echo "  restart  重启服务"
    echo "  status   查看服务状态"
    echo "  logs     查看日志 (最后50行)"
    echo "  logs -f  实时查看日志"
    echo "  init     初始化环境和依赖"
    echo "  help     显示此帮助信息"
    echo ""
    echo "文件位置:"
    echo "  脚本目录: $SCRIPT_DIR"
    echo "  PID文件:  $PID_FILE"
    echo "  日志文件: $LOG_FILE"
    echo "  虚拟环境: $VENV_DIR"
}

# 主函数
main() {
    case "${1:-}" in
        start)
            # 如果已运行，先停止
            if is_running; then
                warn "服务已运行，先停止现有进程..."
                stop_service
            fi
            
            # 检查是否需要初始化
            if [ ! -d "$VENV_DIR" ]; then
                log "首次运行，需要初始化环境..."
                init_setup
            fi
            
            start_service
            ;;
        stop)
            stop_service
            ;;
        restart)
            restart_service
            ;;
        status)
            status_service
            ;;
        logs)
            view_logs "${2:-}"
            ;;
        init)
            init_setup
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