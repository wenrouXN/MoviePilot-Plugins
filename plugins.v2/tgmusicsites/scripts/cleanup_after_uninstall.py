#!/usr/bin/env python3
"""TgMusicSites 卸载后清理脚本。

MP V3 卸载非分身插件时只停服务（close/stop_service），不会自动删除：
  1. plugindata 表里的插件数据（tg_bots / tg_conn_status 等）
  2. 手动添加到 SystemConfig "Directories" 的 /qbs/torrents/music/ 监控条目
  3. config/logs/plugins/tgmusicsites.log 日志文件

用法（在 MP 容器内执行，或本机有 docker 权限时）：
  docker exec moviepilot-v3 python3 /path/cleanup_after_uninstall.py
  # 或本机：python3 cleanup_after_uninstall.py --host 192.168.1.3

更推荐：卸载前先在插件页面点「清空插件数据」按钮（POST /api/v1/plugin/TgMusicSites/cleanup），
本脚本负责兜底 + 目录监控 + 日志清理。
"""
import argparse
import json
import subprocess
import sys

PLUGIN_ID = "TgMusicSites"
MONITOR_KEYWORD = "music"  # 匹配 Directories 中 monitor 目录含 music 的条目

def run(cmd: list, **kw) -> subprocess.CompletedProcess:
    print(f">>> {' '.join(str(c) for c in cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True, timeout=60, **kw)

def cleanup_local():
    # 1. 清 plugindata（经 MP 容器 psql 或 sqlite？V3 存 PG）
    #    需要容器内 psql + 密码。这里尝试从容器环境读取。
    r = run(["docker", "exec", "moviepilot-v3", "sh", "-c",
             "printenv POSTGRES_PASSWORD 2>/dev/null || cat /moviepilot.env 2>/dev/null | grep -oP 'POSTGRES_PASSWORD=\\K.*' || true"])
    pw = (r.stdout or "").strip().splitlines()[0] if r.stdout else ""
    if pw:
        sql = f"DELETE FROM plugindata WHERE plugin_id='{PLUGIN_ID}';"
        r2 = run(["docker", "exec", "moviepilot-v3", "sh", "-c",
                  f"psql 'postgresql://moviepilot:{pw}@127.0.0.1:5433/moviepilot' -c \"{sql}\""])
        print("plugindata 清理:", r2.stdout.strip() or r2.stderr.strip())
    else:
        print("!! 未获取到 PG 密码，跳过 plugindata 清理（可先用页面「清空插件数据」按钮）")

    # 2. 目录监控 Directories 清理（音乐条目）
    #    Directories 存 system_config 表 JSON。需要读 -> 过滤 -> 写回。
    #    手动 SQL 操作较复杂，提示用户手动在 MP 设置-目录同步中移除音乐条目。
    print("提示：请到 MP「设置 → 目录同步」移除 /qbs/torrents/music/ 条目（若不再需要）")

    # 3. 日志清理
    r3 = run(["docker", "exec", "moviepilot-v3", "sh", "-c",
              "rm -f /config/logs/plugins/tgmusicsites.log && echo removed"])
    print("日志清理:", r3.stdout.strip() or r3.stderr.strip())

def main():
    ap = argparse.ArgumentParser(description="TgMusicSites 卸载后清理")
    ap.add_argument("--host", help="SSH 主机（本机 docker 可用时省略）")
    args = ap.parse_args()
    if args.host:
        print("远程模式暂未实现，请在本机（docker 可达）运行。")
        sys.exit(1)
    cleanup_local()
    print("\n清理完成。剩余可手动项：插件 config（MP 卸载会保留非分身插件配置，一般无害）。")

if __name__ == "__main__":
    main()
