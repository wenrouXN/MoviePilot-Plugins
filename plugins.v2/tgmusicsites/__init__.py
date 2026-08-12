"""TG音乐站点插件：将 Telegram 音乐 Bot 作为音乐资源站点接入 MoviePilot V3 搜索链。

v0.3.0 重大变更：Telethon 内嵌 + NextFind 式 Web 登录与管理。
- Web 页面扫码登录（Telethon qr_login），session 持久化到插件 data，重装/升级不丢登录态
- 支持添加多个 Bot，每个 Bot 可自定义搜索命令模板（如 /search {keyword}）
- Web 页面试搜索：选 Bot + 输入关键词 → 显示结果列表
- 搜索/下载拦截仍走 get_module 胁持（search_torrents / async_search_torrents / download）

Telethon 事件循环策略：插件启动时创建专用后台线程 + 独立 asyncio loop，
client 常驻绑定该 loop，所有操作通过 run_coroutine_threadsafe 提交，彻底避免跨事件循环问题。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.chain import ChainBase
from app.chain.download import DownloadChain
from app.core.config import settings
from app.core.context import Context
from app.core.metainfo import MetaInfo
from app.helper.progress import ProgressHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import Notification, NotificationType, MessageChannel
from app.schemas.types import MediaType, SystemConfigKey, TorrentStatus
from app.utils.string import StringUtils
from fastapi import Request

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import SessionPasswordNeededError
    from telethon.tl.types import (
        Message,
        MessageMediaDocument,
        MessageMediaPhoto,
        UpdateNewMessage,
    )
    from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False


class TgMusicSites(_PluginBase):
    """TG音乐站点插件。"""

    # 插件元数据
    plugin_name = "TG音乐站点"
    plugin_desc = "将 Telegram 音乐 Bot 作为音乐资源站点接入 MoviePilot V3 搜索链。支持 Web 页面扫码登录、多 Bot 管理与自定义搜索命令。"
    plugin_icon = "Telegram_A.png"
    plugin_version = "0.3.0"
    plugin_label = "音乐,Telegram,资源站"
    plugin_author = "wenrouXN"
    plugin_config_prefix = "tgmusicsites_"
    plugin_order = 10
    auth_level = 1

    # 运行状态
    _enabled = False
    _download_dir = ""
    _search_timeout = 30
    _download_timeout = 60
    _button_index = 1
    _api_id = 0
    _api_hash = ""

    # TG 站点标记
    _TG_SITE_ID = -1
    _TG_SITE_NAME = "TG音乐"
    _TG_URL_PREFIX = "tg://music/"
    _TG_MAGNET_MARKER = "tgmusic-"  # magnet 伪链中的 uid 标记，用于 MP 下载链识别

    # Telethon 常驻线程与事件循环
    _loop: Optional[asyncio.AbstractEventLoop] = None
    _loop_thread: Optional[threading.Thread] = None
    _client: Optional[TelegramClient] = None
    _client_lock = threading.Lock()
    _client_ready = False  # client 已创建并连接

    # 登录状态机
    _login_state = "idle"  # idle | qr_waiting | qr_scanned | code_sent | 2fa_required | logged_in | error
    _login_qr_data = ""    # 二维码 token（原始 base64）
    _login_qr_image = ""   # 二维码 PNG data URI（页面展示）
    _login_error = ""
    _login_qr_login = None  # qr_login 对象
    _login_phone = ""       # 手机号登录：当前手机号
    _phone_code_hash = ""   # 手机号登录：send_code_request 返回的 phone_code_hash

    # 搜索会话去重
    _search_lock = threading.Lock()
    _last_search_key = ""
    _last_search_time = 0.0
    _last_search_results: List[Dict[str, Any]] = []
    _last_download_info: Dict[str, Any] = {}

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._enabled = False
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        if not self._enabled:
            return
        self._download_dir = str(config.get("download_dir") or "/qbs/torrents/music/").strip()
        self._search_timeout = int(config.get("search_timeout") or 30)
        self._download_timeout = int(config.get("download_timeout") or 60)
        self._button_index = int(config.get("button_index") or 1)
        self._api_id = int(config.get("api_id") or 0)
        self._api_hash = str(config.get("api_hash") or "").strip()
        # 代理配置：写 data 供 _build_proxy 读取（UI 可配置，向后兼容）
        proxy_host = str(config.get("proxy_host") or "127.0.0.1").strip()
        proxy_port = int(config.get("proxy_port") or 7891)
        proxy_type = str(config.get("proxy_type") or "socks5").strip().lower()
        self.save_data("tg_proxy", {"type": proxy_type, "host": proxy_host, "port": proxy_port})

        if not TELEGRAM_AVAILABLE:
            logger.error("TG音乐站点插件：Telethon 未安装，请在插件依赖中安装 telethon>=1.34.0")
            self._enabled = False
            return
        # 启动 Telethon 后台线程（client 常驻，登录态从 data 恢复）
        self._start_telegram_worker()
        logger.info("TG音乐站点插件已启用：下载目录=%s，搜索超时=%ss",
                    self._download_dir, self._search_timeout)

    def _start_telegram_worker(self) -> None:
        """启动 Telethon 专用后台线程。"""
        if self._loop_thread and self._loop_thread.is_alive():
            return
        self._loop_thread = threading.Thread(
            target=self._telegram_loop_main, name="tgmusicsites-loop", daemon=True
        )
        self._loop_thread.start()

    def _telegram_loop_main(self) -> None:
        """Telethon 后台线程主函数：运行独立 asyncio loop。"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            # 从 data 恢复 session（如有）
            session_str = self.get_data("tg_session") or ""
            if session_str:
                try:
                    client = TelegramClient(
                        StringSession(session_str),
                        self._api_id,
                        self._api_hash,
                        proxy=self._build_proxy(),
                    )
                    loop.run_until_complete(client.connect())
                    if client.is_connected():
                        self._client = client
                        self._client_ready = True
                        self._login_state = "logged_in"
                        logger.info("TG音乐站点：Telethon 登录态已恢复")
                    else:
                        logger.warning("TG音乐站点：session 恢复失败，需重新登录")
                        self._login_state = "idle"
                except Exception as e:
                    logger.error(f"TG音乐站点：session 恢复异常: {e}")
                    self._login_state = "idle"
            loop.run_forever()
        except Exception as e:
            logger.error(f"TG音乐站点：Telethon 线程异常退出: {e}")
        finally:
            loop.close()
            self._loop = None
            if self._loop_thread is threading.current_thread():
                self._loop_thread = None

    def _build_proxy(self) -> Optional[Tuple[str, str, int]]:
        """构建 Telethon 代理配置（从 data 读取，默认 socks5://127.0.0.1:7891）。"""
        proxy_cfg = self.get_data("tg_proxy") or {
            "type": "socks5",
            "host": "127.0.0.1",
            "port": 7891,
        }
        ptype = str(proxy_cfg.get("type") or "socks5").lower()
        host = str(proxy_cfg.get("host") or "127.0.0.1")
        port = int(proxy_cfg.get("port") or 7891)
        if ptype in ("socks5", "socks"):
            return ("socks5", host, port)
        elif ptype == "socks4":
            return ("socks4", host, port)
        elif ptype == "http":
            return ("http", host, port)
        return ("socks5", host, port)

    def _submit(self, coro: Any, timeout: Optional[float] = None) -> Any:
        """向 Telethon 专用 loop 提交协程并等待结果（线程安全）。"""
        if not self._loop or self._loop.is_closed():
            raise RuntimeError("Telethon 事件循环未就绪")
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return [
            {
                "path": "/login/qr",
                "endpoint": self.api_login_qr,
                "methods": ["GET"],
                "summary": "生成 TG 登录二维码",
                "description": "生成 Telegram QR 登录二维码（需已配置 api_id/api_hash）",
            },
            {
                "path": "/login/phone",
                "endpoint": self.api_login_phone,
                "methods": ["POST"],
                "summary": "手机号登录：发送验证码",
                "description": "提交手机号（含国家码），发送 Telegram 登录验证码",
            },
            {
                "path": "/login/code",
                "endpoint": self.api_login_code,
                "methods": ["POST"],
                "summary": "手机号登录：提交验证码",
                "description": "提交短信/App 验证码完成登录（如开启两步验证则先返回 2fa_required）",
            },
            {
                "path": "/login/password",
                "endpoint": self.api_login_password,
                "methods": ["POST"],
                "summary": "两步验证：提交密码",
                "description": "登录开启两步验证时，提交账号密码完成登录",
            },
            {
                "path": "/login/status",
                "endpoint": self.api_login_status,
                "methods": ["GET"],
                "summary": "查询 TG 登录状态",
                "description": "轮询登录状态：idle/qr_waiting/qr_scanned/code_sent/2fa_required/logged_in/error",
                "allow_anonymous": True,
            },
            {
                "path": "/login/logout",
                "endpoint": self.api_login_logout,
                "methods": ["POST"],
                "summary": "注销 TG 登录",
                "description": "断开并清除 TG 登录态",
            },
            {
                "path": "/bots",
                "endpoint": self.api_bots,
                "methods": ["GET", "POST", "DELETE"],
                "summary": "TG 音乐 Bot 管理",
                "description": "查询/添加/删除 TG 音乐 Bot（含自定义搜索命令）",
            },
            {
                "path": "/cleanup",
                "endpoint": self.api_cleanup,
                "methods": ["POST"],
                "summary": "清空插件数据",
                "description": "卸载前调用：删除全部 Bot 站点与连接状态数据（plugindata），不删除插件本身",
            },
            {
                "path": "/search",
                "endpoint": self.api_try_search,
                "methods": ["POST"],
                "summary": "Web 试搜索",
                "description": "在 Web 页面试搜索歌曲（选 Bot + 关键词）",
                "allow_anonymous": True,
            },
            {
                "path": "/test",
                "endpoint": self.api_test,
                "methods": ["GET"],
                "summary": "测试 TG 连接",
                "description": "测试 Telethon 连接状态",
                "allow_anonymous": True,
            },
            {
                "path": "/history",
                "endpoint": self.api_history,
                "methods": ["GET"],
                "summary": "下载历史记录",
                "description": "获取插件下载历史（含真实大小/时长/专辑等）",
            },
            {
                "path": "/logs",
                "endpoint": self.api_logs,
                "methods": ["GET"],
                "summary": "插件日志",
                "description": "读取插件日志文件尾部内容（最近 30 行）",
            },
        ]

    def get_module(self) -> Dict[str, Any]:
        """获取插件模块声明，用于胁持系统模块实现（方法名：方法实现）。"""
        if not self._enabled:
            return {}
        return {
            "search_torrents": self.tg_search_torrents,
            "async_search_torrents": self.tg_async_search_torrents,
            "download": self.tg_download,
        }

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """返回插件配置表单与默认配置。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "class": "mb-2"},
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-subtitle-2 font-weight-bold"},
                                "text": "⚙️ 基础设置",
                            },
                            {
                                "component": "VCardText",
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "hint": "关闭后不参与搜索链",
                                            "persistent-hint": True,
                                        },
                                    },
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "api_id",
                                                            "label": "Telegram API ID",
                                                            "hint": "my.telegram.org 获取，仅首次登录需要",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "api_hash",
                                                            "label": "Telegram API Hash",
                                                            "hint": "仅首次登录需要",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "class": "mb-2"},
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-subtitle-2 font-weight-bold"},
                                "text": "📥 下载设置",
                            },
                            {
                                "component": "VCardText",
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "download_dir",
                                            "label": "音乐下载目录",
                                            "hint": "默认 /qbs/torrents/music/",
                                            "persistent-hint": True,
                                        },
                                    },
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "search_timeout",
                                                            "label": "搜索超时（秒）",
                                                            "hint": "默认 30",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "download_timeout",
                                                            "label": "下载超时（秒）",
                                                            "hint": "默认 120",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "button_index",
                                            "label": "默认按钮序号",
                                            "hint": "搜索结果默认点击第几个按钮，默认 1",
                                            "persistent-hint": True,
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "class": "mb-2"},
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-subtitle-2 font-weight-bold"},
                                "text": "🌐 网络代理",
                            },
                            {
                                "component": "VCardText",
                                "content": [
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "proxy_host",
                                                            "label": "代理主机",
                                                            "hint": "默认 127.0.0.1",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "proxy_port",
                                                            "label": "代理端口",
                                                            "hint": "默认 7891",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "proxy_type",
                                            "label": "代理类型",
                                            "items": ["socks5", "http", "socks4"],
                                            "hint": "默认 socks5",
                                            "persistent-hint": True,
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "dense": True,
                            "text": "💡 登录方式：在详情页点击『生成登录二维码』扫码即可；api_id/api_hash 仅首次登录需要，session 自动持久化，重装/升级不丢失。",
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "api_id": "",
            "api_hash": "",
            "download_dir": "/qbs/torrents/music/",
            "search_timeout": 30,
            "download_timeout": 120,
            "button_index": 1,
            "proxy_host": "127.0.0.1",
            "proxy_port": 7891,
            "proxy_type": "socks5"
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面（Vuetify JSON）。"""
        if not self._enabled:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "warning",
                        "text": "插件未启用，请在插件配置中启用并填写 api_id/api_hash 后查看详情。"
                    },
                }
            ]
        bots = self.get_data("tg_bots") or {}
        login_state = self._login_state
        login_text = {
            "idle": "未登录",
            "qr_waiting": "等待扫码",
            "qr_scanned": "已扫码，请在手机确认",
            "code_sent": "验证码已发送，请填写验证码",
            "2fa_required": "需要两步验证密码",
            "logged_in": "已登录",
            "error": f"登录失败: {self._login_error}",
        }.get(login_state, login_state)
        login_ok = login_state == "logged_in"
        conn_state = f"✅ {login_text}" if login_ok else f"❌ {login_text}"
        # 二维码展示区块（登录中时显示）
        qr_section = []
        if self._login_qr_image:
            qr_section = [
                {
                    "component": "VCardText",
                    "props": {"class": "py-2 text-center"},
                    "content": [
                        {
                            "component": "VImg",
                            "props": {
                                "src": self._login_qr_image,
                                "height": 220,
                                "width": 220,
                                "contain": True,
                                "class": "mx-auto"
                            }
                        },
                        {"component": "div", "props": {"class": "text-body-2 text-grey pt-2"}, "text": "用手机 Telegram 扫码登录"},
                    ],
                }
            ]
        # Bot 列表行
        bot_rows = []
        for k, v in bots.items():
            cmd = v.get("search_command") or "/search {keyword}"
            bot_rows.append({
                "component": "div",
                "props": {"class": "d-flex align-center justify-space-between py-1"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "text-body-2"},
                        "text": f"{v.get('name', 'TG音乐')} (@{v.get('bot_username', '')}) 命令: {cmd}",
                    },
                    {
                        "component": "VBtn",
                        "props": {
                            "color": "error",
                            "variant": "tonal",
                            "size": "x-small",
                            "prepend-icon": "mdi-delete",
                        },
                        "text": "删除",
                        "events": {
                            "click": {
                                "api": "plugin/TgMusicSites/bots",
                                "method": "delete",
                                "params": {"bot_id": k},
                            }
                        },
                    },
                ],
            })
        # 下载历史行（含真实大小/时长/专辑）
        history = self.get_data("tg_download_history") or []
        if not isinstance(history, list):
            history = []
        # 插件日志（服务端读取，打开页面即显示最近 30 行）
        log_lines = []
        try:
            log_path = Path(settings.CONFIG_PATH) / "logs" / "plugins" / "tgmusicsites.log"
            if log_path.exists():
                log_lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-30:]
        except Exception:
            log_lines = []
        history_rows = []
        for h in history:
            size_txt = h.get("size_text") or (
                f"{h['size'] / 1024 / 1024:.1f}MB" if h.get("size") else ""
            )
            dur_txt = f"{h['duration']}s" if h.get("duration") else ""
            album_txt = h.get("album") or ""
            meta = " · ".join(x for x in [size_txt, dur_txt, album_txt] if x)
            title = h.get("title") or h.get("file_name") or ""
            history_rows.append({
                "component": "div",
                "props": {"class": "d-flex align-center justify-space-between py-1"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "text-body-2"},
                        "text": f"{h.get('time', '')} {title}",
                    },
                    {
                        "component": "div",
                        "props": {"class": "text-caption text-grey"},
                        "text": meta,
                    },
                ],
            })
        return [
            {
                "component": "VExpansionPanels",
                "props": {
                    "modelValue": 0,
                    "multiple": True,
                },
                "content": [
                    {
                        "component": "VExpansionPanel",
                        "content": [
                            {
                                "component": "VExpansionPanelTitle",
                                "props": {"class": "text-subtitle-1 font-weight-bold"},
                                "content": [
                                    {"component": "span", "text": "📊 状态"},
                                    {"component": "span", "props": {"class": "ml-2 text-caption text-grey"}, "text": "登录状态、连接测试与试搜索"},
                                ],
                            },
                            {
                                "component": "VExpansionPanelText",
                                "content": [
                                {
                                    "component": "VRow",
                                    "props": {"class": "mb-2"},
                                    "content": [
                                        {
                                            "component": "VCol",
                                            "props": {"cols": 12, "md": 6},
                                            "content": [
                                                {
                                                    "component": "VCard",
                                                    "props": {"variant": "tonal", "color": "primary"},
                                                    "content": [
                                                        {
                                                            "component": "VCardTitle",
                                                            "props": {"class": "text-subtitle-1 font-weight-bold"},
                                                            "text": "TG 登录状态",
                                                        },
                                                        {
                                                            "component": "VCardText",
                                                            "props": {"class": "py-2"},
                                                            "content": [
                                                                {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"登录状态：{conn_state}"},
                                                                {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"下载目录：{self._download_dir}"},
                                                                {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"Bot 数量：{len(bots)} 个"},
                                                            ],
                                                        },
                                                        *qr_section,
                                                        # 手机号登录区块
                                                        {
                                                            "component": "VDivider",
                                                            "props": {"class": "my-2"},
                                                        },
                                                        {
                                                            "component": "div",
                                                            "props": {"class": "text-subtitle-2 font-weight-bold py-1"},
                                                            "text": "手机号登录（备用）",
                                                        },
                                                        {
                                                            "component": "VTextField",
                                                            "props": {
                                                                "model": "login_phone",
                                                                "label": "手机号（含国家码）",
                                                                "placeholder": "+8613800138000",
                                                                "hint": "扫码不便时可用手机号+验证码登录",
                                                            },
                                                        },
                                                        {
                                                            "component": "VBtn",
                                                            "props": {
                                                                "color": "primary",
                                                                "variant": "outlined",
                                                                "size": "small",
                                                                "prepend-icon": "mdi-message-text",
                                                            },
                                                            "text": "获取验证码",
                                                            "events": {
                                                                "click": {
                                                                    "api": "plugin/TgMusicSites/login/phone",
                                                                    "method": "post",
                                                                    "params": {"phone": "login_phone"},
                                                                }
                                                            },
                                                        },
                                                        {
                                                            "component": "VTextField",
                                                            "props": {
                                                                "model": "login_code",
                                                                "label": "验证码",
                                                                "placeholder": "12345",
                                                                "hint": "在 Telegram 内查看验证码（部分账号发送到手机短信）",
                                                            },
                                                        },
                                                        {
                                                            "component": "VTextField",
                                                            "props": {
                                                                "model": "login_password",
                                                                "label": "两步验证密码（如开启）",
                                                                "placeholder": "",
                                                                "hint": "账号开启两步验证时填写",
                                                            },
                                                        },
                                                        {
                                                            "component": "VBtn",
                                                            "props": {
                                                                "color": "success",
                                                                "variant": "tonal",
                                                                "size": "small",
                                                                "prepend-icon": "mdi-login",
                                                            },
                                                            "text": "验证码登录",
                                                            "events": {
                                                                "click": {
                                                                    "api": "plugin/TgMusicSites/login/code",
                                                                    "method": "post",
                                                                    "params": {"code": "login_code"},
                                                                }
                                                            },
                                                        },
                                                        {
                                                            "component": "VBtn",
                                                            "props": {
                                                                "color": "warning",
                                                                "variant": "tonal",
                                                                "size": "small",
                                                                "prepend-icon": "mdi-form-textbox-password",
                                                            },
                                                            "text": "两步验证登录",
                                                            "events": {
                                                                "click": {
                                                                    "api": "plugin/TgMusicSites/login/password",
                                                                    "method": "post",
                                                                    "params": {"password": "login_password"},
                                                                }
                                                            },
                                                        },
                                                        {
                                                            "component": "VCardActions",
                                                            "props": {"class": "pt-0"},
                                                            "content": [
                                                                {
                                                                    "component": "VBtn",
                                                                    "props": {
                                                                        "color": "primary",
                                                                        "variant": "tonal",
                                                                        "prepend-icon": "mdi-qrcode",
                                                                    },
                                                                    "text": "生成登录二维码",
                                                                    "events": {
                                                                        "click": {
                                                                            "api": "plugin/TgMusicSites/login/qr",
                                                                            "method": "get",
                                                                        }
                                                                    },
                                                                },
                                                                {
                                                                    "component": "VBtn",
                                                                    "props": {
                                                                        "color": "secondary",
                                                                        "variant": "tonal",
                                                                        "prepend-icon": "mdi-refresh",
                                                                    },
                                                                    "text": "刷新状态",
                                                                    "events": {
                                                                        "click": {
                                                                            "api": "plugin/TgMusicSites/login/status",
                                                                            "method": "get",
                                                                        }
                                                                    },
                                                                },
                                                                {
                                                                    "component": "VBtn",
                                                                    "props": {
                                                                        "color": "error",
                                                                        "variant": "tonal",
                                                                        "prepend-icon": "mdi-logout",
                                                                    },
                                                                    "text": "注销登录",
                                                                    "events": {
                                                                        "click": {
                                                                            "api": "plugin/TgMusicSites/login/logout",
                                                                            "method": "post",
                                                                        }
                                                                    },
                                                                },
                                                            ],
                                                        },
                                                    ],
                                                },
                                            ],
                                        },
                                        {
                                            "component": "VCol",
                                            "props": {"cols": 12, "md": 6},
                                            "content": [
                                                {
                                                    "component": "VCard",
                                                    "props": {"variant": "tonal", "color": "secondary"},
                                                    "content": [
                                                        {
                                                            "component": "VCardTitle",
                                                            "props": {"class": "text-subtitle-1 font-weight-bold"},
                                                            "text": "试搜索",
                                                        },
                                                        {
                                                            "component": "VCardText",
                                                            "props": {"class": "py-2"},
                                                            "content": [
                                                                {
                                                                    "component": "VTextField",
                                                                    "props": {
                                                                        "model": "try_keyword",
                                                                        "label": "搜索关键词",
                                                                        "hint": "输入歌曲名，选择 Bot 后搜索"
                                                                    }
                                                                },
                                                                {
                                                                    "component": "VSelect",
                                                                    "props": {
                                                                        "model": "try_bot",
                                                                        "label": "选择 Bot",
                                                                        "items": [
                                                                            {"title": v.get("name", k), "value": v.get("bot_username", "")}
                                                                            for k, v in bots.items()
                                                                        ] or [{"title": "请先添加 Bot", "value": ""}]
                                                                    }
                                                                },
                                                                {
                                                                    "component": "VBtn",
                                                                    "props": {
                                                                        "color": "success",
                                                                        "variant": "tonal",
                                                                        "prepend-icon": "mdi-magnify",
                                                                    },
                                                                    "text": "试搜索",
                                                                    "events": {
                                                                        "click": {
                                                                            "api": "plugin/TgMusicSites/search",
                                                                            "method": "post",
                                                                            "params": {"keyword": "try_keyword", "bot_username": "try_bot"},
                                                                        }
                                                                    },
                                                                },
                                                            ],
                                                        },
                                                    ],
                                                },
                                            ],
                                        },
                                    ],
                                },
                        
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VExpansionPanel",
                        "content": [
                            {
                                "component": "VExpansionPanelTitle",
                                "props": {"class": "text-subtitle-1 font-weight-bold"},
                                "content": [
                                    {"component": "span", "text": "🔌 站点"},
                                    {"component": "span", "props": {"class": "ml-2 text-caption text-grey"}, "text": "添加 Bot 与站点列表"},
                                ],
                            },
                            {
                                "component": "VExpansionPanelText",
                                "content": [
                                {
                                    "component": "VCard",
                                    "props": {"variant": "outlined", "class": "mt-2"},
                                    "content": [
                                        {
                                            "component": "VCardTitle",
                                            "props": {"class": "text-subtitle-1 font-weight-bold"},
                                            "text": "添加 Bot 站点",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "py-2"},
                                            "content": [
                                                {
                                                    "component": "div",
                                                    "props": {"class": "text-body-2 text-grey pb-2"},
                                                    "text": "手动添加 Telegram 音乐 Bot 作为搜索站点（支持多个 Bot）",
                                                },
                                                {
                                                    "component": "VTextField",
                                                    "props": {
                                                        "model": "new_bot_username",
                                                        "label": "Bot 用户名",
                                                        "placeholder": "music_v1bot",
                                                        "hint": "不带 @ 前缀",
                                                    },
                                                },
                                                {
                                                    "component": "VTextField",
                                                    "props": {
                                                        "model": "new_bot_name",
                                                        "label": "显示名称",
                                                        "placeholder": "音乐机器人",
                                                        "hint": "留空默认 Bot 用户名",
                                                    },
                                                },
                                                {
                                                    "component": "VTextField",
                                                    "props": {
                                                        "model": "new_bot_command",
                                                        "label": "搜索命令",
                                                        "placeholder": "/search {keyword}",
                                                        "hint": "{keyword} 会被替换为搜索词",
                                                    },
                                                },
                                                {
                                                    "component": "VBtn",
                                                    "props": {
                                                        "color": "primary",
                                                        "variant": "tonal",
                                                        "prepend-icon": "mdi-plus",
                                                    },
                                                    "text": "添加 Bot",
                                                    "events": {
                                                        "click": {
                                                            "api": "plugin/TgMusicSites/bots",
                                                            "method": "post",
                                                            "params": {
                                                                "bot_username": "new_bot_username",
                                                                "name": "new_bot_name",
                                                                "search_command": "new_bot_command",
                                                            },
                                                        }
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                                {
                                    "component": "VCard",
                                    "props": {"variant": "outlined", "class": "mt-2"},
                                    "content": [
                                        {
                                            "component": "VCardTitle",
                                            "props": {"class": "text-subtitle-1 font-weight-bold"},
                                            "text": "Bot 列表",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "py-2"},
                                            "content": bot_rows or [
                                                {
                                                    "component": "div",
                                                    "props": {"class": "text-body-2 text-grey"},
                                                    "text": "暂无 Bot，请在上方添加",
                                                }
                                            ],
                                        },
                                        {
                                            "component": "VCardActions",
                                            "props": {"class": "pt-0"},
                                            "content": [
                                                {
                                                    "component": "VBtn",
                                                    "props": {
                                                        "color": "error",
                                                        "variant": "tonal",
                                                        "prepend-icon": "mdi-delete-sweep",
                                                    },
                                                    "text": "清空插件数据（卸载前）",
                                                    "events": {
                                                        "click": {
                                                            "api": "plugin/TgMusicSites/cleanup",
                                                            "method": "post",
                                                        }
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                        
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VExpansionPanel",
                        "content": [
                            {
                                "component": "VExpansionPanelTitle",
                                "props": {"class": "text-subtitle-1 font-weight-bold"},
                                "content": [
                                    {"component": "span", "text": "📜 记录"},
                                    {"component": "span", "props": {"class": "ml-2 text-caption text-grey"}, "text": "下载历史与插件日志"},
                                ],
                            },
                            {
                                "component": "VExpansionPanelText",
                                "content": [
                                {
                                    "component": "VCard",
                                    "props": {"variant": "tonal", "class": "mt-2"},
                                    "content": [
                                        {
                                            "component": "VCardTitle",
                                            "props": {"class": "text-subtitle-1 font-weight-bold"},
                                            "text": "📥 下载历史",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "pt-0"},
                                            "content": history_rows
                                            if history_rows
                                            else [
                                                {
                                                    "component": "div",
                                                    "props": {"class": "text-body-2 text-grey"},
                                                    "text": "暂无下载记录",
                                                }
                                            ],
                                        },
                                    ],
                                },
                                {
                                    "component": "VCard",
                                    "props": {"variant": "tonal", "class": "mt-2"},
                                    "content": [
                                        {
                                            "component": "VCardTitle",
                                            "props": {"class": "text-subtitle-1 font-weight-bold"},
                                            "text": "📋 插件日志",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "pt-0"},
                                            "content": [
                                                {
                                                    "component": "VBtn",
                                                    "props": {
                                                        "color": "primary",
                                                        "variant": "tonal",
                                                        "prepend-icon": "mdi-refresh",
                                                        "size": "small",
                                                    },
                                                    "text": "刷新日志",
                                                    "events": {
                                                        "click": {
                                                            "api": "plugin/TgMusicSites/logs",
                                                            "method": "get",
                                                        }
                                                    },
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-body-2 mt-2",
                                                        "style": "max-height: 300px; overflow-y: auto; font-family: monospace; white-space: pre-wrap; word-break: break-all; font-size: 12px;",
                                                    },
                                                    "text": "\n".join(log_lines) if log_lines else "暂无日志",
                                                },
                                            ],
                                        },
                                    ],
                                },
                        
                                ],
                            },
                        ],
                    },
                ],
            },
        ]

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        # 停止旧 worker 线程（必须在启动新线程前完成，否则 _start_telegram_worker 会因旧线程存活而跳过）
        old_thread = self._loop_thread
        old_loop = self._loop
        if old_loop and not old_loop.is_closed():
            try:
                old_loop.call_soon_threadsafe(old_loop.stop)
            except Exception:
                pass
        if old_thread and old_thread.is_alive():
            old_thread.join(timeout=5)
        self._loop_thread = None
        self._loop = None
        # 断开 Telethon client（在专用 loop 内）
        if self._client and old_loop and not old_loop.is_closed():
            try:
                client = self._client
                loop = old_loop
                def _close():
                    try:
                        loop.run_until_complete(client.disconnect())
                    except Exception:
                        pass
                threading.Thread(target=_close, daemon=True).start()
            except Exception:
                pass
        self._client = None
        self._client_ready = False
        self._login_state = "idle"
        self._login_qr_data = ""
        self._login_qr_login = None

    # ==================== 登录 API ====================

    async def api_login_phone(self, request: Request) -> Dict[str, Any]:
        """手机号登录：提交手机号，发送验证码。"""
        try:
            body = await request.json() or {}
        except Exception:
            body = {}
        phone = str(body.get("phone") or "").strip()
        if not phone:
            return {"success": False, "message": "请填写手机号（含国家码，如 +8613800138000）"}
        if self._client_ready:
            return {"success": True, "message": "已登录，无需重复登录", "state": "logged_in"}
        try:
            async def _send():
                client = TelegramClient(
                    StringSession(),
                    self._api_id,
                    self._api_hash,
                    proxy=self._build_proxy(),
                )
                await client.connect()
                self._client = client
                result = await client.send_code_request(phone)
                self._login_phone = phone
                self._phone_code_hash = result.phone_code_hash
                self._login_state = "code_sent"
            self._submit(_send(), timeout=40)
            return {"success": True, "state": "code_sent", "message": f"验证码已发送至 {phone}，请在 Telegram 内查看"}
        except Exception as e:
            self._login_state = "error"
            self._login_error = str(e)
            logger.error(f"TG音乐站点：发送验证码失败: {e}")
            return {"success": False, "message": f"发送验证码失败: {e}"}

    async def api_login_code(self, request: Request) -> Dict[str, Any]:
        """手机号登录：提交验证码。"""
        try:
            body = await request.json() or {}
        except Exception:
            body = {}
        code = str(body.get("code") or "").strip()
        if not code:
            return {"success": False, "message": "请填写验证码"}
        if not self._login_phone or not self._client:
            return {"success": False, "message": "请先获取验证码"}
        try:
            async def _sign():
                await self._client.sign_in(
                    self._login_phone,
                    code=code,
                    phone_code_hash=self._phone_code_hash,
                )
            self._submit(_sign(), timeout=40)
            me = self._submit(self._client.get_me(), timeout=30)
            session_str = StringSession.save(self._client.session)
            self.save_data("tg_session", session_str)
            self._client_ready = True
            self._login_state = "logged_in"
            self._login_phone = ""
            self._phone_code_hash = ""
            logger.info(f"TG音乐站点：手机号登录成功: {me.username or me.first_name}")
            return {"success": True, "state": "logged_in", "message": "登录成功"}
        except Exception as e:
            try:
                from telethon.errors import SessionPasswordNeededError
                if isinstance(e, SessionPasswordNeededError):
                    self._login_state = "2fa_required"
                    return {"success": False, "state": "2fa_required", "message": "该账号开启了两步验证，请提交密码"}
            except ImportError:
                pass
            self._login_state = "error"
            self._login_error = str(e)
            logger.error(f"TG音乐站点：验证码登录失败: {e}")
            return {"success": False, "message": f"登录失败: {e}"}

    async def api_login_password(self, request: Request) -> Dict[str, Any]:
        """两步验证：提交密码完成登录。"""
        try:
            body = await request.json() or {}
        except Exception:
            body = {}
        password = str(body.get("password") or "").strip()
        if not password:
            return {"success": False, "message": "请填写两步验证密码"}
        if not self._client:
            return {"success": False, "message": "请先获取验证码"}
        try:
            async def _sign():
                await self._client.sign_in(password=password)
            self._submit(_sign(), timeout=40)
            me = self._submit(self._client.get_me(), timeout=30)
            session_str = StringSession.save(self._client.session)
            self.save_data("tg_session", session_str)
            self._client_ready = True
            self._login_state = "logged_in"
            self._login_phone = ""
            self._phone_code_hash = ""
            logger.info(f"TG音乐站点：两步验证登录成功: {me.username or me.first_name}")
            return {"success": True, "state": "logged_in", "message": "登录成功"}
        except Exception as e:
            self._login_state = "error"
            self._login_error = str(e)
            logger.error(f"TG音乐站点：两步验证登录失败: {e}")
            return {"success": False, "message": f"登录失败: {e}"}

    async def api_login_qr(self) -> Dict[str, Any]:
        """生成 TG 登录二维码（返回 PNG data URI 供页面展示）。"""
        if not self._api_id or not self._api_hash:
            return {"success": False, "message": "请先在插件配置中填写 api_id / api_hash"}
        if self._client_ready:
            return {"success": True, "message": "已登录，无需重复扫码", "state": "logged_in"}
        if not self._loop or self._loop.is_closed():
            self._start_telegram_worker()
            # 等待 loop 就绪
            for _ in range(50):
                if self._loop and not self._loop.is_closed():
                    break
                await asyncio.sleep(0.1)
        try:
            qr_data = await asyncio.to_thread(
                self._submit, self._do_qr_login(), timeout=20
            )
            if qr_data:
                self._login_state = "qr_waiting"
                self._login_qr_data = qr_data
                # 生成二维码 PNG data URI（Telethon token → tg://login?token= → QR）
                try:
                    self._login_qr_image = self._make_qr_image(qr_data)
                except Exception as e:
                    logger.error(f"TG音乐站点：二维码图片生成失败: {e}")
                    self._login_qr_image = ""
                # 启动后台轮询登录结果
                threading.Thread(target=self._poll_qr_login, daemon=True).start()
                return {
                    "success": True,
                    "qr_token": qr_data,
                    "qr_image": self._login_qr_image,
                    "state": "qr_waiting",
                }
            return {"success": False, "message": "二维码生成失败"}
        except Exception as e:
            self._login_state = "error"
            self._login_error = str(e)
            return {"success": False, "message": f"二维码生成失败: {e}"}

    @staticmethod
    def _make_qr_image(login_url: str) -> str:
        """将 TG 登录 URL 转为二维码 PNG data URI。"""
        import qrcode
        qr = qrcode.QRCode(box_size=8, border=2)
        qr.add_data(login_url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()
        return f"data:image/png;base64,{b64}"

    async def _do_qr_login(self) -> Optional[str]:
        """在专用 loop 内执行 qr_login，返回二维码 URL。"""
        try:
            # 关闭旧的登录流程 client（避免多次生成二维码时连接泄漏）
            # 仅当当前处于登录流程（未登录完成）时断开；已登录的正式 client 不碰
            if self._login_state in ("qr_waiting", "qr_scanned") and self._client:
                try:
                    await self._client.disconnect()
                except Exception:
                    pass
            client = TelegramClient(
                StringSession(),
                self._api_id,
                self._api_hash,
                proxy=self._build_proxy(),
            )
            await client.connect()
            qr_login = await client.qr_login()
            self._client = client
            self._login_qr_login = qr_login
            # QRLogin.url 即 tg://login 二维码内容，可直接用于生成二维码
            url = qr_login.url
            if url:
                return url
            return None
        except Exception as e:
            logger.error(f"TG音乐站点：qr_login 异常: {e}")
            return None

    def _poll_qr_login(self) -> None:
        """后台轮询扫码登录结果（专用 loop 内），二维码自动续期。

        每轮 qr.wait 超时后 Telethon 会刷新 token，此处同步更新二维码图片，
        保证页面上的二维码始终可扫，直到登录成功或出错。
        """
        if not self._loop or self._loop.is_closed():
            return
        while self._login_state in ("qr_waiting", "qr_scanned"):
            try:
                done = self._submit(self._qr_wait_once(), timeout=40)
                if done:
                    return  # 登录完成
                # 超时未扫码：token 已刷新，重新生成二维码图片
                qr = self._login_qr_login
                if qr:
                    url = qr.url
                    if url and url != self._login_qr_data:
                        self._login_qr_data = url
                        try:
                            self._login_qr_image = self._make_qr_image(url)
                            logger.info("TG音乐站点：二维码已自动刷新")
                        except Exception as e:
                            logger.error(f"TG音乐站点：二维码刷新失败: {e}")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"TG音乐站点：登录轮询异常: {e}")
                return
            time.sleep(0.3)

    async def _qr_wait_once(self) -> bool:
        """等待一轮扫码（30s）。登录成功返回 True，超时/未扫码返回 False。"""
        qr = self._login_qr_login
        if not qr:
            return True
        try:
            await qr.wait(timeout=30)
            if self._client and self._client.is_connected():
                me = await self._client.get_me()
                # 保存 session
                session_str = StringSession.save(self._client.session)
                self.save_data("tg_session", session_str)
                self._client_ready = True
                self._login_state = "logged_in"
                self._login_qr_data = ""
                self._login_qr_image = ""
                self._login_qr_login = None
                logger.info(f"TG音乐站点：扫码登录成功: {me.username or me.first_name}")
                return True
        except asyncio.TimeoutError:
            pass
        return False

    async def api_login_status(self) -> Dict[str, Any]:
        """查询 TG 登录状态。"""
        # 如果 client 已连接但状态未同步（如 QR 登录完成后），同步一次
        if self._client_ready and self._client and self._client.is_connected() and self._login_state != "logged_in":
            self._login_state = "logged_in"
        # 如果已标记登录但 client 掉线，状态修正为错误
        if self._login_state == "logged_in" and (not self._client_ready or not self._client or not self._client.is_connected()):
            self._login_state = "idle"
        return {
            "success": True,
            "state": self._login_state,
            "connected": self._client_ready,
            "user": self._get_login_user(),
            "message": {
                "idle": "未登录",
                "qr_waiting": "等待扫码",
                "qr_scanned": "已扫码，请在手机确认",
                "code_sent": "验证码已发送，请填写验证码",
                "2fa_required": "需要两步验证密码",
                "logged_in": "已登录",
                "error": f"登录失败: {self._login_error}",
            }.get(self._login_state, self._login_state),
        }

    def _get_login_user(self) -> str:
        """获取当前登录用户。"""
        try:
            if self._client and self._client.is_connected():
                async def _me():
                    me = await self._client.get_me()
                    return me.username or me.first_name or ""
                return self._submit(_me(), timeout=10)
        except Exception:
            pass
        return ""

    async def api_login_logout(self) -> Dict[str, Any]:
        """注销 TG 登录。"""
        try:
            if self._client and self._loop and not self._loop.is_closed():
                async def _logout():
                    try:
                        await self._client.log_out()
                    except Exception:
                        pass
                    await self._client.disconnect()
                self._submit(_logout(), timeout=30)
            self._client = None
            self._client_ready = False
            self._login_state = "idle"
            self._login_qr_data = ""
            self._login_qr_login = None
            # 清除持久化 session
            self.save_data("tg_session", "")
            return {"success": True, "message": "已注销登录"}
        except Exception as e:
            return {"success": False, "message": f"注销失败: {e}"}

    # ==================== Bot 管理 API ====================

    async def api_bots(self, request: Request) -> Dict[str, Any]:
        """TG 音乐 Bot 管理 API（GET/POST/DELETE）。"""
        method = request.method
        if method == "GET":
            bots = self.get_data("tg_bots") or {}
            return {"success": True, "data": bots}
        elif method == "POST":
            try:
                body = await request.json() or {}
            except Exception:
                body = {}
            bots = self.get_data("tg_bots") or {}
            bot_id = str(body.get("bot_id") or f"bot_{int(time.time())}")
            bot_username = str(body.get("bot_username") or "").strip().lstrip("@")
            if not bot_username:
                return {"success": False, "message": "bot_username 不能为空"}
            bots[bot_id] = {
                "bot_username": bot_username,
                "name": body.get("name") or bot_username,
                "search_command": str(body.get("search_command") or "/search {keyword}").strip(),
                "button_index": int(body.get("button_index") or self._button_index or 1),
                "enabled": body.get("enabled", True),
                "created": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            self.save_data("tg_bots", bots)
            return {"success": True, "data": bots}
        elif method == "DELETE":
            try:
                body = await request.json() or {}
            except Exception:
                body = {}
            bot_id = body.get("bot_id")
            bots = self.get_data("tg_bots") or {}
            if bot_id and bot_id in bots:
                del bots[bot_id]
                self.save_data("tg_bots", bots)
                return {"success": True, "data": bots}
            return {"success": False, "message": f"Bot {bot_id} 不存在"}

    async def api_cleanup(self, request: Request) -> Dict[str, Any]:
        """清空插件数据（卸载前调用）。

        删除 tg_bots / tg_conn_status 等全部插件数据。
        注意：MP 卸载非分身插件时不自动删除 plugindata，
        卸载前应先调用本接口，或卸载后运行清理脚本。
        """
        cleared = []
        for key in ["tg_bots", "tg_conn_status"]:
            try:
                self.del_data(key)
                cleared.append(key)
            except Exception as e:
                logger.error(f"TG音乐站点：清理数据 {key} 失败: {e}")
        # 内存态同步清理
        self._last_search_results = []
        self._client_ready = False
        self._login_state = "idle"
        logger.info(f"TG音乐站点：插件数据已清空: {cleared}")
        return {"success": True, "message": f"已清空插件数据: {', '.join(cleared) or '无'}", "cleared": cleared}

    # ==================== 试搜索 API ====================

    async def api_try_search(self, request: Request) -> Dict[str, Any]:
        """Web 页面试搜索。"""
        try:
            body = await request.json() or {}
        except Exception:
            body = {}
        keyword = str(body.get("keyword") or "").strip()
        bot_username = str(body.get("bot_username") or "").strip().lstrip("@")
        if not keyword:
            return {"success": False, "message": "关键词不能为空"}
        if not self._client_ready:
            return {"success": False, "message": "TG 未登录，请先扫码登录"}
        try:
            results = await asyncio.to_thread(self._tg_search_bot, keyword, bot_username)
            return {"success": True, "count": len(results), "results": results}
        except Exception as e:
            return {"success": False, "message": f"搜索失败: {e}"}

    async def api_test(self) -> Dict[str, Any]:
        """测试 Telethon 连接状态。"""
        if not TELEGRAM_AVAILABLE:
            return {"success": False, "message": "Telethon 未安装"}
        if not self._api_id or not self._api_hash:
            return {"success": False, "message": "未配置 api_id/api_hash"}
        if self._client_ready and self._client and self._client.is_connected():
            user = ""
            try:
                async def _me():
                    me = await self._client.get_me()
                    return me.username or me.first_name or ""
                user = self._submit(_me(), timeout=10)
            except Exception:
                pass
            return {"success": True, "message": f"连接成功: {user}"}
        # 尝试临时连接验证
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            session_str = self.get_data("tg_session") or ""
            client = TelegramClient(
                StringSession(session_str) if session_str else StringSession(),
                self._api_id, self._api_hash,
                proxy=self._build_proxy(),
            )
            connected = await asyncio.to_thread(self._connect_test, client)
            if connected:
                user = await asyncio.to_thread(self._get_test_user, client)
                await asyncio.to_thread(self._disconnect_test, client)
                return {"success": True, "message": f"连接成功: {user}"}
            return {"success": False, "message": "连接失败：无法连接 Telegram 服务器"}
        except Exception as e:
            return {"success": False, "message": f"连接失败: {str(e)}"}

    async def api_history(self) -> Dict[str, Any]:
        """获取下载历史记录（含真实大小/时长/专辑等元数据）。"""
        try:
            history = self.get_data("tg_download_history") or []
            if not isinstance(history, list):
                history = []
            return {"success": True, "count": len(history), "data": history}
        except Exception as e:
            return {"success": False, "message": f"获取下载历史失败: {str(e)}", "data": []}

    async def api_logs(self) -> Dict[str, Any]:
        """读取插件日志文件尾部内容（最近 30 行）。"""
        try:
            log_path = Path(settings.CONFIG_PATH) / "logs" / "plugins" / "tgmusicsites.log"
            if not log_path.exists():
                return {"success": True, "data": [], "message": "日志文件不存在（插件尚未产生日志）"}
            lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            tail = lines[-30:]
            return {"success": True, "count": len(tail), "data": tail}
        except Exception as e:
            return {"success": False, "message": f"读取日志失败: {str(e)}", "data": []}

    @staticmethod
    def _connect_test(client: Any) -> bool:
        """测试连接（同步包装）。"""
        import asyncio as _asyncio
        try:
            _asyncio.new_event_loop().run_until_complete(client.connect())
            return bool(client.is_connected())
        except Exception:
            return False

    @staticmethod
    def _get_test_user(client: Any) -> str:
        """获取测试用户（同步包装）。"""
        import asyncio as _asyncio
        try:
            loop = _asyncio.new_event_loop()
            me = loop.run_until_complete(client.get_me())
            return me.username or me.first_name or ""
        except Exception:
            return ""

    @staticmethod
    def _disconnect_test(client: Any) -> None:
        """断开测试连接（同步包装）。"""
        import asyncio as _asyncio
        try:
            _asyncio.new_event_loop().run_until_complete(client.disconnect())
        except Exception:
            pass

    # ==================== 搜索拦截 ====================

    def tg_search_torrents(
        self,
        keyword: str,
        mtype: Optional[MediaType] = None,
        page: Optional[int] = 0,
        site: Optional[Any] = None,
    ) -> List[Any]:
        """胁持 search_torrents：仅处理音乐类型。

        site 参数为兼容 MP 调用（站点插件都带 site），TG 搜索不使用该参数。
        """
        if mtype != MediaType.MUSIC:
            return []
        if not self._should_trigger_tg_search(keyword):
            return []
        return self._tg_search(keyword, page)

    async def tg_async_search_torrents(
        self,
        keyword: str,
        mtype: Optional[MediaType] = None,
        page: Optional[int] = 0,
        site: Optional[Any] = None,
    ) -> List[Any]:
        """胁持 async_search_torrents：异步版本。"""
        if mtype != MediaType.MUSIC:
            return []
        if not self._should_trigger_tg_search(keyword):
            return []
        return await asyncio.to_thread(self._tg_search, keyword, page)

    def _should_trigger_tg_search(self, keyword: str) -> bool:
        """判断是否应该触发 TG 搜索（同一关键词 60 秒内只搜一次）。"""
        now = time.time()
        with self._search_lock:
            if keyword == self._last_search_key and now - self._last_search_time < 60:
                return False
            self._last_search_key = keyword
            self._last_search_time = now
            return True

    def _tg_search(self, keyword: str, page: Optional[int] = 0) -> List[Any]:
        """执行 TG bot 搜索（Telethon 直连），返回 TorrentInfo 列表。"""
        if page and page > 1:
            return []
        bots = self.get_data("tg_bots") or {}
        if not bots:
            logger.info(f"TG音乐站点：未配置 Bot，跳过搜索 '{keyword}'")
            return []
        try:
            all_results = []
            for bot_id, bot_cfg in bots.items():
                if not bot_cfg.get("enabled", True):
                    continue
                bot_username = bot_cfg.get("bot_username") or ""
                if not bot_username:
                    continue
                try:
                    results = self._tg_search_bot(keyword, bot_username, bot_cfg)
                    # 标记结果所属 bot
                    for r in results:
                        r["bot_username"] = bot_username
                        r["button_index"] = bot_cfg.get("button_index") or self._button_index or 1
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"TG音乐站点：Bot @{bot_username} 搜索失败: {e}")
            if not all_results:
                logger.info(f"TG音乐站点：搜索 '{keyword}' 无结果")
                return []
            # 缓存结果，供下载时使用
            with self._search_lock:
                self._last_search_results = all_results
            logger.info(f"TG音乐站点：搜索 '{keyword}' 返回 {len(all_results)} 条结果")
            return self._results_to_torrents(all_results)
        except Exception as e:
            logger.error(f"TG音乐站点搜索失败: {str(e)}")
            return []

    def _tg_search_bot(
        self, keyword: str, bot_username: str, bot_cfg: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """对单个 Bot 执行搜索，返回结果列表（含 button_data 供下载）。"""
        if not self._client_ready or not self._client:
            raise RuntimeError("TG 未登录")
        bot_cfg = bot_cfg or {}
        search_command = str(bot_cfg.get("search_command") or "/search {keyword}")
        button_index = int(bot_cfg.get("button_index") or self._button_index or 1)
        command = search_command.replace("{keyword}", keyword)
        logger.info(f"TG音乐站点：向 @{bot_username} 发送命令: {command}")
        async def _search():
            client = self._client
            # 解析 bot 实体
            try:
                bot_entity = await client.get_input_entity(bot_username)
            except Exception:
                bot_entity = bot_username
            # 记录发送前最新消息 id
            try:
                before = await client.get_messages(bot_entity, limit=1)
                before_id = before[0].id if before else 0
            except Exception:
                before_id = 0
            # 发送搜索命令
            await client.send_message(bot_entity, command)
            # 轮询回复（最多 search_timeout 秒）
            results = []
            deadline = time.time() + self._search_timeout
            seen_ids = set()
            while time.time() < deadline:
                await asyncio.sleep(1)
                try:
                    messages = await client.get_messages(bot_entity, limit=10)
                except Exception:
                    continue
                for m in messages:
                    if m.id <= before_id or m.id in seen_ids:
                        continue
                    if not m.message:
                        continue
                    seen_ids.add(m.id)
                    # 识别搜索结果消息（含数字编号特征）
                    parsed = self._parse_result_message(m)
                    if parsed:
                        results.extend(parsed)
                if results:
                    break
            return results
        try:
            return self._submit(_search(), timeout=self._search_timeout + 15)
        except Exception as e:
            logger.error(f"TG音乐站点：@{bot_username} 搜索异常: {e}")
            return []

    @staticmethod
    def _clean_title(raw: str) -> str:
        """清洗搜索结果标题：去尾部标点/分隔符噪声与重复空格。"""
        t = raw.strip()
        # 去掉尾部连续的标点/分隔符（、。.．-—_—/\\| 空格等）
        t = re.sub(r"[、。.．，,;；:：!！?？\-—_—/\\|\s]+$", "", t)
        # 压缩内部连续空格
        t = re.sub(r"\s{2,}", " ", t)
        return t.strip()

    def _parse_result_message(self, msg: Any) -> List[Dict[str, Any]]:
        """解析 Bot 搜索结果消息，提取候选与按钮数据。"""
        text = (msg.message or "").strip()
        if not text:
            return []
        # 寻找编号条目：数字. 标题 或 数字、标题
        entries = []
        for line in text.splitlines():
            line = line.strip()
            m = re.match(r"^(\d+)[.、．]\s*(.+)$", line)
            if m:
                title = self._clean_title(m.group(2))
                if title:
                    entries.append({"index": int(m.group(1)), "title": title})
        if not entries:
            return []
        # 提取按钮数据（reply_markup）——按条目序号取对应按钮，保证每条结果 uid 唯一
        buttons = []
        try:
            if msg.reply_markup and msg.reply_markup.rows:
                for row in msg.reply_markup.rows:
                    for btn in row.buttons:
                        buttons.append(btn)
        except Exception:
            pass
        results = []
        for e in entries:
            # 优先取与序号对应的按钮（index 从 1 开始），按钮不足时退化为第一个
            btn = buttons[e["index"] - 1] if 0 < e["index"] <= len(buttons) else (buttons[0] if buttons else None)
            button_data = ""
            button_text = ""
            if btn is not None:
                raw = getattr(btn, "data", b"") or b""
                if isinstance(raw, bytes):
                    button_data = base64.b64encode(raw).decode()
                else:
                    button_data = str(raw)
                button_text = getattr(btn, "text", "") or ""
            results.append({
                "index": e["index"],
                "title": e["title"],
                "description": text,
                "button_data": button_data,
                "button_text": button_text,
                "msg_id": getattr(msg, "id", 0),
            })
        return results

    def _results_to_torrents(self, results: List[Dict[str, Any]]) -> List[Any]:
        """将 TG 搜索结果转换为 TorrentInfo 列表。

        注意：必须用 app.core.context.TorrentInfo（有 to_dict 的普通类），
        MP 搜索链（chain/search.py -> api/endpoints/search.py）会对结果统一
        调用 torrent_info.to_dict()；app.schemas.context.TorrentInfo 是
        pydantic BaseModel 没有 to_dict，会抛 AttributeError。
        """
        from app.core.context import TorrentInfo
        torrents = []
        for r in results:
            button_data = r.get("button_data") or ""
            msg_id = r.get("msg_id") or 0
            # 构造唯一标识作为 enclosure
            uid = hashlib.md5(f"{msg_id}:{button_data}".encode()).hexdigest()[:16]
            # enclosure 用 magnet 伪链：MP download_torrent 对 magnet: 前缀直接返回该字符串，
            # 不会尝试打开 tg:// 链接；后续 self.download() -> run_module("download") 由
            # 本插件 tg_download 拦截识别 tgmusic-{uid} 标记，反查缓存触发真实 TG 下载。
            torrent = TorrentInfo()
            torrent.site = self._TG_SITE_ID
            # 站点名=bot 名：多个 bot 即多个站点（无 bot 时退回插件名）
            bot_name = (r.get("bot_username") or "").strip()
            torrent.site_name = bot_name or self._TG_SITE_NAME
            torrent.site_order = 0
            torrent.site_proxy = True
            torrent.title = r.get("title") or ""
            # 描述=单条结果（标题+序号），不再塞整段搜索过程文本
            idx = r.get("index") or 0
            torrent.description = f"{idx}. {torrent.title}" if idx and torrent.title else (torrent.title or "")
            torrent.enclosure = (
                f"magnet:?xt=urn:btih:{uid}{'0' * 24}"
                f"&dn={self._TG_MAGNET_MARKER}{uid}"
            )
            torrent.size = 0.0
            torrent.seeders = 0
            torrent.peers = 0
            torrent.grabs = 0
            torrent.pubdate = time.strftime("%Y-%m-%d %H:%M:%S")
            torrent.category = MediaType.MUSIC.value
            torrent.labels = ["TG音乐"]
            torrent.page_url = f"https://t.me/{r.get('bot_username') or ''}" if r.get("bot_username") else ""
            torrents.append(torrent)
        return torrents

    # ==================== 下载拦截 ====================

    def tg_download(
        self,
        content: Any,
        download_dir: Path,
        cookie: str,
        episodes=None,
        category: Optional[str] = None,
        label: Optional[str] = None,
        downloader: Optional[str] = None,
    ) -> Optional[Tuple[Optional[str], Optional[str], Optional[str], str]]:
        """胁持 download：当下载 TG 资源时走 Telethon 下载。"""
        # 只处理 TG magnet 伪链（含 tgmusic-{uid} 标记）
        if (
            isinstance(content, str)
            and content.startswith("magnet:?")
            and self._TG_MAGNET_MARKER in content
        ):
            logger.info(f"TG音乐站点：开始下载 TG 资源 {content[:60]}...")
            try:
                return self._tg_download_file(content, download_dir)
            except Exception as e:
                logger.error(f"TG音乐站点下载失败: {str(e)}")
                return None, None, None, f"TG下载失败: {str(e)}"
        # 兼容旧 tg://music/ 格式（历史缓存）
        if isinstance(content, str) and content.startswith(self._TG_URL_PREFIX):
            logger.info(f"TG音乐站点：开始下载 TG 资源 {content}")
            try:
                return self._tg_download_file(content, download_dir)
            except Exception as e:
                logger.error(f"TG音乐站点下载失败: {str(e)}")
                return None, None, None, f"TG下载失败: {str(e)}"
        # 非 TG 资源放行给系统模块
        return None

    def _tg_download_file(
        self, enclosure: str, download_dir: Path
    ) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
        """执行 TG bot 文件下载（Telethon 直连）。"""
        if not self._client_ready or not self._client:
            return None, None, None, "TG 未登录"
        # 从 enclosure 反查缓存的结果（支持 magnet 伪链与旧 tg:// 格式）
        if self._TG_MAGNET_MARKER in enclosure:
            uid = enclosure.split(self._TG_MAGNET_MARKER, 1)[1][:16]
        else:
            uid = enclosure.replace(self._TG_URL_PREFIX, "")
        result = None
        with self._search_lock:
            for r in self._last_search_results:
                button_data = r.get("button_data") or ""
                msg_id = r.get("msg_id") or 0
                if hashlib.md5(f"{msg_id}:{button_data}".encode()).hexdigest()[:16] == uid:
                    result = r
                    break
        if not result:
            return None, None, None, f"找不到 TG 资源 {enclosure} 的下载信息"
        # 执行下载
        try:
            target_dir = str(Path(self._download_dir))
            file_path = self._submit(
                self._do_download(result, target_dir),
                timeout=self._download_timeout + 60,
            )
            if not file_path:
                return None, None, None, "TG 下载失败：未获取到文件"
            # 记录下载历史（含真实大小/时长/专辑等元数据）
            try:
                history = self.get_data("tg_download_history") or []
                if not isinstance(history, list):
                    history = []
                info = dict(self._last_download_info or {})
                info.update({
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "title": result.get("title") or "",
                    "bot_username": result.get("bot_username") or "",
                })
                history.insert(0, info)
                # 最多保留 50 条
                self.save_data("tg_download_history", history[:50])
            except Exception as e:
                logger.warning(f"TG音乐站点：记录下载历史失败: {e}")
            fake_hash = hashlib.md5(str(file_path).encode()).hexdigest()[:40]
            logger.info(f"TG音乐站点：文件已下载到 {file_path}")
            return "TGMusic", fake_hash, "NoSubfolder", ""
        except Exception as e:
            logger.error(f"TG音乐站点下载异常: {str(e)}")
            return None, None, None, f"TG下载异常: {str(e)}"

    async def _do_download(self, result: Dict[str, Any], target_dir: str) -> Optional[str]:
        """在专用 loop 内执行下载：点击按钮 → 轮询新消息 → download_media。"""
        client = self._client
        msg_id = int(result.get("msg_id") or 0)
        button_data_b64 = result.get("button_data") or ""
        button_index = int(result.get("button_index") or self._button_index or 1)
        try:
            button_data = base64.b64decode(button_data_b64) if button_data_b64 else b""
            # 获取消息实体
            bot_username = result.get("bot_username") or ""
            try:
                bot_entity = await client.get_input_entity(bot_username) if bot_username else None
            except Exception:
                bot_entity = None
            # 点击按钮触发下载
            if bot_entity and msg_id and button_data:
                try:
                    await client(GetBotCallbackAnswerRequest(
                        peer=bot_entity,
                        msg_id=msg_id,
                        data=button_data,
                    ))
                    logger.info(f"TG音乐站点：已点击按钮 msg_id={msg_id} 触发下载")
                except Exception as e:
                    logger.info(f"TG音乐站点：点击按钮回调忽略（可能无需点击）: {e}")
            # 轮询 bot 发来的新文件消息
            Path(target_dir).mkdir(parents=True, exist_ok=True)
            deadline = time.time() + self._download_timeout
            file_path = ""
            last_id = msg_id
            while time.time() < deadline:
                await asyncio.sleep(2)
                try:
                    if bot_entity:
                        messages = await client.get_messages(bot_entity, limit=10)
                        newest_id = messages[0].id if messages else 0
                        for m in messages:
                            if m.out:
                                continue
                            if m.id <= last_id:
                                continue
                            last_id = max(last_id, m.id)
                            if m.media:
                                fname = self._media_filename(m)
                                logger.info(f"TG音乐站点：发现新媒体消息 id={m.id} name={fname}")
                                try:
                                    path = await client.download_media(m, file=target_dir)
                                except Exception as e:
                                    logger.info(f"TG音乐站点：download_media 失败: {e}")
                                    continue
                                if path:
                                    file_path = str(path)
                                    # 无后缀补 .mp3
                                    if not Path(file_path).suffix:
                                        new_path = f"{file_path}.mp3"
                                        Path(file_path).rename(new_path)
                                        file_path = new_path
                                    # 提取文件元数据（真实大小/时长/专辑/资源ID），供展示与日志
                                    info = self._extract_file_meta(m)
                                    info["file_path"] = file_path
                                    self._last_download_info = info
                                    logger.info(
                                        f"TG音乐站点：文件元数据 size={info.get('size')} "
                                        f"duration={info.get('duration')}s "
                                        f"album={info.get('album')} "
                                        f"resource={info.get('resource_id')}"
                                    )
                                    return file_path
                        logger.info(f"TG音乐站点：轮询中 已扫 {len(messages)} 条 最新id={newest_id} last_id={last_id} 剩余{int(deadline-time.time())}s")
                except Exception as e:
                    logger.debug(f"TG音乐站点：下载轮询异常: {e}")
            logger.info(f"TG音乐站点：轮询超时未获取到文件")
            return file_path or None
        except Exception as e:
            logger.error(f"TG音乐站点：_do_download 异常: {e}")
            return None

    def _extract_file_meta(self, m: Any) -> Dict[str, Any]:
        """提取文件消息元数据：真实大小/时长/专辑/资源ID/文件名。"""
        meta: Dict[str, Any] = {}
        try:
            # 真实大小与 mime（document）
            if m.media and hasattr(m.media, "document") and m.media.document:
                doc = m.media.document
                meta["size"] = doc.size
                meta["mime"] = doc.mime_type
                for a in doc.attributes:
                    name = type(a).__name__
                    if name == "DocumentAttributeAudio":
                        meta["duration"] = getattr(a, "duration", 0)
                        if getattr(a, "title", None):
                            meta["audio_title"] = a.title
                        if getattr(a, "performer", None):
                            meta["performer"] = a.performer
                    elif name == "DocumentAttributeFilename":
                        if getattr(a, "file_name", None):
                            meta["file_name"] = a.file_name
            # 文本里补专辑/资源ID："专辑：不散" / "大小：18.74MB" / "音乐ID：网易云音乐3339230677"
            text = (m.message or "") or ""
            m_album = re.search(r"专辑[：:](\S+)", text)
            if m_album:
                meta["album"] = m_album.group(1).strip()
            m_size = re.search(r"大小[：:]([0-9.]+\s*[KMGT]?B)", text, re.IGNORECASE)
            if m_size:
                meta["size_text"] = m_size.group(1)
            m_res = re.search(r"音乐ID[^：:\n]*[：:]\s*([\u4e00-\u9fa5A-Za-z0-9]+)", text)
            if m_res:
                meta["resource_id"] = m_res.group(1).strip()
        except Exception:
            pass
        return meta

    def _media_filename(self, m: Any) -> str:
        """提取媒体文件名（兼容 document/audio/voice/photo）。"""
        try:
            # Telethon .file 属性优先（含 name 与 mime_type）
            if m.file and m.file.name:
                return str(m.file.name)
            if m.media:
                if hasattr(m.media, "document") and m.media.document:
                    doc = m.media.document
                    for attr in doc.attributes:
                        if hasattr(attr, "file_name") and attr.file_name:
                            return str(attr.file_name)
                    # 无文件名：audio/voice 用 id 兜底，带后缀优先
                    for attr in doc.attributes:
                        if hasattr(attr, "title") and attr.title:
                            return f"tg_music_{m.id}.mp3"
                    return f"tg_music_{m.id}.mp3"
                if hasattr(m.media, "photo") and m.media.photo:
                    return f"tg_music_{m.id}.jpg"
        except Exception:
            pass
        return ""

