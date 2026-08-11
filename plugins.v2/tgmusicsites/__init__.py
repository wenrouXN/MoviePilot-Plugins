"""TG音乐站点插件：将 Telegram 音乐 Bot 作为音乐资源站点接入 MoviePilot V3 搜索链。

v0.2.0 重大变更：Telethon 通信层抽离为独立服务 tg-music-bridge（NextFind 式集成）。
插件只负责调 bridge 的 HTTP API，不再维护 Telethon/代理/节点/会话。
"""

from __future__ import annotations

import asyncio
import hashlib
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
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class TgMusicSites(_PluginBase):
    """TG音乐站点插件。"""

    # 插件元数据
    plugin_name = "TG音乐站点"
    plugin_desc = "将 Telegram 音乐 Bot 作为音乐资源站点接入 MoviePilot V3 搜索链，通过轻量桥接服务 tg-music-bridge 实现 TG 搜索与下载。"
    plugin_icon = "Telegram_A.png"
    plugin_version = "0.2.0"
    plugin_label = "音乐,Telegram,资源站"
    plugin_author = "wenrouXN"
    plugin_config_prefix = "tgmusicsites_"
    plugin_order = 10
    auth_level = 1

    # 运行状态
    _enabled = False
    _bot_username = ""
    _download_dir = ""
    _bridge_url = ""
    _search_timeout = 30
    _download_timeout = 60
    _button_index = 1

    # TG 站点标记
    _TG_SITE_ID = -1
    _TG_SITE_NAME = "TG音乐"
    _TG_URL_PREFIX = "tg://music/"

    # 搜索会话去重
    _search_lock = threading.Lock()
    _last_search_key = ""
    _last_search_time = 0.0
    _last_search_results: List[Dict[str, Any]] = []

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._enabled = False
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        if not self._enabled:
            return
        self._bot_username = str(config.get("bot_username") or "music_v1bot").strip()
        self._download_dir = str(config.get("download_dir") or "/qbs/torrents/music/").strip()
        self._bridge_url = str(config.get("bridge_url") or "").strip().rstrip("/")
        self._search_timeout = int(config.get("search_timeout") or 30)
        self._download_timeout = int(config.get("download_timeout") or 60)
        self._button_index = int(config.get("button_index") or 1)

        if not HTTPX_AVAILABLE:
            logger.error("TG音乐站点插件：httpx 未安装")
            self._enabled = False
            return
        if not self._bridge_url:
            logger.error("TG音乐站点插件：未配置桥接服务地址（bridge_url）")
            self._enabled = False
            return
        if not self._bridge_url.startswith("http"):
            logger.error("TG音乐站点插件：bridge_url 格式错误（需 http://host:port）")
            self._enabled = False
            return
        logger.info("TG音乐站点插件已启用：bot=%s，bridge=%s，下载目录=%s",
                    self._bot_username, self._bridge_url, self._download_dir)

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
                "path": "/sites",
                "endpoint": self.api_sites,
                "methods": ["GET", "POST", "DELETE"],
                "summary": "TG音乐站点生命周期管理",
                "description": "查询/添加/删除 TG 音乐站点配置",
            },
            {
                "path": "/test",
                "endpoint": self.api_test,
                "methods": ["GET"],
                "summary": "测试 TG 音乐 Bot 连接",
                "description": "测试 tg-music-bridge 服务连接",
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
        """返回插件配置表单与默认配置。TG 登录由 tg-music-bridge 服务负责，插件只需配置桥接地址。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VSwitch",
                        "props": {"model": "enabled", "label": "启用插件"}
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "bridge_url",
                            "label": "桥接服务地址",
                            "hint": "tg-music-bridge 服务地址，如 http://192.168.1.68:8300"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "bot_username",
                            "label": "TG 音乐 Bot 用户名",
                            "hint": "默认 music_v1bot"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "download_dir",
                            "label": "音乐下载目录",
                            "hint": "默认 /qbs/torrents/music/"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {"model": "search_timeout", "label": "搜索超时(秒)", "hint": "默认 30"}
                    },
                    {
                        "component": "VTextField",
                        "props": {"model": "download_timeout", "label": "下载超时(秒)", "hint": "默认 60"}
                    },
                    {
                        "component": "VTextField",
                        "props": {"model": "button_index", "label": "默认按钮序号", "hint": "默认 1"}
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "dense": True,
                            "class": "mt-2",
                            "text": "Telegram 登录由独立服务 tg-music-bridge 负责（凭据在服务端配置，长期有效）。插件仅通过 HTTP 调用该服务，无需 Telethon/代理/会话配置。"
                        }
                    }
                ]
            }
        ], {
            "enabled": False,
            "bridge_url": "http://192.168.1.68:8300",
            "bot_username": "music_v1bot",
            "download_dir": "/qbs/torrents/music/",
            "search_timeout": 30,
            "download_timeout": 60,
            "button_index": 1
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面（Vuetify JSON）。"""
        if not self._enabled:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "warning",
                        "text": "插件未启用，请在插件配置中启用后查看详情。"
                    },
                }
            ]
        sites = self.get_data("tg_sites") or {}
        conn = self.get_data("tg_conn_status") or {}
        conn_state = "未测试"
        if conn.get("success") is True:
            conn_state = f"✅ 已连接 ({conn.get('time', '')})"
        elif conn.get("success") is False:
            conn_state = f"❌ 连接失败 ({conn.get('time', '')})"
        bridge_ok = bool(self._bridge_url)
        bridge_state = f"✅ {self._bridge_url}" if bridge_ok else "❌ 未配置"
        # 默认站点 = 配置的 bot（自动生成，无需手动添加）；tg_sites 里的为附加站点
        default_site = {
            "component": "div",
            "props": {"class": "d-flex align-center justify-space-between py-1"},
            "content": [
                {
                    "component": "div",
                    "props": {"class": "text-body-2"},
                    "text": f"⭐ 默认站点：@{self._bot_username}（配置项）",
                },
                {
                    "component": "div",
                    "props": {"class": "text-body-2 text-grey"},
                    "text": "启用中",
                },
            ],
        }
        extra_rows = [
            {
                "component": "div",
                "props": {"class": "d-flex align-center justify-space-between py-1"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "text-body-2"},
                        "text": f"{v.get('name', 'TG音乐')} (@{v.get('bot_username', '')})",
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
                                "api": "plugin/TgMusicSites/sites",
                                "method": "delete",
                                "params": {"site_id": k},
                            }
                        },
                    },
                ],
            }
            for k, v in sites.items()
        ]
        site_rows = [default_site] + (extra_rows or [])
        return [
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
                                        "text": "运行状态",
                                    },
                                    {
                                        "component": "VCardText",
                                        "props": {"class": "py-2"},
                                        "content": [
                                            {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"Bot：@{self._bot_username}"},
                                            {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"下载目录：{self._download_dir}"},
                                            {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"桥接服务：{bridge_state}"},
                                            {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"连接状态：{conn_state}"},
                                        ],
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
                                                    "prepend-icon": "mdi-connection",
                                                },
                                                "text": "测试连接",
                                                "events": {
                                                    "click": {
                                                        "api": "plugin/TgMusicSites/test",
                                                        "method": "get",
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
                                        "text": "站点管理",
                                    },
                                    {
                                        "component": "VCardText",
                                        "props": {"class": "py-2"},
                                        "content": [
                                            {"component": "div", "props": {"class": "text-body-2 py-1"}, "text": f"站点数：1 个默认 + {len(sites)} 个附加"},
                                            {"component": "div", "props": {"class": "text-body-2 py-1 text-grey"}, "text": "默认站点由配置 bot_username 自动生成，无需手动添加；附加站点通过 API 管理"},
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
                "props": {"variant": "outlined", "class": "mt-2"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "text-subtitle-1 font-weight-bold"},
                        "text": "TG 站点列表",
                    },
                    {
                        "component": "VCardText",
                        "props": {"class": "py-2"},
                        "content": site_rows,
                    },
                ],
            },
        ]

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        pass

    # ==================== API ====================

    async def api_sites(self, request: Request) -> Dict[str, Any]:
        """TG音乐站点生命周期管理 API。"""
        method = request.method
        if method == "GET":
            sites = self.get_data("tg_sites") or {}
            return {"success": True, "data": sites}
        elif method == "POST":
            try:
                body = await request.json() or {}
            except Exception:
                body = {}
            sites = self.get_data("tg_sites") or {}
            site_id = str(body.get("site_id") or f"tg_{int(time.time())}")
            sites[site_id] = {
                "bot_username": body.get("bot_username") or self._bot_username,
                "name": body.get("name") or "TG音乐",
                "enabled": body.get("enabled", True),
                "created": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            self.save_data("tg_sites", sites)
            return {"success": True, "data": sites}
        elif method == "DELETE":
            try:
                body = await request.json() or {}
            except Exception:
                body = {}
            site_id = body.get("site_id")
            sites = self.get_data("tg_sites") or {}
            if site_id and site_id in sites:
                del sites[site_id]
                self.save_data("tg_sites", sites)
                return {"success": True, "data": sites}
            return {"success": False, "message": f"站点 {site_id} 不存在"}

    async def api_test(self) -> Dict[str, Any]:
        """测试 tg-music-bridge 服务连接，并将结果保存供详情页展示。"""
        if not self._bridge_url:
            result = {"success": False, "message": "未配置桥接服务地址"}
            self.save_data("tg_conn_status", {
                "time": time.strftime("%m-%d %H:%M:%S"),
                "success": False,
                "message": result["message"],
            })
            return result
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{self._bridge_url}/health")
                data = resp.json()
            if resp.status_code == 200 and data.get("connected"):
                user = data.get("user") or ""
                result = {"success": True, "message": f"连接成功: {user}"}
            else:
                result = {"success": False, "message": f"桥接服务未连接: {data.get('last_conn', {}).get('message', 'unknown')}"}
        except Exception as e:
            result = {"success": False, "message": f"连接失败: {str(e)}"}
        self.save_data("tg_conn_status", {
            "time": time.strftime("%m-%d %H:%M:%S"),
            "success": result["success"],
            "message": result["message"],
        })
        return result

    # ==================== Bridge HTTP 客户端 ====================

    def _bridge_call(self, path: str, payload: Optional[Dict[str, Any]] = None,
                     timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """调用 bridge HTTP API（同步包装，供 asyncio.to_thread 使用）。"""
        import urllib.request
        import urllib.error
        import json as _json
        url = f"{self._bridge_url}{path}"
        data = _json.dumps(payload).encode() if payload else None
        req = urllib.request.Request(url, data=data)
        if payload:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=timeout or max(self._search_timeout + 10, 60)) as resp:
                return _json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            try:
                return _json.loads(e.read().decode())
            except Exception:
                return {"success": False, "message": f"HTTP {e.code}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ==================== 搜索拦截 ====================

    def tg_search_torrents(
        self,
        keyword: str,
        mtype: Optional[MediaType] = None,
        page: Optional[int] = 0,
    ) -> List[Any]:
        """胁持 search_torrents：仅处理音乐类型。"""
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
        """执行 TG bot 搜索（调 bridge），返回 TorrentInfo 列表。"""
        if page and page > 1:
            return []
        if not self._bot_username:
            logger.error("TG音乐站点：未配置 bot 用户名")
            return []
        try:
            resp = self._bridge_call("/search", {
                "keyword": keyword,
                "bot_username": self._bot_username,
            })
            if not resp or not resp.get("success"):
                logger.error(f"TG音乐站点：bridge 搜索失败: {resp}")
                return []
            results = resp.get("results") or []
            if not results:
                logger.info(f"TG音乐站点：搜索 '{keyword}' 无结果")
                return []
            # 缓存结果，供下载时使用
            with self._search_lock:
                self._last_search_results = results
            logger.info(f"TG音乐站点：搜索 '{keyword}' 返回 {len(results)} 条结果")
            return self._results_to_torrents(results)
        except Exception as e:
            logger.error(f"TG音乐站点搜索失败: {str(e)}")
            return []

    def _results_to_torrents(self, results: List[Dict[str, Any]]) -> List[Any]:
        """将 TG 搜索结果转换为 TorrentInfo 列表。"""
        from app.schemas.context import TorrentInfo
        torrents = []
        for r in results:
            button_data = r.get("button_data") or ""
            msg_id = r.get("msg_id") or 0
            # 构造唯一标识作为 enclosure
            uid = hashlib.md5(f"{msg_id}:{button_data}".encode()).hexdigest()[:16]
            torrent = TorrentInfo(
                site=self._TG_SITE_ID,
                site_name=self._TG_SITE_NAME,
                site_order=0,
                site_proxy=True,
                title=r.get("title") or "",
                description=r.get("description") or "",
                enclosure=f"{self._TG_URL_PREFIX}{uid}",
                size=0.0,
                seeders=0,
                peers=0,
                grabs=0,
                pubdate=time.strftime("%Y-%m-%d %H:%M:%S"),
                category=MediaType.MUSIC.value,
                labels=["TG音乐"],
                page_url=f"https://t.me/{self._bot_username}",
            )
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
        """胁持 download：当下载 TG 资源时走 bridge 下载。"""
        # 只处理 TG 链接
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
        """执行 TG bot 文件下载（调 bridge）。"""
        # 从 enclosure 反查缓存的结果
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
        # 执行下载（调 bridge）
        try:
            target_dir = str(Path(self._download_dir))
            resp = self._bridge_call("/download", {
                "msg_id": result.get("msg_id"),
                "button_data": result.get("button_data"),
                "target_dir": target_dir,
            }, timeout=self._download_timeout + 60)
            if not resp or not resp.get("success"):
                return None, None, None, f"TG 下载失败: {(resp or {}).get('message', 'bridge 无响应')}"
            file_path = resp.get("file") or ""
            if not file_path:
                return None, None, None, "TG 下载失败：bridge 未返回文件路径"
            # 生成伪 hash（用文件路径 md5）
            fake_hash = hashlib.md5(str(file_path).encode()).hexdigest()[:40]
            logger.info(f"TG音乐站点：文件已下载到 {file_path}")
            return "TGMusic", fake_hash, "NoSubfolder", ""
        except Exception as e:
            logger.error(f"TG音乐站点下载异常: {str(e)}")
            return None, None, None, f"TG下载异常: {str(e)}"
