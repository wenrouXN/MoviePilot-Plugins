"""TG音乐站点插件：将 Telegram 音乐 Bot 作为音乐资源站点接入 MoviePilot V3 搜索链。"""

from __future__ import annotations

import asyncio
import hashlib
import os
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

# 尝试导入 telethon，失败时给友好提示
try:
    from telethon import TelegramClient, functions
    from telethon.sessions import StringSession
    from telethon.network import ConnectionTcpFull
    from telethon.errors import SessionPasswordNeededError
    import socks
    TELE_THON_AVAILABLE = True
except ImportError:
    TELE_THON_AVAILABLE = False


class TgMusicSites(_PluginBase):
    """TG音乐站点插件。"""

    # 插件元数据
    plugin_name = "TG音乐站点"
    plugin_desc = "将 Telegram 音乐 Bot 作为音乐资源站点接入 MoviePilot V3 搜索链，音乐订阅刷新时自动搜索并下载 TG 音乐资源。"
    plugin_icon = "Telegram_A.png"
    plugin_version = "0.1.0"
    plugin_label = "音乐,Telegram,资源站"
    plugin_author = "wenrouXN"
    plugin_config_prefix = "tgmusicsites_"
    plugin_order = 10
    auth_level = 1

    # 运行状态
    _enabled = False
    _bot_username = ""
    _download_dir = ""
    _proxy_host = ""
    _proxy_port = 0
    _api_id = 0
    _api_hash = ""
    _session_string = ""
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
        self._proxy_host = str(config.get("proxy_host") or "").strip()
        self._proxy_port = int(config.get("proxy_port") or 0)
        self._proxy_type = str(config.get("proxy_type") or "socks5").strip().lower()
        self._api_id = int(config.get("api_id") or 0)
        self._api_hash = str(config.get("api_hash") or "").strip()
        self._session_string = str(config.get("session_string") or "").strip()
        self._search_timeout = int(config.get("search_timeout") or 30)
        self._download_timeout = int(config.get("download_timeout") or 60)
        self._button_index = int(config.get("button_index") or 1)

        # 兼容从环境变量读取凭据
        if not self._api_id:
            self._api_id = int(os.environ.get("TELEGRAM_API_ID") or 0)
        if not self._api_hash:
            self._api_hash = os.environ.get("TELEGRAM_API_HASH") or ""
        if not self._session_string:
            self._session_string = os.environ.get("TELEGRAM_SESSION_STRING") or ""

        if not TELE_THON_AVAILABLE:
            logger.error("TG音乐站点插件：telethon 未安装，请安装 requirements.txt")
            self._enabled = False
            return
        if not self._api_id or not self._api_hash or not self._session_string:
            logger.error("TG音乐站点插件：Telegram 凭据不完整（api_id/api_hash/session_string）")
            self._enabled = False
            return
        logger.info("TG音乐站点插件已启用：bot=%s，下载目录=%s", self._bot_username, self._download_dir)

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
                "description": "测试 Telethon 连接与搜索",
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
                        "component": "VSwitch",
                        "props": {"model": "enabled", "label": "启用插件"}
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
                        "props": {"model": "api_id", "label": "Telegram API ID"}
                    },
                    {
                        "component": "VTextField",
                        "props": {"model": "api_hash", "label": "Telegram API Hash"}
                    },
                    {
                        "component": "VTextField",
                        "props": {"model": "session_string", "label": "Telegram Session String"}
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "proxy_host",
                            "label": "代理主机",
                            "hint": "如 127.0.0.1，留空直连"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {"model": "proxy_port", "label": "代理端口", "hint": "如 7890 (HTTP) / 7891 (SOCKS5)"}
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
                    }
                ]
            }
        ], {
            "enabled": False,
            "bot_username": "music_v1bot",
            "download_dir": "/qbs/torrents/music/",
            "api_id": "",
            "api_hash": "",
            "session_string": "",
            "proxy_host": "192.168.1.68",
            "proxy_port": 7891,
            "proxy_type": "socks5",
            "search_timeout": 30,
            "download_timeout": 60,
            "button_index": 1
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面。"""
        if not self._enabled:
            return None
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "text": f"TG音乐站点插件已启用。Bot: @{self._bot_username}，下载目录: {self._download_dir}"
                }
            }
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
        """测试 TG 音乐 Bot 连接。"""
        client = None
        try:
            client = self._get_client()
            if not client:
                return {"success": False, "message": "Telethon 客户端创建失败"}
            # Telethon connect() 成功时返回 None（仅失败时返回 False/抛异常），不能按真值判断
            await client.connect()
            me = await client.get_me()
            return {"success": True, "message": f"连接成功: {me.first_name}"}
        except Exception as e:
            return {"success": False, "message": f"连接失败: {str(e)}"}
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    # ==================== Telethon 客户端 ====================

    def _get_client(self) -> Optional[TelegramClient]:
        """创建 Telethon 客户端（每次新建，避免跨事件循环复用导致 loop 冲突）。"""
        if not TELE_THON_AVAILABLE:
            return None
        try:
            # 修复 session 缺失的 base64 padding（StringSession 要求去版本号后为 4 的倍数）
            session_str = self._session_string
            if session_str:
                body = session_str[1:] if session_str[0].isdigit() else session_str
                if len(body) % 4:
                    session_str = session_str + "=" * (4 - len(body) % 4)
            proxy = None
            if self._proxy_host and self._proxy_port:
                if self._proxy_type == "socks5":
                    proxy = (socks.SOCKS5, self._proxy_host, self._proxy_port)
                else:
                    proxy = ("http", self._proxy_host, self._proxy_port)
            return TelegramClient(
                StringSession(session_str),
                self._api_id,
                self._api_hash,
                proxy=proxy,
                connection=ConnectionTcpFull,
            )
        except Exception as e:
            logger.error(f"创建 Telethon 客户端失败: {str(e)}")
            return None

    def _run_async(self, coro):
        """在独立事件循环中运行异步协程（线程安全）。"""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.close()
            except Exception:
                pass

    # ==================== 搜索拦截 ====================

    def tg_search_torrents(
        self,
        site: dict,
        keyword: str,
        mtype: Optional[MediaType] = None,
        page: Optional[int] = 0,
    ) -> List[Any]:
        """胁持 search_torrents：当搜索音乐时注入 TG 站点结果。"""
        # 只处理音乐类型，非音乐放行给系统模块
        if mtype != MediaType.MUSIC:
            return []
        # 同一关键词 60 秒内只搜索一次 TG（多个站点并发调用时去重）
        if not self._should_trigger_tg_search(keyword):
            return []
        return self._tg_search(keyword, page)

    async def tg_async_search_torrents(
        self,
        site: dict,
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
        """执行 TG bot 搜索，返回 TorrentInfo 列表。"""
        if page and page > 1:
            return []
        if not self._bot_username:
            logger.error("TG音乐站点：未配置 bot 用户名")
            return []
        try:
            # 整个链路（创建 client + 搜索 + 断开）必须在同一个事件循环内执行
            results = self._run_async(self._search_flow(keyword))
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

    async def _search_flow(self, keyword: str) -> List[Dict[str, Any]]:
        """在同一个事件循环内完成 client 创建、搜索、断开。"""
        client = None
        try:
            client = self._get_client()
            if not client:
                logger.error("TG音乐站点：客户端不可用")
                return []
            return await self._search_bot(client, self._bot_username, keyword)
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    async def _search_bot(self, client: TelegramClient, bot: str, query: str) -> List[Dict[str, Any]]:
        """在 bot 中搜索音乐，返回候选列表。"""
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("TG音乐站点：会话未授权")
            return []
        bot_entity = await client.get_entity(bot)
        # 发送搜索指令
        await client.send_message(bot_entity, f"/search {query}")
        # 等待结果消息
        timeout = self._search_timeout
        start = time.time()
        result_msgs = []
        while time.time() - start < timeout:
            await asyncio.sleep(1)
            msgs = await client.get_messages(bot_entity, limit=5)
            for m in msgs:
                if m.out:
                    continue
                if not m.message:
                    continue
                text = m.message
                # 结果消息特征：包含"搜索结果"和编号
                if "搜索结果" in text and re.search(r"\d+\.", text):
                    result_msgs.append(m)
            if result_msgs:
                break
        if not result_msgs:
            return []
        # 解析候选
        msg = result_msgs[0]
        candidates = self._parse_search_results(msg.message)
        buttons = self._extract_buttons(msg)
        # 候选和按钮按序号对齐，只有有按钮的候选才能下载
        paired = []
        for i, cand in enumerate(candidates):
            if i < len(buttons):
                cand["button_data"] = buttons[i].get("data")
                cand["button_text"] = buttons[i].get("text")
                cand["msg_id"] = msg.id
                paired.append(cand)
        return paired

    @staticmethod
    def _parse_search_results(text: str) -> List[Dict[str, Any]]:
        """解析 bot 搜索结果文本为候选列表，支持单行和多行格式。"""
        candidates = []
        idx = 0
        # 匹配 "数字. 标题" 片段，标题到下一个 "数字. " 或行尾
        pattern = re.compile(r"(?:^|\s)(\d+)\.\s+([^0-9][^。]*?)(?=\s+\d+\.\s+|$)")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            for m in pattern.finditer(line):
                idx += 1
                title = m.group(2).strip()
                candidates.append({
                    "index": idx,
                    "title": title,
                    "description": "",
                })
        return candidates

    @staticmethod
    def _extract_buttons(msg) -> List[Dict[str, Any]]:
        """提取消息的内联按钮。"""
        buttons = []
        if getattr(msg, "reply_markup", None):
            rows = getattr(msg.reply_markup, "rows", []) or []
            for row in rows:
                for btn in getattr(row, "buttons", []) or []:
                    buttons.append({
                        "text": getattr(btn, "text", ""),
                        "data": getattr(btn, "data", b""),
                    })
        return buttons

    def _results_to_torrents(self, results: List[Dict[str, Any]]) -> List[Any]:
        """将 TG 搜索结果转换为 TorrentInfo 列表。"""
        from app.schemas.context import TorrentInfo
        torrents = []
        for r in results:
            button_data = r.get("button_data") or b""
            msg_id = r.get("msg_id") or 0
            # 构造唯一标识作为 enclosure
            uid = hashlib.md5(f"{msg_id}:{button_data.hex()}".encode()).hexdigest()[:16]
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
        """胁持 download：当下载 TG 资源时走 Telethon 下载。"""
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
        """执行 TG bot 文件下载。"""
        # 从 enclosure 反查缓存的结果
        uid = enclosure.replace(self._TG_URL_PREFIX, "")
        result = None
        with self._search_lock:
            for r in self._last_search_results:
                button_data = r.get("button_data") or b""
                msg_id = r.get("msg_id") or 0
                if hashlib.md5(f"{msg_id}:{button_data.hex()}".encode()).hexdigest()[:16] == uid:
                    result = r
                    break
        if not result:
            return None, None, None, f"找不到 TG 资源 {enclosure} 的下载信息"
        # 执行下载
        try:
            target_dir = Path(self._download_dir)
            target_dir.mkdir(parents=True, exist_ok=True)
            # 整个链路（创建 client + 下载 + 断开）必须在同一个事件循环内执行
            file_path = self._run_async(
                self._download_flow(
                    result.get("msg_id"),
                    result.get("button_data"),
                    target_dir,
                )
            )
            if not file_path:
                return None, None, None, "TG 下载失败：未获取到文件"
            # 生成伪 hash（用文件路径 md5）
            fake_hash = hashlib.md5(str(file_path).encode()).hexdigest()[:40]
            logger.info(f"TG音乐站点：文件已下载到 {file_path}")
            return "TGMusic", fake_hash, "NoSubfolder", ""
        except Exception as e:
            logger.error(f"TG音乐站点下载异常: {str(e)}")
            return None, None, None, f"TG下载异常: {str(e)}"

    async def _download_flow(
        self, msg_id: int, button_data: bytes, target_dir: Path
    ) -> Optional[Path]:
        """在同一个事件循环内完成 client 创建、下载、断开。"""
        client = None
        try:
            client = self._get_client()
            if not client:
                return None
            return await self._download_bot_file(
                client,
                self._bot_username,
                msg_id,
                button_data,
                target_dir,
            )
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    async def _download_bot_file(
        self,
        client: TelegramClient,
        bot: str,
        msg_id: int,
        button_data: bytes,
        target_dir: Path,
    ) -> Optional[Path]:
        """点击按钮并下载文件。"""
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("TG音乐站点：会话未授权")
            return None
        bot_entity = await client.get_entity(bot)
        # 获取源消息
        src_msg = await client.get_messages(bot_entity, ids=msg_id)
        if not src_msg:
            logger.error("TG音乐站点：找不到源消息")
            return None
        # 点击回调按钮
        try:
            await client(functions.messages.GetBotCallbackAnswerRequest(
                peer=bot_entity,
                msg_id=msg_id,
                data=button_data,
            ))
        except Exception as e:
            logger.warn(f"TG音乐站点：回调按钮失败（可能无需点击）: {str(e)}")
        # 等待新文件消息
        timeout = self._download_timeout
        start = time.time()
        file_msg = None
        while time.time() - start < timeout:
            await asyncio.sleep(1)
            msgs = await client.get_messages(bot_entity, limit=5)
            for m in msgs:
                if m.out:
                    continue
                if m.media and m.id > msg_id:
                    file_msg = m
                    break
            if file_msg:
                break
        if not file_msg:
            logger.error("TG音乐站点：等待文件消息超时")
            return None
        # 下载文件
        if file_msg.file:
            filename = file_msg.file.name or f"music_{msg_id}"
            if not Path(filename).suffix:
                filename += ".mp3"
            target = target_dir / filename
            await client.download_media(file_msg, file=target)
            if target.exists() and target.stat().st_size > 0:
                return target
        return None
