"""TgMusicSites 插件单测（pytest 原生）。

覆盖 get_module 注册、搜索拦截/放行、结果转 TorrentInfo 等核心逻辑。
依赖 MoviePilot 后端（app.*）与插件包：根 conftest 会先隔离 CONFIG_DIR 并把
后端、plugins.v2 注入 sys.path，再以顶层包名导入插件。
"""

import sys
import threading
from unittest.mock import patch

from app.schemas.types import MediaType
from tgmusicsites import TgMusicSites


def _initialized_plugin(enabled: bool = True) -> TgMusicSites:
    """创建指定启用状态的插件实例。

    用 object.__new__ 绕过 _PluginBase.__init__（会初始化 chain/helpers
    触发真实网络探测），手动设置插件属性——官方测试同款做法。
    """
    plugin = object.__new__(TgMusicSites)
    plugin._enabled = enabled
    plugin._bot_username = "music_v1bot"
    plugin._download_dir = "/qbs/torrents/music/"
    plugin._proxy_host = "127.0.0.1"
    plugin._proxy_port = 7891
    plugin._proxy_type = "socks5"
    plugin._api_id = 12345
    plugin._api_hash = "a" * 32
    plugin._session_string = "1" + "b" * 200
    plugin._search_timeout = 30
    plugin._download_timeout = 60
    plugin._button_index = 1
    plugin._search_lock = threading.Lock()
    plugin._last_search_key = ""
    plugin._last_search_time = 0.0
    plugin._last_search_results = []
    return plugin


def test_get_module_enabled():
    """启用时 get_module 应注册三个拦截方法。"""
    plugin = _initialized_plugin(enabled=True)
    modules = plugin.get_module()
    assert "search_torrents" in modules
    assert "async_search_torrents" in modules
    assert "download" in modules


def test_get_module_disabled():
    """禁用时 get_module 应返回空 dict（官方规范：未启用不注册模块）。"""
    plugin = _initialized_plugin(enabled=False)
    assert plugin.get_module() == {}


def test_get_state():
    """get_state 反映启用状态。"""
    assert _initialized_plugin(enabled=True).get_state() is True
    assert _initialized_plugin(enabled=False).get_state() is False


def test_non_music_passthrough():
    """非音乐类型搜索应放行（返回空列表，让系统模块继续）。"""
    plugin = _initialized_plugin(enabled=True)
    with patch.object(plugin, "_should_trigger_tg_search", return_value=True), \
         patch.object(plugin, "_tg_search", return_value=[{"title": "x"}]):
        result = plugin.tg_search_torrents(
            site={"id": 1}, keyword="沙丘", mtype=MediaType.MOVIE, page=0
        )
        assert result == []


def test_music_search_returns_torrents():
    """音乐搜索应返回 TorrentInfo 列表。"""
    plugin = _initialized_plugin(enabled=True)
    fake_torrent = {"index": 1, "title": "七里香", "description": "", "button_data": b"\x01\x02", "msg_id": 100, "bot_username": "music_v1bot"}
    # _tg_search 的真实实现会调 _results_to_torrents 转换；这里 patch 掉网络部分，
    # 让 _tg_search 直接返回转换后的 TorrentInfo（与真实流程一致）
    torrents = plugin._results_to_torrents([fake_torrent])
    with patch.object(plugin, "_should_trigger_tg_search", return_value=True), \
         patch.object(plugin, "_tg_search", return_value=torrents):
        result = plugin.tg_search_torrents(
            site={"id": 1}, keyword="周杰伦 七里香", mtype=MediaType.MUSIC, page=0
        )
        assert len(result) == 1
        torrent = result[0]
        assert torrent.title == "七里香"
        assert torrent.site == -1
        assert torrent.category == MediaType.MUSIC.value
        assert torrent.enclosure.startswith("magnet:?")
        assert "tgmusic-" in torrent.enclosure
        # 站点名=bot 名；描述=单条结果（非整段过程文本）
        assert torrent.site_name == "music_v1bot"
        assert torrent.description == "1. 七里香"


def test_results_to_torrents_enclosure_unique():
    """不同按钮数据的 enclosure 应唯一。"""
    plugin = _initialized_plugin(enabled=True)
    results = [
        {"title": "A", "button_data": b"\x01", "msg_id": 1},
        {"title": "B", "button_data": b"\x02", "msg_id": 1},
    ]
    torrents = plugin._results_to_torrents(results)
    assert len({t.enclosure for t in torrents}) == 2


def test_should_trigger_dedup():
    """同一关键词 60 秒内只触发一次 TG 搜索。"""
    plugin = _initialized_plugin(enabled=True)
    assert plugin._should_trigger_tg_search("周杰伦") is True
    assert plugin._should_trigger_tg_search("周杰伦") is False
    assert plugin._should_trigger_tg_search("林俊杰") is True


def test_tg_download_non_tg_passthrough():
    """非 TG 资源下载应放行（返回 None，让系统模块继续）。"""
    plugin = _initialized_plugin(enabled=True)
    assert plugin.tg_download("magnet:?xt=urn:btih:abc", None, "") is None


def test_stop_service_clears_worker_state():
    """stop_service 应清空 worker 状态，使 _start_telegram_worker 可重建线程。

    回归：旧版 stop_service 只断开 client 不清 _loop_thread，导致 init_plugin
    重建 worker 时因旧线程存活而跳过，session 永远无法恢复。
    """
    plugin = _initialized_plugin(enabled=True)
    # 模拟旧 worker 线程仍存活的状态
    fake_thread = threading.Thread(target=lambda: None)
    fake_thread.start()
    plugin._loop_thread = fake_thread
    plugin._loop = None
    plugin._client = None
    plugin._client_ready = True
    plugin._login_state = "logged_in"
    # 旧线程没有 loop（run_forever 未跑），stop_service 应能处理
    plugin.stop_service()
    assert plugin._loop_thread is None
    assert plugin._loop is None
    assert plugin._client_ready is False
    assert plugin._login_state == "idle"
    # stop 后应能重新启动 worker（不真启线程，验证不再被旧线程挡住）
    assert plugin._loop_thread is None


def test_clean_title():
    """标题清洗：去尾部噪声保留内部分隔符。"""
    from tgmusicsites import TgMusicSites
    assert TgMusicSites._clean_title("晴天 周杰伦- A-LNK") == "晴天 周杰伦- A-LNK"
    assert TgMusicSites._clean_title("晴天 B-KLl 周杰伦、") == "晴天 B-KLl 周杰伦"
    assert TgMusicSites._clean_title("晴天 周杰伦- ") == "晴天 周杰伦"
    assert TgMusicSites._clean_title("  七里香  ") == "七里香"


def test_extract_file_meta():
    """文件消息元数据提取：大小/时长/专辑/资源ID/文件名。"""
    from tgmusicsites import TgMusicSites
    class DocumentAttributeAudio:
        def __init__(s, **kw): s.__dict__.update(kw)
    class DocumentAttributeFilename:
        def __init__(s, **kw): s.__dict__.update(kw)
    class _Doc:
        size = 19647181
        mime_type = "audio/x-flac"
        attributes = [DocumentAttributeAudio(duration=182, title="晴天", performer="周杰伦- A-LNK"),
                      DocumentAttributeFilename(file_name="晴天-周杰伦- A-LNK.flac")]
    class _Media:
        document = _Doc()
    class _Msg:
        media = _Media()
        message = ("歌曲：晴天 - 周杰伦- A-LNK\n专辑：不散\n"
                   "#网易云音乐 #flac 大小：18.74MB 854.533kbps\n"
                   "音乐ID(可在内联查询使用)：\n网易云音乐3339230677\n"
                   "via @music_v1bot")
    meta = TgMusicSites()._extract_file_meta(_Msg())
    assert meta["size"] == 19647181
    assert meta["duration"] == 182
    assert meta["album"] == "不散"
    assert meta["size_text"] == "18.74MB"
    assert meta["resource_id"] == "网易云音乐3339230677"
    assert meta["file_name"] == "晴天-周杰伦- A-LNK.flac"
