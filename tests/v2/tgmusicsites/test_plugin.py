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
    plugin._bridge_url = "http://127.0.0.1:8300"
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
            keyword="沙丘", mtype=MediaType.MOVIE, page=0
        )
        assert result == []


def test_music_search_returns_torrents():
    """音乐搜索应返回 TorrentInfo 列表。"""
    plugin = _initialized_plugin(enabled=True)
    fake_torrent = {"title": "七里香", "description": "", "button_data": "AQI=", "msg_id": 100}
    torrents = plugin._results_to_torrents([fake_torrent])
    with patch.object(plugin, "_should_trigger_tg_search", return_value=True), \
         patch.object(plugin, "_tg_search", return_value=torrents):
        result = plugin.tg_search_torrents(
            keyword="周杰伦 七里香", mtype=MediaType.MUSIC, page=0
        )
        assert len(result) == 1
        torrent = result[0]
        assert torrent.title == "七里香"
        assert torrent.site == -1
        assert torrent.category == MediaType.MUSIC.value
        assert torrent.enclosure.startswith("tg://music/")


def test_results_to_torrents_enclosure_unique():
    """不同按钮数据的 enclosure 应唯一。"""
    plugin = _initialized_plugin(enabled=True)
    results = [
        {"title": "A", "button_data": "AQ==", "msg_id": 1},
        {"title": "B", "button_data": "Ag==", "msg_id": 1},
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


def test_tg_download_bridge_call():
    """TG 资源下载应调 bridge /download 并返回 (TGMusic, hash, NoSubfolder, '')。"""
    plugin = _initialized_plugin(enabled=True)
    fake_result = {"title": "七里香", "button_data": "AQI=", "msg_id": 100}
    uid = plugin._results_to_torrents([fake_result])[0].enclosure.split("/")[-1]
    with patch.object(plugin, "_last_search_results", [fake_result]):
        with patch.object(plugin, "_bridge_call", return_value={
            "success": True, "file": "/qbs/torrents/music/七里香.flac"
        }) as mock_call:
            result = plugin._tg_download_file(f"tg://music/{uid}", None)
            assert result[0] == "TGMusic"
            assert result[2] == "NoSubfolder"
            assert result[3] == ""
            mock_call.assert_called_once()
            call_kwargs = mock_call.call_args
            assert call_kwargs[0][1]["msg_id"] == 100
