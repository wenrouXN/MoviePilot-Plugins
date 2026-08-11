# tgmusic-sites

将 Telegram 音乐 Bot（@music_v1bot）作为音乐资源站点接入 MoviePilot V3 搜索链的官方插件。

## 原理

MoviePilot V3 的搜索链（SearchChain）通过 `run_module` / `async_run_module` 分发方法调用，插件可用 `get_module()` 声明"胁持"系统模块实现：

| 胁持方法 | 作用 |
|---------|------|
| `search_torrents` / `async_search_torrents` | MP 搜索音乐时，同时调用 TG bot 搜索，把结果转成 TorrentInfo 与 PT 站点结果合并 |
| `download` | MP 要下载 TG 资源（enclosure 为 `tg://music/...`）时，走 Telethon 回调下载文件，绕过 qBittorrent |

## 工作流程

1. MP 音乐订阅刷新 → SearchChain 遍历站点调 `search_torrents`
2. 插件拦截：调 @music_v1bot `/search {keyword}` → 解析结果列表（带按钮）→ 转成 TorrentInfo（enclosure 标记 `tg://music/{uid}`）
3. MP 选种 → DownloadChain 调 `download` → 插件拦截：Telethon 点击回调按钮 → 等待文件消息 → 下载到 `/qbs/torrents/music/`
4. MP 目录监控/整理链自动接管整理

## 配置

- `bot_username`: TG 音乐 bot 用户名（默认 `music_v1bot`）
- `download_dir`: 音乐下载目录（默认 `/qbs/torrents/music/`）
- `api_id` / `api_hash` / `session_string`: Telegram 凭据（留空则从环境变量 `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` / `TELEGRAM_SESSION_STRING` 读取）
- `proxy_host` / `proxy_port`: SOCKS5 代理（默认 `192.168.1.68:7891`，即 NAS 上 mihomo 代理；`proxy_type` 可选 `socks5`/`http`）
- `search_timeout` / `download_timeout`: 超时秒数
- `button_index`: 默认按钮序号

## MP 侧必配：音乐目录监控

插件下载的文件落在 `download_dir`，需 MP 监控该目录自动整理。在 MP 后台「目录同步」添加配置：

- 名称：音乐
- 下载目录：`/qbs/torrents/music/`（与插件 `download_dir` 一致）
- 媒体库目录：`/qbs/links/music/`
- 媒体类型：音乐
- 监控方式：**monitor**（本地目录监控，不是 downloader！）
- 整理方式：link

配好后 MP 日志出现 `✓ 本地目录监控已启动: /qbs/torrents/music` 即生效。

## 安装

将本目录放入 MoviePilot 本地插件源（`PLUGIN_LOCAL_REPO_PATHS` 配置的路径）后，在插件市场安装。

## 开发

- 插件类：`plugins.v2/tgmusicsites/__init__.py`
- 包元数据：`package.v2.json`
- 依赖：`requirements.txt`（telethon + pysocks + python-socks）

### 开发测试环境（1.68 = 192.168.1.68）

本机有独立的 MoviePilot 开发环境（`/vol1/1000/config/share/moviepilot-dev/docker-compose.yml`），前端 3101 / API 3102，PostgreSQL 5434 / Redis 6380，与生产完全隔离。

- 本仓库已挂载到 dev 容器 `/config/local-plugins`（改代码即见）
- 安装：`GET /api/v1/plugin/install/TgMusicSites?repo_url=local%3A%2F%2FTgMusicSites%3Fpath%3D%2Fconfig%2Flocal-plugins%26version%3Dv2&force=true`
- 配置：`PUT /api/v1/plugin/TgMusicSites`（body 为配置 dict）
- 验证：`GET /api/v1/plugin/TgMusicSites/test`（连接测试）/ `/sites`（站点管理）
- pytest：见 `tests/`，在 dev 容器内 `MOVIEPILOT_BACKEND_PATH=/app python3 -m pytest tests/v2/tgmusicsites/ -v`
- **铁律：开发测试在 1.68，验证通过才准部署到 1.3 生产**

### 生产部署（1.3 = 192.168.1.3）

1. 1.68 验证通过（pytest 全过 + 端到端）
2. scp 项目到 `/vol1/1000/homecloud/moviepilot-v2/config/local-plugins/`
3. force=true 重装 + 写配置 + API 验证
4. 观察生产日志确认

## 注意事项

- 依赖 Telethon 会话凭据（MTProto），不是网页版
- 需要能连通 Telegram（本机通过 mihomo 代理，Telegram 分流组需选稳定节点，如新加坡/日本线路）
- 不修改任何 MP 生产源码
- 站点生命周期管理通过插件自己的 API `/api/v1/plugin/TgMusicSites/sites` 实现（增删查改），不进 MP 站点表
- Telethon `connect()` 成功返回 None 而非 True，勿按真值判断
- client 创建/搜索/断开必须在同一事件循环内（跨循环会报 `An asyncio.Future, a coroutine or an awaitable is required`）
