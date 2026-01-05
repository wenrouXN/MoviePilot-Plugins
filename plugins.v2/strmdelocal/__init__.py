from typing import List, Tuple, Dict, Any, Optional
from app.plugins import _PluginBase
from app.core.event import eventmanager, Event
from app.schemas.types import EventType, NotificationType
import shutil
from pathlib import Path
from app.log import logger
from app.helper.module import ModuleHelper
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.downloadhistory_oper import DownloadHistoryOper
import os
import traceback
import time
import threading
from queue import Queue, Empty
import re

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

MEDIA_EXTENSIONS = {'.mp4', '.mkv', '.ts', '.iso', '.rmvb', '.avi', '.mov', 
                    '.mpeg', '.mpg', '.wmv', '.3gp', '.asf', '.m4v', '.flv', 
                    '.m2ts', '.tp', '.f4v'}

# 常见的元数据/刮削文件后缀
META_EXTENSIONS = {'.nfo', '.jpg', '.png', '.xml', '.bif', '.json'}

class StrmFileHandler(FileSystemEventHandler):
    def __init__(self, queue: Queue):
        self._queue = queue
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.strm'):
            self._queue.put(Path(event.src_path))
    def on_moved(self, event):
        if not event.is_directory and event.dest_path.lower().endswith('.strm'):
            self._queue.put(Path(event.dest_path))

class StrmDeLocal(_PluginBase):
    plugin_id = "StrmDeLocal"
    plugin_name = "STRM本地媒体资源清理"
    plugin_desc = "探测STRM入库，自动核对并清理本地媒体资源"
    plugin_icon = ""
    plugin_version = "1.0"
    plugin_author = "wenrouXN"

    def __init__(self):
        super().__init__()
        self._transferhistory = TransferHistoryOper()
        self._downloadhistory = DownloadHistoryOper()
        self._enabled = False
        self._path_mappings = []
        self._observer = None
        self._queue = Queue()
        self._worker_thread = None
        self._stop_event = threading.Event()

    def _to_bool(self, value: Any) -> bool:
        if isinstance(value, bool): return value
        if isinstance(value, str): return value.lower() == 'true'
        return bool(value)

    def init_plugin(self, config: dict = None):
        logger.info("插件初始化中 (V1.0)...")
        if not config: config = self.get_config() or {}
        self._enabled = self._to_bool(config.get("enabled", False))
        mappings = config.get("path_mappings") or ""
        self._path_mappings = [x.strip() for x in mappings.split('\n') if x.strip()] if isinstance(mappings, str) else []
        self._notify_only = self._to_bool(config.get("notify_only", True))
        self._send_notify = self._to_bool(config.get("send_notify", True))
        self._notify_interval = int(config.get("notify_interval") or 10)
        self._clean_metadata = self._to_bool(config.get("clean_metadata", False))
        self._delete_torrent = self._to_bool(config.get("delete_torrent", False))
        self._remove_record = self._to_bool(config.get("remove_record", False))
        self._deep_search = self._to_bool(config.get("deep_search", False))
        self._keep_dirs = config.get("keep_dirs") or []
        if isinstance(self._keep_dirs, str):
            self._keep_dirs = [x.strip() for x in self._keep_dirs.replace('\n', '|').split('|') if x.strip()]
        self._exclude_keywords = config.get("exclude_keywords") or []
        if isinstance(self._exclude_keywords, str):
            self._exclude_keywords = [x.strip() for x in self._exclude_keywords.replace('\n', '|').split('|') if x.strip()]

        if self._enabled:
            mode = "仅通知" if self._notify_only else "执行清理"
            notify = "开启" if self._send_notify else "关闭"
            deep = "开启" if self._deep_search else "关闭"
            info = f"当前配置: 模式={mode} | 通知={notify} | 冷却={self._notify_interval}s | 映射={len(self._path_mappings)}条 | 深度查找={deep}"
            logger.info(info)
            self.stop_service()
            self.start_service()
        else:
            self.stop_service()
            logger.info("[StrmDeLocal] 插件未启用")

    def start_service(self):
        self._stop_event.clear()
        self._worker_thread = threading.Thread(target=self._process_queue_loop, name="StrmDeLocalWorker", daemon=True)
        self._worker_thread.start()
        self._observer = Observer()
        active_count = 0
        for mapping in self._path_mappings:
            if ":" not in mapping: continue
            strm_root = mapping.split(":", 1)[0].strip()
            if Path(strm_root).exists():
                try:
                    self._observer.schedule(StrmFileHandler(self._queue), path=strm_root, recursive=True)
                    active_count += 1
                    logger.info(f"成功挂载监控源: {strm_root}")
                except: pass
        if active_count > 0: self._observer.start()

    def stop_service(self):
        if self._observer:
            try: self._observer.stop(); self._observer.join(timeout=1)
            except: pass
            self._observer = None
        self._stop_event.set()
        if self._worker_thread:
            try: self._queue.put(None); self._worker_thread.join(timeout=1)
            except: pass
            self._worker_thread = None

    def get_state(self) -> bool: return self._enabled
    def get_api(self) -> List[Dict[str, Any]]: return []
    @staticmethod
    def get_command() -> List[Dict[str, Any]]: return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                    {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 
                        'text': '本插件通过探测 STRM 入库事件，自动核对并清理对应的本地媒体文件。默认处于【执行清理】模式，会物理删除文件。'}},
                ]}
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                    {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}},
                ]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                    {'component': 'VSwitch', 'props': {'model': 'notify_only', 'label': '仅通知模式'}},
                ]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                    {'component': 'VSwitch', 'props': {'model': 'send_notify', 'label': '发送通知'}},
                ]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                    {'component': 'VTextField', 'props': {'model': 'notify_interval', 'label': '通知冷却(秒)', 'placeholder': '10'}},
                ]},
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                     {'component': 'VDivider'},
                     {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'class': 'mt-4',
                        'text': '查找模式：默认仅通过转移记录精确匹配。开启深度查找后，会在精确匹配后继续使用文件名模糊匹配，确保清理更彻底。'}},
                ]}
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                    {'component': 'VSwitch', 'props': {'model': 'deep_search', 'label': '启用深度查找'}},
                ]},
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                     {'component': 'VDivider'},
                     {'component': 'VAlert', 'props': {'type': 'warning', 'variant': 'tonal', 'class': 'mt-4',
                        'text': '以下选项控制清理时的附加行为。若未开启【仅通知模式】，主媒体文件始终会被删除。'}},
                ]}
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                    {'component': 'VSwitch', 'props': {'model': 'clean_metadata', 'label': '清理刮削文件'}},
                ]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                    {'component': 'VSwitch', 'props': {'model': 'delete_torrent', 'label': '联动删除种子'}},
                ]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                    {'component': 'VSwitch', 'props': {'model': 'remove_record', 'label': '删除转移记录'}},
                ]},
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                     {'component': 'VDivider'},
                ]}
            ]},
            {'component': 'VTextarea', 'props': {
                'model': 'path_mappings', 'label': '路径映射 (strm路径:本地路径)', 'rows': 4, 'placeholder': '/strm/movie:/mnt/media/movie'
            }},
            {'component': 'VTextarea', 'props': {
                'model': 'exclude_keywords', 'label': '排除关键词 (用 | 分隔)', 'rows': 2, 'placeholder': 'iso|remux'
            }},
            {'component': 'VTextarea', 'props': {
                'model': 'keep_dirs', 'label': '不清理目录', 'rows': 2, 'placeholder': '/mnt/media/movie/keepers'
            }},
        ], {
            "enabled": False, "send_notify": True, "notify_interval": 10, "notify_only": True, "path_mappings": "",
            "clean_metadata": False, "delete_torrent": False, "remove_record": False, "exclude_keywords": "", "keep_dirs": "",
            "deep_search": False
        }

    def _send_batch_notification(self, stats: dict):
        if not self._send_notify: return
        if stats["scanned"] == 0: return
        title = "【STRM本地资源清理】任务完成"
        text = f"扫描: {stats['scanned']} | 匹配: {stats['matched']} | 清理: {stats['deleted']}"
        if stats["failed"] > 0:
            text += f" | 失败: {stats['failed']}"
        if stats["deleted_files"]:
            text += "\n详情:"
            for f in stats["deleted_files"][:8]:
                text += f"\n- {Path(f).name}"
            if len(stats["deleted_files"]) > 8: text += "\n..."
        self.post_message(mtype=NotificationType.SiteMessage, title=title, text=text)

    def get_page(self) -> List[dict]:
        historys = self.get_data('history') or []
        res = []
        for h in sorted(historys, key=lambda x: x.get('time'), reverse=True):
             res.append({'component': 'VCard', 'props': {'variant': 'outlined', 'class': 'mb-2'}, 'content': [
                 {'component': 'VCardTitle', 'text': f"{h.get('title')} [{h.get('action')}]"},
                 {'component': 'VCardText', 'text': h.get('target')}
             ]})
        return res

    def _process_queue_loop(self):
        stats = {"scanned": 0, "matched": 0, "deleted": 0, "failed": 0, "deleted_files": []}
        has_data = False
        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=self._notify_interval)
                if task is None: break
                has_data = True
                self._handle_single_file(task, stats)
                self._queue.task_done()
            except Empty:
                if has_data:
                    self._send_batch_notification(stats)
                    stats = {"scanned": 0, "matched": 0, "deleted": 0, "failed": 0, "deleted_files": []}
                    has_data = False
            except: 
                logger.error(f"队列处理异常: {traceback.format_exc()}")
                has_data = False

    def _find_by_transfer_history(self, strm_path: Path, local_base: Path) -> Tuple[bool, List[Path], Optional[str]]:
        path_str = str(strm_path).replace("\\\\", "/")
        tmdb_id = None
        tmdb_match = re.search(r'\{tmdb[=-]?(\d+)\}', path_str, re.I)
        if not tmdb_match:
            tmdb_match = re.search(r'tmdb[=-](\d+)', path_str, re.I)
        if not tmdb_match:
            tmdb_match = re.search(r'\[tmdbid[=-](\d+)\]', path_str, re.I)
        if tmdb_match:
            tmdb_id = int(tmdb_match.group(1))
        
        season_num, episode_num = None, None
        se_match = re.search(r'[sS](\d+)[eE](\d+)', strm_path.stem)
        if se_match:
            season_num = f"S{se_match.group(1).zfill(2)}"
            episode_num = f"E{se_match.group(2).zfill(2)}"
        
        if not tmdb_id:
            logger.info(f"未能从路径提取 TMDB ID")
            return False, [], None
        
        msg = f"TMDB:{tmdb_id}"
        if season_num: msg += f" {season_num}"
        if episode_num: msg += f"{episode_num}"
        logger.info(f"提取: {msg}")
        
        transfer_records = []
        try:
            if season_num and episode_num:
                transfer_records = self._transferhistory.get_by(tmdbid=tmdb_id, season=season_num, episode=episode_num) or []
            else:
                transfer_records = self._transferhistory.get_by(tmdbid=tmdb_id) or []
        except: return False, [], msg
        
        if not transfer_records:
            return False, [], msg
        
        matched_files = []
        local_base_str = str(local_base).replace("\\\\", "/")
        for record in transfer_records:
            dest_path = record.dest
            if dest_path and dest_path.replace("\\\\", "/").startswith(local_base_str):
                p = Path(dest_path)
                if not self._is_excluded(p): matched_files.append(p)
        
        return bool(matched_files), matched_files, msg

    def _recursive_check_and_cleanup(self, dir_path: Path, stats: dict = None):
        if not dir_path.exists() or not dir_path.is_dir():
            return
        
        has_valid_content = False
        try:
            for item in dir_path.iterdir():
                if item.is_dir():
                    has_valid_content = True
                    break
                elif item.is_file():
                    if item.suffix.lower() in MEDIA_EXTENSIONS:
                        has_valid_content = True
                        break
        except: return

        if has_valid_content:
            logger.debug(f"目录保留(含内容): {dir_path.name}")
            return

        if self._notify_only:
            logger.info(f"[仅通知] 可回收空目录(含杂项): {dir_path}")
            return

        try:
            shutil.rmtree(dir_path)
            logger.info(f"已回收目录: {dir_path}")
            if stats: stats["deleted"] += 1
            if dir_path.parent.exists():
                self._recursive_check_and_cleanup(dir_path.parent, stats)
        except Exception as e:
            logger.warning(f"目录删除失败: {e}")

    def _del_meta_for_file(self, media_path: Path):
        """清理刮削文件：支持模糊匹配和更多格式"""
        if not self._clean_metadata: return
        parent = media_path.parent
        if not parent.exists(): return
        
        stem = media_path.stem
        
        # 1. 严格后缀匹配
        for ext in META_EXTENSIONS:
            f = parent / f"{stem}{ext}"
            try: 
                if f.exists(): f.unlink(); logger.info(f"Del Meta: {f.name}")
            except: pass
            
        # 2. 前缀模糊匹配
        try:
            for f in parent.glob(f"{stem}*"):
                if f == media_path: continue
                if f.suffix.lower() not in META_EXTENSIONS: continue
                name = f.stem
                if name == stem: 
                    pass
                elif name.startswith(f"{stem} ") or name.startswith(f"{stem}.") or \
                     name.startswith(f"{stem}-") or name.startswith(f"{stem}_"):
                    try: f.unlink(); logger.info(f"Del Fuzzy Meta: {f.name}");
                    except: pass
        except: pass

    def _handle_single_file(self, strm_path: Path, stats: dict = None):
        path_str = str(strm_path).replace("\\\\", "/")
        if stats is not None: stats["scanned"] += 1
        
        hit_rule = self._check_exclusion(strm_path)
        if hit_rule: return
        
        local_base, source_root = None, None
        for mapping in self._path_mappings:
            if ":" not in mapping: continue
            s, l = mapping.split(":", 1)
            s_r = s.strip().replace("\\\\", "/")
            if path_str.startswith(s_r):
                source_root = s_r
                local_base = Path(l.strip())
                break
        
        if not local_base: return

        rel_path = path_str[len(source_root):].strip("/")
        parts = rel_path.split("/")
        processed_files = set()
        
        found_by_history, history_files, h_msg = self._find_by_transfer_history(strm_path, local_base)
        
        if found_by_history and history_files:
            logger.info(f"精确匹配: {len(history_files)} 文件 (含缺失)")
            if stats: stats["matched"] += len(history_files)
            
            for file_path in history_files:
                self._perform_cleanup(file_path, stats, processed_files)
                self._recursive_check_and_cleanup(file_path.parent, stats)

            self._save_history(h_msg, "清理完成" if not self._notify_only else "发现文件", 
                             str(len(history_files)) + " 个文件")
        
        if self._deep_search:
            self._do_deep_search(strm_path, local_base, parts, processed_files, stats)

    def _get_torrent_hash(self, file_path: Path, h_record=None) -> Optional[str]:
        """获取种子Hash: 优先记录，其次反查"""
        try:
            # 1. 优先使用记录中保存的hash
            if h_record and h_record.download_hash:
                return h_record.download_hash
            
            # 2. 尝试使用 src 路径反查 (针对硬链接场景)
            if h_record and h_record.src:
                h = self._downloadhistory.get_hash_by_fullpath(h_record.src)
                if h: return h

            # 3. 尝试使用当前文件路径反查 (针对直接下载场景)
            return self._downloadhistory.get_hash_by_fullpath(str(file_path))
        except:
            return None

    def _perform_cleanup(self, file_path: Path, stats: dict, processed_files: set):
        """执行全套清理动作: 刮削文件 -> 记录 -> 种子 -> 物理文件"""
        if str(file_path) in processed_files: return
        
        if not self._notify_only:
            # 1. 尝试获取转移记录 (用于获取hash/src)
            h_record = None
            try: h_record = self._transferhistory.get_by_dest(str(file_path))
            except: pass

            # 2. 清理刮削文件
            try: self._del_meta_for_file(file_path)
            except: pass

            # 3. 清理种子 (V4.6: 使用Hash触发事件)
            if self._delete_torrent:
                t_hash = self._get_torrent_hash(file_path, h_record)
                if t_hash:
                    try: eventmanager.send_event(EventType.DownloadFileDeleted, {"hash": t_hash})
                    except: pass
                else:
                    logger.debug(f"未找到种子HASH，跳过删种: {file_path}")

            # 4. 清理转移记录
            if self._remove_record and h_record:
                try: self._transferhistory.delete(h_record.id)
                except: pass

            # 5. 物理删除主文件
            if file_path.exists():
                try:
                    file_path.unlink()
                    logger.info(f"已删除: {file_path.name}")
                    if stats: 
                        stats["deleted"] += 1
                        stats["deleted_files"].append(str(file_path))
                except Exception as e:
                    logger.warning(f"删除失败 {file_path}: {e}")
                    if stats: stats["failed"] += 1
            else:
                logger.info(f"主文件已缺失，仅清理关联项: {file_path.name}")
        else:
            status = "拟删除" if file_path.exists() else "拟清理关联项"
            logger.info(f"[仅通知] {status}: {file_path}")

        processed_files.add(str(file_path))

    def _do_deep_search(self, strm_path: Path, local_base: Path, parts: List[str], processed_files: set, stats: dict):
        current = local_base
        for part in parts[:-1]:
            target = current / part
            if target.exists():
                current = target
            else:
                found = False
                try: 
                    sub_dirs = [x for x in current.iterdir() if x.is_dir()]
                except: return
                
                name_year = re.search(r'(.+?)[\\(\\[（](\\d{4})[\\)\\]）]', part)
                if name_year:
                    n, y = name_year.group(1).strip().lower(), name_year.group(2)
                    for d in sub_dirs:
                        if n in d.name.lower() and y in d.name:
                            current = d; found = True; break
                if not found and re.search(r'[sS]eason\\s*\\d+', part, re.I):
                    num = int(re.search(r'\\d+', part).group())
                    for d in sub_dirs:
                        m = re.search(r'[sS]eason\\s*(\\d+)', d.name, re.I)
                        if m and int(m.group(1)) == num:
                            current = d; found = True; break
                if not found: return 

        strm_stem = strm_path.stem
        tv = re.search(r'(.+)[sS](\\d+)[eE](\\d+)', strm_stem)
        if tv:
            se = f"S{tv.group(2).zfill(2)}E{tv.group(3).zfill(2)}"
            if current.exists():
                for f in current.iterdir():
                    if f.is_file() and f.suffix.lower() in MEDIA_EXTENSIONS and se.lower() in f.name.lower():
                        if str(f) not in processed_files and not self._is_excluded(f):
                            if stats: stats["matched"] += 1
                            self._perform_cleanup(f, stats, processed_files)
                self._recursive_check_and_cleanup(current, stats)
        else:
            if current != local_base and str(current) not in processed_files:
                if stats: stats["matched"] += 1
                self._do_cleanup_dir(current, strm_stem, stats)

    def _do_cleanup_dir(self, target_dir: Path, title: str, stats: dict = None):
        if self._is_excluded(target_dir): return
        if self._notify_only:
             self._save_history(title, "发现可清理", str(target_dir))
             return
        try:
            if self._remove_record: self._del_records(target_dir)
            if self._delete_torrent: self._del_torrents(target_dir)
            shutil.rmtree(target_dir)
            if stats: 
                stats["deleted"] += 1
                stats["deleted_files"].append(str(target_dir))
            self._save_history(title, "清理完成", str(target_dir))
        except: pass
    
    def _del_records(self, d: Path):
        for root, _, fs in os.walk(d):
            for f in fs:
                h = self._transferhistory.get_by_dest(os.path.join(root, f))
                if h: self._transferhistory.delete(h.id)

    def _del_torrents(self, d: Path):
        for root, _, fs in os.walk(d):
            for f in fs:
                # V4.6: 使用Hash清理 (目录级清理逻辑)
                file_path = Path(root) / f
                h_record = None
                try: h_record = self._transferhistory.get_by_dest(str(file_path))
                except: pass
                t_hash = self._get_torrent_hash(file_path, h_record)
                if t_hash:
                     eventmanager.send_event(EventType.DownloadFileDeleted, {"hash": t_hash})

    def _check_exclusion(self, p: Path) -> Optional[str]:
        s = str(p)
        for d in self._keep_dirs:
            if d and d in s: return d
        for k in self._exclude_keywords:
            if k and k in s: return k
        return None
    
    def _is_excluded(self, p: Path) -> bool:
        return bool(self._check_exclusion(p))

    def _save_history(self, title: str, action: str, target: str):
        history = self.get_data('history') or []
        history.append({"time": time.strftime("%Y-%m-%d %H:%M:%S"), "title": title, "action": action, "target": target})
        self.save_data('history', history[-100:])

    def state_change_callback(self, *args, **kwargs): pass
