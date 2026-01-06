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
import json

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

MEDIA_EXTENSIONS = {'.mp4', '.mkv', '.ts', '.iso', '.rmvb', '.avi', '.mov', 
                    '.mpeg', '.mpg', '.wmv', '.3gp', '.asf', '.m4v', '.flv', 
                    '.m2ts', '.tp', '.f4v'}

META_EXTENSIONS = {'.nfo', '.jpg', '.png', '.xml', '.bif', '.json'}

# 莫兰迪色系 (用于UI渲染)
MORANDI_RED = "#E6B0AA"    
MORANDI_BLUE = "#AED6F1"   
MORANDI_GREEN = "#A9DFBF"  
MORANDI_GREY = "#D7DBDD"   
MORANDI_PURPLE = "#D2B4DE" 

# 日志前缀
LOG_PREFIX = "[StrmDeLocal]"

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
    plugin_desc = "监控STRM目录变化，当检测到新STRM文件时，根据路径映射规则清理对应本地资源库中的相关媒体文件、种子及刮削数据,释放本地存储空间"
    plugin_icon = ""
    plugin_version = "1.1.6"
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

    def _log(self, msg: str, level: str = "info", title: str = None):
        """统一日志格式: 【标题】 内容"""
        prefix = f"【{title}】" if title else "【StrmDeLocal】"
        full_msg = f"{prefix} {msg}"
        if level == "info": logger.info(full_msg)
        elif level == "warning": logger.warning(full_msg)
        elif level == "error": logger.error(full_msg)
        elif level == "debug": logger.debug(full_msg)

    def _count_strm_files(self, path: Path) -> int:
        """统计目录下 .strm 文件数量"""
        count = 0
        try:
            for f in path.rglob("*.strm"):
                if f.is_file(): count += 1
        except: pass
        return count

    def init_plugin(self, config: dict = None):
        self._log("--------------------")
        self._log("插件初始化中 (V1.1.6)...")
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
            self._log(f"当前配置: 模式={mode} | 通知={notify} | 冷却={self._notify_interval}s | 深度查找={deep}")
            
            # 路径检查
            for mapping in self._path_mappings:
                 if ":" in mapping:
                     strm_path = mapping.split(":")[0].strip()
                     if not Path(strm_path).exists():
                         self._log(f"配置警告: 监控路径不存在 -> {strm_path}", "warning")
            
            self.stop_service()
            self.start_service()
        else:
            self.stop_service()
            self._log("插件未启用")

    def start_service(self):
        self._stop_event.clear()
        self._worker_thread = threading.Thread(target=self._process_queue_loop, name="StrmDeLocalWorker", daemon=True)
        self._worker_thread.start()
        self._observer = Observer()
        active_count = 0
        for mapping in self._path_mappings:
            if ":" not in mapping: continue
            strm_root = mapping.split(":", 1)[0].strip()
            strm_path = Path(strm_root)
            if strm_path.exists():
                try:
                    self._observer.schedule(StrmFileHandler(self._queue), path=strm_root, recursive=True)
                    active_count += 1
                    # V1.1.3: 统计现有 strm 文件数
                    strm_count = self._count_strm_files(strm_path)
                    self._log(f"成功挂载监控源: {strm_root} (已存在 {strm_count} 个 .strm 文件)")
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
                        'text': '监控STRM目录变化，当检测到新STRM文件时，根据路径映射规则清理对应本地资源库中的相关媒体文件、种子及刮削数据,释放本地存储空间。'}},
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
        
        # 新增通知日志
        self._log(f"批量处理完成,发送通知: {text.replace(chr(10), ' ')}")
        
        self.post_message(mtype=NotificationType.SiteMessage, title=title, text=text)

    def get_page(self) -> List[dict]:
        historys = self.get_data('history') or []
        history_cards = []
        
        for h in sorted(historys, key=lambda x: x.get('time'), reverse=True):
            action = h.get('action', '未知')
            title = h.get('title', '无标题')
            time_str = h.get('time', '')
            raw_files = h.get('files', [])
            
            tags = []
            file_details = []
            
            if raw_files:
                v, n, i, o = 0, 0, 0, 0
                for f in raw_files:
                    f_low = f.lower()
                    file_details.append(f)
                    if f_low.endswith(('.mp4', '.mkv', '.ts', '.iso')): v+=1
                    elif f_low.endswith(('.nfo', '.xml')): n+=1
                    elif f_low.endswith(('.jpg', '.png', '.bif')): i+=1
                    else: o+=1
                
                if v: tags.append({'text': f'视频 {v}', 'color': MORANDI_RED})
                if n: tags.append({'text': f'元数据 {n}', 'color': MORANDI_GREY})
                if i: tags.append({'text': f'图片 {i}', 'color': MORANDI_BLUE})
                if o: tags.append({'text': f'其他 {o}', 'color': MORANDI_PURPLE})
            elif h.get('target'):
                file_details.append(str(h.get('target')))
                tags.append({'text': '旧版记录', 'color': MORANDI_GREY})

            action_color = MORANDI_GREEN if "发现" in action else MORANDI_BLUE
            if "清理" in action: action_color = MORANDI_RED
            
            detail_lines = []
            for f in file_details[:10]:
                detail_lines.append({'component': 'div', 'class': 'text-caption text-grey-darken-1', 'text': f"• {Path(f).name}"})
            if len(file_details) > 10:
                detail_lines.append({'component': 'div', 'class': 'text-caption text-grey', 'text': f"... 等共 {len(file_details)} 个文件"})

            card_content = [
                {'component': 'div', 'class': 'd-flex align-center justify-space-between', 'content': [
                     {'component': 'div', 'class': 'd-flex align-center', 'content': [
                         {'component': 'VChip', 'props': {'size': 'small', 'color': action_color, 'variant': 'flat', 'class': 'mr-2'}, 'text': action},
                         {'component': 'span', 'class': 'text-body-2 font-weight-bold', 'text': title},
                     ]},
                     {'component': 'span', 'class': 'text-caption text-grey', 'text': time_str}
                ]},
                {'component': 'div', 'class': 'd-flex flex-wrap gap-1 mt-2 mb-2', 'content': [
                    {'component': 'VChip', 'props': {'size': 'x-small', 'color': t['color'], 'variant': 'tonal', 'class': 'mr-1'}, 'text': t['text']}
                    for t in tags
                ]},
                {'component': 'div', 'class': 'pa-2 rounded bg-grey-lighten-4', 'content': detail_lines}
            ]
            
            history_cards.append({
                'component': 'VCard',
                'props': {'class': 'mb-3', 'variant': 'outlined'},
                'content': [{'component': 'VCardText', 'content': card_content}]
            })

        return history_cards if history_cards else [
            {'component': 'div', 'class': 'text-center text-grey mt-4', 'text': '暂无清理记录'}
        ]

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
                self._log(f"队列处理异常: {traceback.format_exc()}", "error")
                has_data = False

    def _find_by_transfer_history(self, strm_path: Path, local_base: Path, title: str = None) -> Tuple[bool, List[Path], Optional[str]]:
        path_str = str(strm_path).replace("\\\\", "/")
        
        # 提取 TMDB ID
        tmdb_id = None
        tmdb_match = re.search(r'\{(?:tmdb|tmdbid)[=-]?(\d+)\}', path_str, re.I)
        if not tmdb_match:
            tmdb_match = re.search(r'tmdb[=-](\d+)', path_str, re.I)
        if not tmdb_match:
            tmdb_match = re.search(r'\[tmdbid[=-](\d+)\]', path_str, re.I)
        
        if tmdb_match:
            tmdb_id = int(tmdb_match.group(1))
        
        # 提取季集信息 (用于查询，但不在日志中显示)
        season_num, episode_num = None, None
        se_match = re.search(r'[sS](\d+)[eE](\d+)', strm_path.stem)
        if se_match:
            season_num = f"S{se_match.group(1).zfill(2)}"
            episode_num = f"E{se_match.group(2).zfill(2)}"
        
        if not tmdb_id:
            self._log(f"-> 提取失败: 未能识别 TMDB ID", title=title)
            return False, [], None
        
        # 日志只显示 TMDB ID，不显示季集
        self._log(f"-> 提取成功: TMDB:{tmdb_id}", title=title)
        
        # 用于返回的消息
        msg = f"TMDB:{tmdb_id}"
        
        # 查询转移记录
        transfer_records = []
        try:
            if season_num and episode_num:
                transfer_records = self._transferhistory.get_by(tmdbid=tmdb_id, season=season_num, episode=episode_num) or []
            else:
                transfer_records = self._transferhistory.get_by(tmdbid=tmdb_id) or []
        except Exception as e:
            self._log(f"-> 查询转移记录失败: {e}", "warning", title=title)
            return False, [], msg
        
        if not transfer_records:
            self._log(f"-> 查询结果: 未找到匹配的转移记录", title=title)
            return False, [], msg
        
        self._log(f"-> 查询结果: 找到 {len(transfer_records)} 条转移记录", title=title)
        
        matched_files = []
        local_base_str = str(local_base).replace("\\\\", "/")
        for record in transfer_records:
            dest_path = record.dest
            if dest_path and dest_path.replace("\\\\", "/").startswith(local_base_str):
                p = Path(dest_path)
                if not self._is_excluded(p): matched_files.append(p)
        
        if matched_files:
            self._log(f"-> 本地文件匹配: {len(matched_files)} 个文件在配置的本地路径下", title=title)
        else:
            self._log(f"-> 本地文件匹配: 无 (转移记录目标路径不在配置的本地路径范围内)", title=title)
        
        return bool(matched_files), matched_files, msg

    def _recursive_check_and_cleanup(self, dir_path: Path, stats: dict = None, title: str = None):
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
            return

        if self._notify_only:
            self._log(f"-> 发现可回收目录: {dir_path}", title=title)
            return

        try:
            shutil.rmtree(dir_path)
            self._log(f"-> 已回收空目录: {dir_path}", title=title)
            if stats: stats["deleted"] += 1
            if dir_path.parent.exists():
                self._recursive_check_and_cleanup(dir_path.parent, stats, title=title)
        except Exception as e:
            self._log(f"-> 目录回收失败: {e}", "warning", title=title)

    def _del_meta_for_file(self, media_path: Path):
        if not self._clean_metadata: return
        parent = media_path.parent
        if not parent.exists(): return
        
        stem = media_path.stem
        deleted_count = 0
        
        for ext in META_EXTENSIONS:
            f = parent / f"{stem}{ext}"
            try: 
                if f.exists(): f.unlink(); deleted_count += 1
            except: pass
            
        try:
            for f in parent.glob(f"{stem}*"):
                if f == media_path: continue
                if f.suffix.lower() not in META_EXTENSIONS: continue
                name = f.stem
                if name == stem: 
                    pass
                elif name.startswith(f"{stem} ") or name.startswith(f"{stem}.") or \
                     name.startswith(f"{stem}-") or name.startswith(f"{stem}_"):
                    try: f.unlink(); deleted_count += 1
                    except: pass
        except: pass
        
        if deleted_count > 0:
            self._log(f"-> 已清理刮削文件: {deleted_count} 个", title=self._current_title if hasattr(self, '_current_title') else None)

    def _handle_single_file(self, strm_path: Path, stats: dict = None):
        # 提取标题用于日志
        title = strm_path.stem
        
        # 直接输出检测信息 (不再需要分隔线，标题已提供区分)
        self._log(f"监测到 strm 入库: {strm_path}", title=title)
        
        path_str = str(strm_path).replace("\\\\", "/")
        if stats is not None: stats["scanned"] += 1
        
        # 检查排除规则
        hit_rule = self._check_exclusion(strm_path)
        if hit_rule:
            self._log(f"命中排除规则: [{hit_rule}]，已跳过", title=title)
            return
        
        # 查找路径映射
        local_base, source_root = None, None
        for mapping in self._path_mappings:
            if ":" not in mapping: continue
            s, l = mapping.split(":", 1)
            s_r = s.strip().replace("\\\\", "/")
            if path_str.startswith(s_r):
                source_root = s_r
                local_base = Path(l.strip())
                break
        
        if not local_base: 
            self._log(f"-> 路径映射失败: 未找到匹配的映射规则，已跳过", "warning", title=title)
            return
        
        self._log(f"-> 路径映射: {source_root} => {local_base}", title=title)

        rel_path = path_str[len(source_root):].strip("/")
        parts = rel_path.split("/")
        processed_files = set()
        
        # 通过转移记录查找
        found_by_history, history_files, h_msg = self._find_by_transfer_history(strm_path, local_base, title=title)
        
        if found_by_history and history_files:
            self._log(f"-> 精确匹配成功: {len(history_files)} 个本地文件", title=title)
            if stats: stats["matched"] += len(history_files)
            
            for file_path in history_files:
                self._perform_cleanup(file_path, stats, processed_files, title=title)
                self._recursive_check_and_cleanup(file_path.parent, stats, title=title)
            
            # 结果汇总
            action = "清理完成" if not self._notify_only else "发现待清理"
            self._log(f"{action}，处理 {len(history_files)} 个文件", title=title)
            
            # 保存历史
            files_record = list(set(processed_files))
            if self._notify_only and history_files:
                files_record = [str(f) for f in history_files]
            self._save_history(h_msg or strm_path.stem, action, 
                             f"涉及 {len(files_record)} 个文件", files_list=files_record)
        else:
            # 尝试深度查找
            if self._deep_search:
                self._log(f"-> 精确匹配失败，启用深度查找...", title=title)
                self._do_deep_search(strm_path, local_base, parts, processed_files, stats, title=title)
                
                if processed_files:
                    action = "清理完成" if not self._notify_only else "发现待清理"
                    self._log(f"{action}，深度查找处理 {len(processed_files)} 个文件", title=title)
                    self._save_history(h_msg or strm_path.stem, action, 
                                     f"涉及 {len(processed_files)} 个文件 (深度查找)", files_list=list(processed_files))
                else:
                    self._log(f"未找到对应本地媒体资源，已跳过", title=title)
            else:
                self._log(f"未找到对应本地媒体资源，已跳过", title=title)

    def _get_torrent_hash(self, file_path: Path, h_record=None) -> Optional[str]:
        try:
            if h_record and h_record.download_hash:
                return h_record.download_hash
            
            if h_record and h_record.src:
                h = self._downloadhistory.get_hash_by_fullpath(h_record.src)
                if h: return h

            return self._downloadhistory.get_hash_by_fullpath(str(file_path))
        except:
            return None

    def _perform_cleanup(self, file_path: Path, stats: dict, processed_files: set, title: str = None):
        if str(file_path) in processed_files: return
        
        file_exists = file_path.exists()
        
        if not self._notify_only:
            h_record = None
            try: h_record = self._transferhistory.get_by_dest(str(file_path))
            except: pass

            # 清理刮削文件
            if self._clean_metadata:
                self._current_title = title  # 传递title给_del_meta_for_file
                try: self._del_meta_for_file(file_path)
                except: pass

            # 清理种子
            if self._delete_torrent:
                t_hash = self._get_torrent_hash(file_path, h_record)
                if t_hash:
                    try: 
                        eventmanager.send_event(EventType.DownloadFileDeleted, {"hash": t_hash})
                        self._log(f"-> 已触发删种: {t_hash[:8]}...", title=title)
                    except: pass

            # 清理转移记录
            if self._remove_record and h_record:
                try: 
                    self._transferhistory.delete(h_record.id)
                    self._log(f"-> 已删除转移记录: ID={h_record.id}", title=title)
                except: pass

            # 物理删除主文件
            if file_exists:
                try:
                    file_path.unlink()
                    self._log(f"-> 已删除文件: {file_path}", title=title)
                    if stats: 
                        stats["deleted"] += 1
                        stats["deleted_files"].append(str(file_path))
                except Exception as e:
                    self._log(f"-> 文件删除失败: {file_path} ({e})", "warning", title=title)
                    if stats: stats["failed"] += 1
            else:
                self._log(f"-> 文件已缺失，仅清理关联项: {file_path}", title=title)
        else:
            status = "拟删除" if file_exists else "拟清理关联项"
            self._log(f"-> [仅通知] {status}: {file_path}", title=title)

        processed_files.add(str(file_path))

    def _do_deep_search(self, strm_path: Path, local_base: Path, parts: List[str], processed_files: set, stats: dict, title: str = None):
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
                
                # 不再输出候选列表以提高性能
                self._log(f"-> 本地目录不匹配: [{part}]，尝试智能重定向...", title=title)
                
                name_year = re.search(r'(.+?)[\(\[（](\d{4})[\)\]）]', part)
                if name_year:
                    n, y = name_year.group(1).strip().lower(), name_year.group(2)
                    for d in sub_dirs:
                        if n in d.name.lower() and y in d.name:
                            current = d; found = True
                            self._log(f"-> 智能重定向成功: {d.name}", title=title)
                            break
                if not found and re.search(r'[sS]eason\s*\d+', part, re.I):
                    num = int(re.search(r'\d+', part).group())
                    for d in sub_dirs:
                        m = re.search(r'[sS]eason\s*(\d+)', d.name, re.I)
                        if m and int(m.group(1)) == num:
                            current = d; found = True
                            self._log(f"-> 季目录匹配成功: {d.name}", title=title)
                            break
                if not found:
                    self._log(f"-> 本地媒体定位失败: 未找到目录 [{part}]", title=title)
                    return 

        strm_stem = strm_path.stem
        tv = re.search(r'(.+)[sS](\d+)[eE](\d+)', strm_stem)
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
            self._log(f"-> 已删除目录: {target_dir.name}")
            if stats: 
                stats["deleted"] += 1
                stats["deleted_files"].append(str(target_dir))
            
            self._save_history(title, "清理完成", f"目录: {target_dir.name}", files_list=[str(target_dir)])
        except: pass
    
    def _del_records(self, d: Path):
        for root, _, fs in os.walk(d):
            for f in fs:
                h = self._transferhistory.get_by_dest(os.path.join(root, f))
                if h: self._transferhistory.delete(h.id)

    def _del_torrents(self, d: Path):
        for root, _, fs in os.walk(d):
            for f in fs:
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

    def _save_history(self, title: str, action: str, target: str, files_list: List[str] = None):
        history = self.get_data('history') or []
        new_item = {
            "time": time.strftime("%Y-%m-%d %H:%M:%S"), 
            "title": title, 
            "action": action, 
            "target": target
        }
        if files_list:
            new_item["files"] = files_list
            
        history.append(new_item)
        self.save_data('history', history[-100:])

    def state_change_callback(self, *args, **kwargs): pass
