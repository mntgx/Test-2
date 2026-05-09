#  @MrMNTG @MusammilN
#please give credits https://github.com/MN-BOTS/ShobanaFilterBot
import logging
from struct import pack
import re
import base64
import asyncio
import time
import os
import sqlite3
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError, OperationFailure
from motor.motor_asyncio import AsyncIOMotorClient
import hashlib
from collections import OrderedDict, defaultdict
from sqlalchemy import text

from info import (
    DATABASE_URI, DATABASE_NAME, COLLECTION_NAME, USE_CAPTION_FILTER,
    DATABASE_URI2, DATABASE_URI3, DATABASE_URI4, DATABASE_URI5,
    DATABASE_NAME2, DATABASE_NAME3, DATABASE_NAME4, DATABASE_NAME5, INDEX_MODE, MEDIA_CACHE_MAX,
    DISK_MEDIA_CACHE, DISK_MEDIA_CACHE_PATH, ADVANCED_DUPLICATE_SKIP,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SEARCH_CACHE_TTL = 300
SEARCH_CACHE_MAX = 2048
_SEARCH_CACHE = OrderedDict()

BAD_RELEASE_TAGS = (
    'predvdrip', 'camrip', 'hdts', 'prehd', 'dvdscr', 'hq real',
)

DUP_LANG_ALIASES = {
    "multi": "multi", "multi audio": "multi", "multi language": "multi", "multilingual": "multi",
    "dual": "multi", "dual audio": "multi", "tri audio": "multi", "triple audio": "multi",
    "malayalam": "mal", "mal": "mal", "ml": "mal", "mallu": "mal",
    "tamil": "tam", "tam": "tam", "ta": "tam",
    "hindi": "hin", "hin": "hin", "hi": "hin", "bollywood": "hin",
    "english": "eng", "eng": "eng", "en": "eng",
    "telugu": "tel", "tel": "tel", "te": "tel",
    "kannada": "kan", "kan": "kan", "kn": "kan",
    "bengali": "ben", "bangla": "ben", "ben": "ben", "bn": "ben",
    "marathi": "mar", "mar": "mar", "mr": "mar",
    "punjabi": "pan", "pun": "pan", "pan": "pan", "pa": "pan",
    "gujarati": "guj", "guj": "guj", "gu": "guj",
    "odia": "ori", "oriya": "ori", "ori": "ori",
    "assamese": "asm", "assam": "asm", "asm": "asm",
    "urdu": "urd", "urd": "urd", "ur": "urd",
    "bhojpuri": "bho", "bho": "bho",
    "nepali": "nep", "nep": "nep", "ne": "nep",
    "sinhala": "sin", "sinhalese": "sin", "sin": "sin", "si": "sin",
    "arabic": "ara", "ara": "ara", "ar": "ara",
    "korean": "kor", "kor": "kor", "ko": "kor",
    "japanese": "jpn", "jpn": "jpn", "ja": "jpn",
    "chinese": "chi", "mandarin": "chi", "chi": "chi", "zh": "chi",
    "french": "fre", "fr": "fre", "spanish": "spa", "es": "spa",
    "german": "ger", "de": "ger", "russian": "rus", "ru": "rus",
    "thai": "tha", "th": "tha", "indonesian": "ind", "indo": "ind",
}
DUP_LANG_RE = re.compile(r"\b(" + "|".join(map(re.escape, sorted(DUP_LANG_ALIASES, key=len, reverse=True))) + r")\b", re.I)
DUP_SERIES_TOKEN_RE = re.compile(
    r"(?:"
    r"\bS(?P<s1>\d{1,2})\s*(?:E|EP|EPISODE)\s*(?P<e1>\d{1,3})\b|"
    r"\bS(?P<s5>\d{1,2})\s*[.\-_ ]+\s*(?P<e5>\d{1,3})\b|"
    r"\b(?P<s3>\d{1,2})\s*x\s*(?P<e3>\d{1,3})\b|"
    r"\bSeason\s*(?P<s2>\d{1,2}).*?\b(?:Episode|Ep|E)\s*(?P<e2>\d{1,3})\b|"
    r"\bSeason\s*(?P<s4>\d{1,2})\b|"
    r"\b(?:Episode|Ep)\s*(?P<e4>\d{1,3})\b"
    r")",
    re.I,
)
DUP_DROP_WORDS_RE = re.compile(
    r"\b(\d{3,4}p|2160p|1080p|720p|480p|4k|uhd|hdr|hdr10|dv|dolby|atmos|x264|x265|hevc|avc|h\.?264|h\.?265|aac|ac3|eac3|ddp?\d?(?:\.\d)?|5\.1|7\.1|web[- ]?dl|web[- ]?rip|webrip|hdrip|bluray|blu[- ]?ray|brrip|dvdrip|hdtv|proper|repack|remux|extended|unrated|uncut|theatrical|imax|esub|subs?|dubbed|org|original|cleaned|hq|hd|sd)\b",
    re.I,
)


def clean_duplicate_name(name: str) -> str:
    raw = str(name or "").lower()
    raw = re.sub(r"\.[a-z0-9]{2,4}$", " ", raw)
    raw = re.sub(r"@\w+", " ", raw)
    raw = re.sub(r"https?://\S+|www\.\S+", " ", raw)
    raw = re.sub(
        r"^[\[\(]([^\]\)]{1,40})[\]\)]\s*",
        lambda m: f" {m.group(1)} " if DUP_SERIES_TOKEN_RE.search(m.group(1)) else " ",
        raw,
    )
    raw = DUP_DROP_WORDS_RE.sub(" ", raw)
    raw = re.sub(r"[._+\-]+", " ", raw)
    raw = re.sub(r"[^a-z0-9\s]", " ", raw)
    return re.sub(r"\s+", " ", raw).strip()


def duplicate_language_key(clean_name: str) -> str:
    found = []
    for match in DUP_LANG_RE.finditer(clean_name):
        code = DUP_LANG_ALIASES.get(match.group(1).lower())
        if code == "multi":
            return "multi"
        if code and code not in found:
            found.append(code)
    return "+".join(found) if found else "unknown"


def duplicate_series_parts(clean_name: str):
    match = DUP_SERIES_TOKEN_RE.search(clean_name)
    if not match:
        return None
    gd = match.groupdict()
    season = int(gd.get('s1') or gd.get('s2') or gd.get('s3') or gd.get('s4') or gd.get('s5') or 0)
    episode = int(gd.get('e1') or gd.get('e2') or gd.get('e3') or gd.get('e4') or gd.get('e5') or 0)
    before = clean_name[:match.start()].strip()
    after = clean_name[match.end():].strip()
    title = before if len(before) >= 3 else after
    title = DUP_LANG_RE.sub(" ", title)
    title = re.sub(r"\b(19|20)\d{2}\b", " ", title)
    title = re.sub(r"\b(seasons?|episodes?|complete|all)\b", " ", title, flags=re.I)
    title = re.sub(r"\s+", " ", title).strip()
    return title, season, episode


def duplicate_movie_title(clean_name: str):
    title = DUP_SERIES_TOKEN_RE.sub(" ", clean_name)
    title = DUP_LANG_RE.sub(" ", title)
    return re.sub(r"\s+", " ", title).strip()


def duplicate_key_for_doc(doc):
    clean_name = clean_duplicate_name(doc.get('file_name'))
    lang = duplicate_language_key(clean_name)
    series = duplicate_series_parts(clean_name)
    if series:
        title, season, episode = series
        if not title:
            title = duplicate_movie_title(clean_name)
        return ("series", title, lang, season, episode)
    return ("movie", duplicate_movie_title(clean_name), lang)


def duplicate_sizes_match(left: int, right: int) -> bool:
    if not left or not right:
        return True
    return abs(int(left) - int(right)) <= max(10 * 1024 * 1024, int(min(int(left), int(right)) * 0.02))
SERIES_RE = re.compile(
    r'(?:\bS\d{1,2}\s*(?:E|EP|EPISODE)\s*\d{1,3}\b|\bSeason\s*\d{1,2}\b|\bEpisode\s*\d{1,3}\b|\bE(?:P)?\s*\d{1,3}\b)',
    re.IGNORECASE,
)
VALID_INDEX_MODES = {'both', 'series', 'movies'}
_ACTIVE_INDEX_MODE = INDEX_MODE if INDEX_MODE in VALID_INDEX_MODES else 'both'
_MEDIA_CACHE = OrderedDict()
_MEDIA_CACHE_READY = False
_MEDIA_CACHE_COMPLETE = False
_MEDIA_CACHE_LOCK = asyncio.Lock()
_DISK_CACHE_READY = False
_DISK_CACHE_COMPLETE = False


def normalize_index_mode(value):
    mode = str(value or 'both').strip().lower()
    return mode if mode in VALID_INDEX_MODES else 'both'


def set_index_mode(value):
    global _ACTIVE_INDEX_MODE
    _ACTIVE_INDEX_MODE = normalize_index_mode(value)
    return _ACTIVE_INDEX_MODE


def get_index_mode():
    return _ACTIVE_INDEX_MODE


def is_bad_release_name(file_name):
    normalized = re.sub(r'[_\-.+]+', ' ', str(file_name or '')).lower()
    return any(tag in normalized for tag in BAD_RELEASE_TAGS)


def is_series_name(file_name):
    return bool(DUP_SERIES_TOKEN_RE.search(clean_duplicate_name(file_name)))


def media_allowed_for_index(file_name, mode=None):
    if is_bad_release_name(file_name):
        return False, 'bad_release'
    mode = normalize_index_mode(mode or _ACTIVE_INDEX_MODE)
    series = is_series_name(file_name)
    if mode == 'series' and not series:
        return False, 'movie_in_series_mode'
    if mode == 'movies' and series:
        return False, 'series_in_movies_mode'
    return True, None


def _cache_doc(doc):
    global _MEDIA_CACHE_COMPLETE
    if not doc or MEDIA_CACHE_MAX <= 0:
        return
    d = _as_media_doc(doc)
    fid = d.get('_id') or d.get('file_id')
    if fid:
        _MEDIA_CACHE[fid] = d
        _MEDIA_CACHE.move_to_end(fid)
        while len(_MEDIA_CACHE) > MEDIA_CACHE_MAX:
            _MEDIA_CACHE.popitem(last=False)
            _MEDIA_CACHE_COMPLETE = False


def _uncache_doc(file_id):
    _MEDIA_CACHE.pop(file_id, None)




def _disk_cache_enabled():
    return bool(DISK_MEDIA_CACHE and DISK_MEDIA_CACHE_PATH)


def _disk_cache_path():
    return str(DISK_MEDIA_CACHE_PATH)


def _disk_connect():
    path = _disk_cache_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _disk_init_sync(reset=False):
    with _disk_connect() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS media ("
            "file_id TEXT PRIMARY KEY, file_ref TEXT, file_name TEXT, "
            "file_size INTEGER, file_type TEXT, mime_type TEXT, caption TEXT, created_at REAL)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_disk_media_created_at ON media(created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_disk_media_file_type ON media(file_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_disk_media_file_name ON media(file_name)")
        if reset:
            conn.execute("DELETE FROM media")


def _disk_doc_tuple(doc):
    d = _as_media_doc(doc)
    return (
        d.get('_id') or d.get('file_id'),
        d.get('file_ref'),
        d.get('file_name'),
        d.get('file_size'),
        d.get('file_type'),
        d.get('mime_type'),
        d.get('caption'),
        d.get('created_at') or 0,
    )


def _disk_upsert_many_sync(docs):
    rows = [row for row in (_disk_doc_tuple(doc) for doc in docs) if row[0]]
    if not rows:
        return 0
    with _disk_connect() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO media(file_id,file_ref,file_name,file_size,file_type,mime_type,caption,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )
    return len(rows)


def _disk_delete_many_sync(file_ids):
    ids = [fid for fid in file_ids if fid]
    if not ids:
        return
    with _disk_connect() as conn:
        conn.executemany("DELETE FROM media WHERE file_id=?", [(fid,) for fid in ids])


def _disk_clear_sync():
    with _disk_connect() as conn:
        conn.execute("DELETE FROM media")


def _disk_row_to_doc(row):
    return SQLMediaDoc(
        dict(
            file_id=row['file_id'],
            _id=row['file_id'],
            file_ref=row['file_ref'],
            file_name=row['file_name'],
            file_size=row['file_size'],
            file_type=row['file_type'],
            mime_type=row['mime_type'],
            caption=row['caption'],
            created_at=row['created_at'],
        )
    )


def _disk_count_all_sync():
    with _disk_connect() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM media").fetchone()[0] or 0)


def _disk_search_sync(query, file_type=None, max_results=10, offset=0, fast=False):
    terms = [t for t in str(query or '').split() if t]
    where = []
    params = []
    if file_type:
        where.append("file_type = ?")
        params.append(file_type)
    for term in terms:
        where.append("(file_name LIKE ? COLLATE NOCASE" + (" OR COALESCE(caption, '') LIKE ? COLLATE NOCASE" if USE_CAPTION_FILTER else "") + ")")
        like = f"%{term}%"
        params.append(like)
        if USE_CAPTION_FILTER:
            params.append(like)
    where_clause = " AND ".join(where) if where else "1=1"
    limit = max_results + 1 if fast else max_results
    with _disk_connect() as conn:
        total_results = None if fast else int(conn.execute(f"SELECT COUNT(*) FROM media WHERE {where_clause}", params).fetchone()[0] or 0)
        rows = conn.execute(
            f"SELECT file_id, file_ref, file_name, file_size, file_type, mime_type, caption, created_at "
            f"FROM media WHERE {where_clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
    files = [_disk_row_to_doc(row) for row in rows]
    has_more = fast and len(files) > max_results
    if has_more:
        files = files[:max_results]
    next_offset = offset + max_results
    if fast:
        total_results = offset + len(files) + (1 if has_more else 0)
        if not has_more:
            next_offset = ''
    elif next_offset >= total_results:
        next_offset = ''
    return files, next_offset, total_results


def _finish_cache_page(matched, max_results, offset, fast):
    page = matched[offset: offset + max_results + (1 if fast else 0)]
    has_more = fast and len(page) > max_results
    files = [_as_media_doc(d) for d in page[:max_results]]
    next_offset = offset + max_results
    if fast:
        total_results = offset + len(files) + (1 if has_more else 0)
        if not has_more:
            next_offset = ''
    else:
        total_results = len(matched)
        if next_offset >= total_results:
            next_offset = ''
    return files, next_offset, total_results


def _search_media_cache(search_filter, max_results, offset, fast):
    matched = [d for d in _MEDIA_CACHE.values() if _match_filter(d, search_filter)]
    matched.sort(key=lambda d: d.get('created_at', 0), reverse=True)
    return _finish_cache_page(matched, max_results, offset, fast)

def _cache_get(key):
    cached = _SEARCH_CACHE.get(key)
    if not cached:
        return None
    created_at, value = cached
    if time.monotonic() - created_at > SEARCH_CACHE_TTL:
        _SEARCH_CACHE.pop(key, None)
        return None
    _SEARCH_CACHE.move_to_end(key)
    return value


def _cache_set(key, value):
    _SEARCH_CACHE[key] = (time.monotonic(), value)
    _SEARCH_CACHE.move_to_end(key)
    while len(_SEARCH_CACHE) > SEARCH_CACHE_MAX:
        _SEARCH_CACHE.popitem(last=False)


def _finish_search(files, next_offset, total_results, started_at, return_time):
    elapsed = round(time.perf_counter() - started_at, 3)
    if return_time:
        return files, next_offset, total_results, elapsed
    return files, next_offset, total_results

USE_MONGO = bool(DATABASE_URI)

if not USE_MONGO:
    from database.sql_store import store


class SQLMediaDoc(dict):
    def __getattr__(self, item):
        if item == 'file_id':
            return self.get('file_id') or self.get('_id')
        if item == '_id':
            return self.get('_id') or self.get('file_id')
        return self.get(item)


class SQLDeleteResult:
    def __init__(self, deleted_count=0):
        self.deleted_count = deleted_count


class SQLCursor:
    def __init__(self, docs, projection=None):
        self.docs = docs
        self._skip = 0
        self._limit = None
        self.projection = projection

    def sort(self, field, direction):
        reverse = direction == -1
        key = 'created_at' if field == '$natural' else field
        self.docs.sort(key=lambda d: d.get(key), reverse=reverse)
        return self

    def skip(self, value):
        self._skip = value
        return self

    def limit(self, value):
        self._limit = value
        return self

    async def to_list(self, length=None):
        docs = self.docs[self._skip:]
        if self._limit is not None:
            docs = docs[: self._limit]
        if length is not None:
            docs = docs[:length]
        if self.projection is not None:
            keys = [k for k, v in self.projection.items() if v]
            projected = []
            for d in docs:
                item = SQLMediaDoc()
                for k in keys:
                    if k == '_id':
                        item['_id'] = d.get('file_id')
                    elif k in d:
                        item[k] = d[k]
                projected.append(item)
            return projected
        return [_as_media_doc(d) for d in docs]


def _as_media_doc(doc):
    if doc is None:
        return SQLMediaDoc()
    d = SQLMediaDoc(doc)
    if d.get('file_id') is None and d.get('_id') is not None:
        d['file_id'] = d.get('_id')
    if d.get('_id') is None and d.get('file_id') is not None:
        d['_id'] = d.get('file_id')
    return d


def _match_filter(doc, query):
    if not query:
        return True
    for key, val in query.items():
        if key == '$or':
            if not any(_match_filter(doc, cond) for cond in val):
                return False
            continue
        if key == '_id':
            if isinstance(val, dict) and '$in' in val:
                if (doc.get('file_id') or doc.get('_id')) not in val['$in']:
                    return False
            elif (doc.get('file_id') or doc.get('_id')) != val:
                return False
            continue

        target = doc.get(key)
        if isinstance(val, re.Pattern):
            if not val.search(str(target or '')):
                return False
        else:
            if target != val:
                return False
    return True


class SQLMediaCollection:
    async def _all_docs(self):
        with store.begin() as conn:
            rows = conn.execute(text("SELECT file_id, file_ref, file_name, file_size, file_type, mime_type, caption, created_at FROM media")).fetchall()
        docs = []
        for r in rows:
            docs.append(
                dict(
                    file_id=r[0],
                    _id=r[0],
                    file_ref=r[1],
                    file_name=r[2],
                    file_size=r[3],
                    file_type=r[4],
                    mime_type=r[5],
                    caption=r[6],
                    created_at=r[7],
                )
            )
        return docs

    async def find(self, query=None, projection=None):
        docs = [d for d in await self._all_docs() if _match_filter(d, query or {})]
        return SQLCursor(docs, projection=projection)

    async def delete_many(self, query):
        docs = [d for d in await self._all_docs() if _match_filter(d, query)]
        ids = [d['file_id'] for d in docs]
        if not ids:
            return SQLDeleteResult(0)
        with store.begin() as conn:
            for fid in ids:
                conn.execute(text("DELETE FROM media WHERE file_id=:fid"), {"fid": fid})
        for fid in ids:
            _uncache_doc(fid)
        if _disk_cache_enabled() and _DISK_CACHE_READY:
            await asyncio.to_thread(_disk_delete_many_sync, ids)
        _SEARCH_CACHE.clear()
        return SQLDeleteResult(len(ids))

    async def delete_one(self, query):
        docs = [d for d in await self._all_docs() if _match_filter(d, query)]
        if not docs:
            return SQLDeleteResult(0)
        fid = docs[0]['file_id']
        with store.begin() as conn:
            conn.execute(text("DELETE FROM media WHERE file_id=:fid"), {"fid": fid})
        _uncache_doc(fid)
        if _disk_cache_enabled() and _DISK_CACHE_READY:
            await asyncio.to_thread(_disk_delete_many_sync, [fid])
        _SEARCH_CACHE.clear()
        return SQLDeleteResult(1)

    async def drop(self):
        global _MEDIA_CACHE_COMPLETE, _DISK_CACHE_COMPLETE
        with store.begin() as conn:
            conn.execute(text("DELETE FROM media"))
        _MEDIA_CACHE.clear()
        _MEDIA_CACHE_COMPLETE = True
        if _disk_cache_enabled() and _DISK_CACHE_READY:
            await asyncio.to_thread(_disk_clear_sync)
            _DISK_CACHE_COMPLETE = True
        _SEARCH_CACHE.clear()


if USE_MONGO:
    _mongo_defs = [
        (DATABASE_URI, DATABASE_NAME),
        (DATABASE_URI2, DATABASE_NAME2),
        (DATABASE_URI3, DATABASE_NAME3),
        (DATABASE_URI4, DATABASE_NAME4),
        (DATABASE_URI5, DATABASE_NAME5),
    ]
    _seen = set()
    _mongo_collections = []
    for uri, db_name in _mongo_defs:
        if not uri:
            continue
        key = (uri.strip(), (db_name or DATABASE_NAME).strip())
        if key in _seen:
            continue
        _seen.add(key)
        client = AsyncIOMotorClient(key[0])
        _mongo_collections.append(client[key[1]][COLLECTION_NAME])

    if not _mongo_collections:
        raise RuntimeError("At least one MongoDB URI is required when DATABASE_URI mode is enabled")

    MONGO_SHARD_COUNT = len(_mongo_collections)
    logger.info("Media DB shards enabled: %d", MONGO_SHARD_COUNT)

    class MongoUnionCursor:
        def __init__(self, query=None, projection=None):
            self.query = query or {}
            self.projection = projection
            self._sort = None
            self._skip = 0
            self._limit = None

        def sort(self, field, direction):
            self._sort = (field, direction)
            return self

        def skip(self, value):
            self._skip = value
            return self

        def limit(self, value):
            self._limit = value
            return self

        async def to_list(self, length=None):
            requested = self._limit if self._limit is not None else length
            per_shard_limit = self._skip + requested if requested is not None else None

            if MONGO_SHARD_COUNT == 1:
                cursor = _mongo_collections[0].find(self.query, self.projection)
                if self._sort:
                    field, direction = self._sort
                    sort_field = 'created_at' if field == '$natural' else field
                    cursor = cursor.sort(sort_field, direction)
                if self._skip:
                    cursor = cursor.skip(self._skip)
                if requested is not None:
                    cursor = cursor.limit(requested)
                docs = await cursor.to_list(length=requested)
                return [_as_media_doc(d) for d in docs]

            async def _fetch(col):
                cursor = col.find(self.query, self.projection)
                if self._sort:
                    field, direction = self._sort
                    sort_field = 'created_at' if field == '$natural' else field
                    cursor = cursor.sort(sort_field, direction)
                if per_shard_limit is not None:
                    cursor = cursor.limit(per_shard_limit)
                docs = await cursor.to_list(length=per_shard_limit)
                return [_as_media_doc(d) for d in docs]

            parts = await asyncio.gather(*[_fetch(c) for c in _mongo_collections])
            docs = [d for part in parts for d in part]

            if self._sort:
                field, direction = self._sort
                reverse = direction == -1
                key = 'created_at' if field in ('$natural', '_id') else field
                docs.sort(key=lambda d: d.get(key, 0), reverse=reverse)

            docs = docs[self._skip:]
            cap = requested
            if cap is not None:
                docs = docs[:cap]
            if length is not None:
                docs = docs[:length]
            return docs

    class MongoMergedCollection:
        async def find(self, query=None, projection=None):
            return MongoUnionCursor(query=query, projection=projection)

        async def delete_many(self, query):
            global _MEDIA_CACHE_COMPLETE, _DISK_CACHE_COMPLETE
            results = await asyncio.gather(*[col.delete_many(query) for col in _mongo_collections])
            deleted_count = sum(r.deleted_count for r in results)
            if deleted_count:
                _MEDIA_CACHE.clear()
                _MEDIA_CACHE_COMPLETE = False
                _DISK_CACHE_COMPLETE = False
                _SEARCH_CACHE.clear()
            return SQLDeleteResult(deleted_count)

        async def delete_one(self, query):
            global _MEDIA_CACHE_COMPLETE, _DISK_CACHE_COMPLETE
            deleted = 0
            for col in _mongo_collections:
                if deleted:
                    break
                res = await col.delete_one(query)
                deleted += res.deleted_count
            if deleted:
                _MEDIA_CACHE.clear()
                _MEDIA_CACHE_COMPLETE = False
                _DISK_CACHE_COMPLETE = False
                _SEARCH_CACHE.clear()
            return SQLDeleteResult(deleted)

        async def drop(self):
            global _MEDIA_CACHE_COMPLETE, _DISK_CACHE_COMPLETE
            await asyncio.gather(*[col.drop() for col in _mongo_collections])
            _MEDIA_CACHE.clear()
            _MEDIA_CACHE_COMPLETE = True
            if _disk_cache_enabled() and _DISK_CACHE_READY:
                await asyncio.to_thread(_disk_clear_sync)
                _DISK_CACHE_COMPLETE = True
            _SEARCH_CACHE.clear()

    class Media:
        collection = MongoMergedCollection()

        @staticmethod
        async def ensure_indexes():
            async def _create_idx(col, spec):
                try:
                    await col.create_index(spec)
                except OperationFailure as exc:
                    # Some providers can return stale/invalid options for existing
                    # indexes (especially around implicit _id index metadata).
                    # Do not crash bot startup for non-fatal index option issues.
                    if getattr(exc, 'code', None) == 197 or 'InvalidIndexSpecificationOption' in str(exc):
                        logger.warning("Skipping incompatible index option on %s: %s", col.name, exc)
                        return
                    raise

            tasks = []
            for col in _mongo_collections:
                tasks.append(_create_idx(col, [('file_name', 1)]))
                tasks.append(_create_idx(col, [('created_at', -1)]))
            await asyncio.gather(*tasks)

        @staticmethod
        async def count_documents(query=None):
            q = query or {}
            if _MEDIA_CACHE_COMPLETE:
                return sum(1 for doc in _MEDIA_CACHE.values() if _match_filter(doc, q))
            if _DISK_CACHE_COMPLETE and not q:
                return await asyncio.to_thread(_disk_count_all_sync)
            if MONGO_SHARD_COUNT == 1:
                return await _mongo_collections[0].count_documents(q)
            counts = await asyncio.gather(*[col.count_documents(q) for col in _mongo_collections])
            return sum(counts)

        @staticmethod
        def find(query=None):
            return MongoUnionCursor(query=query)

    def _target_collection(file_id: str):
        idx = int(hashlib.md5(file_id.encode('utf-8')).hexdigest(), 16) % len(_mongo_collections)
        return _mongo_collections[idx]

else:
    def _load_docs_sync(query=None):
        with store.begin() as conn:
            rows = conn.execute(text("SELECT file_id, file_ref, file_name, file_size, file_type, mime_type, caption, created_at FROM media")).fetchall()
        docs = []
        for r in rows:
            d = dict(file_id=r[0], _id=r[0], file_ref=r[1], file_name=r[2], file_size=r[3], file_type=r[4], mime_type=r[5], caption=r[6], created_at=r[7])
            if _match_filter(d, query or {}):
                docs.append(d)
        return docs

    class Media:
        collection = SQLMediaCollection()

        @staticmethod
        async def ensure_indexes():
            return

        @staticmethod
        async def count_documents(query=None):
            return len(_load_docs_sync(query))

        @staticmethod
        def find(query=None):
            return SQLCursor(_load_docs_sync(query))


async def preload_media_cache(force=False):
    """Warm media caches after deploy/restart without exhausting RAM."""
    global _MEDIA_CACHE_READY, _MEDIA_CACHE_COMPLETE, _DISK_CACHE_READY, _DISK_CACHE_COMPLETE
    if _MEDIA_CACHE_READY and not force:
        return len(_MEDIA_CACHE)
    async with _MEDIA_CACHE_LOCK:
        if _MEDIA_CACHE_READY and not force:
            return len(_MEDIA_CACHE)

        _MEDIA_CACHE.clear()
        _MEDIA_CACHE_COMPLETE = False
        _DISK_CACHE_COMPLETE = False
        cache_limit = max(int(MEDIA_CACHE_MAX or 0), 0)

        if _disk_cache_enabled() and USE_MONGO:
            await asyncio.to_thread(_disk_init_sync, True)
            projection = {
                'file_ref': 1, 'file_name': 1, 'file_size': 1, 'file_type': 1,
                'mime_type': 1, 'caption': 1, 'created_at': 1,
            }
            total = 0
            batch = []
            recent_docs = []
            for col in _mongo_collections:
                cursor = col.find({}, projection).sort('created_at', -1).batch_size(500)
                async for doc in cursor:
                    if cache_limit > 0 and len(recent_docs) < cache_limit:
                        recent_docs.append(_as_media_doc(doc))
                    batch.append(doc)
                    if len(batch) >= 1000:
                        total += await asyncio.to_thread(_disk_upsert_many_sync, batch)
                        batch = []
                if batch:
                    total += await asyncio.to_thread(_disk_upsert_many_sync, batch)
                    batch = []
            for doc in sorted(recent_docs, key=lambda d: d.get('created_at', 0)):
                fid = doc.get('_id') or doc.get('file_id')
                if fid:
                    _MEDIA_CACHE[fid] = doc
                    while len(_MEDIA_CACHE) > cache_limit:
                        _MEDIA_CACHE.popitem(last=False)
            _DISK_CACHE_READY = True
            _DISK_CACHE_COMPLETE = True
            _MEDIA_CACHE_READY = True
            _SEARCH_CACHE.clear()
            logger.info('Mirrored %d media records into disk cache at %s', total, _disk_cache_path())
            return total

        if cache_limit <= 0:
            _MEDIA_CACHE_READY = True
            _SEARCH_CACHE.clear()
            logger.info('Media runtime cache preload disabled (MEDIA_CACHE_MAX=%s)', MEDIA_CACHE_MAX)
            return 0

        docs = OrderedDict()
        loaded = 0
        truncated = False
        if USE_MONGO:
            projection = {
                'file_ref': 1, 'file_name': 1, 'file_size': 1, 'file_type': 1,
                'mime_type': 1, 'caption': 1, 'created_at': 1,
            }
            per_shard_limit = max(1, (cache_limit // max(MONGO_SHARD_COUNT, 1)) + 1)

            async def _load(col):
                return await (
                    col.find({}, projection)
                    .sort('created_at', -1)
                    .limit(per_shard_limit)
                    .to_list(length=per_shard_limit)
                )

            parts = await asyncio.gather(*[_load(col) for col in _mongo_collections])
            for part in parts:
                loaded += len(part)
                if len(part) >= per_shard_limit:
                    truncated = True
                for doc in part:
                    d = _as_media_doc(doc)
                    fid = d.get('_id') or d.get('file_id')
                    if fid:
                        docs[fid] = d
        else:
            with store.begin() as conn:
                rows = conn.execute(
                    text(
                        "SELECT file_id, file_ref, file_name, file_size, file_type, mime_type, caption, created_at "
                        "FROM media ORDER BY created_at DESC LIMIT :limit"
                    ),
                    {"limit": cache_limit + 1},
                ).fetchall()
            loaded = len(rows)
            truncated = loaded > cache_limit
            for row in rows[:cache_limit]:
                d = _sql_row_to_doc(row)
                docs[d.get('file_id')] = d

        for fid, doc in sorted(docs.items(), key=lambda item: item[1].get('created_at', 0)):
            _MEDIA_CACHE[fid] = doc
            while len(_MEDIA_CACHE) > cache_limit:
                _MEDIA_CACHE.popitem(last=False)

        _MEDIA_CACHE_READY = True
        _MEDIA_CACHE_COMPLETE = not truncated and loaded <= cache_limit
        _SEARCH_CACHE.clear()
        logger.info(
            'Preloaded %d recent media records into runtime cache (complete=%s, max=%d)',
            len(_MEDIA_CACHE), _MEDIA_CACHE_COMPLETE, cache_limit,
        )
        return len(_MEDIA_CACHE)


async def _advanced_duplicate_exists(doc):
    if not ADVANCED_DUPLICATE_SKIP:
        return False
    key = duplicate_key_for_doc(doc)
    if not key or not key[1] or not int(doc.get('file_size') or 0):
        return False

    file_size = int(doc.get('file_size') or 0)
    tolerance = max(10 * 1024 * 1024, int(file_size * 0.02))
    min_size = max(1, file_size - tolerance)
    max_size = file_size + tolerance
    projection = {'file_name': 1, 'file_size': 1}

    def _matches(candidate):
        return (
            candidate
            and duplicate_sizes_match(file_size, int(candidate.get('file_size') or 0))
            and duplicate_key_for_doc(candidate) == key
        )

    try:
        if USE_MONGO:
            query = {'file_size': {'$gte': min_size, '$lte': max_size}}
            for col in _mongo_collections:
                cursor = col.find(query, projection).batch_size(100)
                async for candidate in cursor:
                    if _matches(candidate):
                        return True
            return False

        with store.begin() as conn:
            rows = conn.execute(
                text("SELECT file_id, file_name, file_size FROM media WHERE file_size BETWEEN :min_size AND :max_size"),
                {"min_size": min_size, "max_size": max_size},
            ).fetchall()
        return any(_matches({'_id': row[0], 'file_name': row[1], 'file_size': row[2]}) for row in rows)
    except Exception:
        logger.exception('Advanced duplicate check failed; continuing normal save')
        return False


async def save_file(media):
    """Save file in database"""

    # TODO: Find better way to get same file_id for same media to avoid duplicates
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))
    allowed, reason = media_allowed_for_index(file_name)
    if not allowed:
        logger.info('Skipping %s due to index policy: %s', getattr(media, 'file_name', 'NO_FILE'), reason)
        return False, 3

    if USE_MONGO:
        doc = {
            '_id': file_id,
            'file_ref': file_ref,
            'file_name': file_name,
            'file_size': media.file_size,
            'file_type': media.file_type,
            'mime_type': media.mime_type,
            'caption': media.caption.html if media.caption else None,
            'created_at': time.time(),
        }
        if await _advanced_duplicate_exists(doc):
            logger.warning('%s is already saved as an advanced duplicate', getattr(media, "file_name", "NO_FILE"))
            return False, 0
        try:
            await _target_collection(file_id).insert_one(doc)
            _cache_doc(doc)
            if _disk_cache_enabled() and _DISK_CACHE_READY:
                await asyncio.to_thread(_disk_upsert_many_sync, [doc])
            _SEARCH_CACHE.clear()
        except DuplicateKeyError:
            logger.warning(f'{getattr(media, "file_name", "NO_FILE")} is already saved in database')
            return False, 0
        except Exception:
            logger.exception('Error occurred while saving file in database')
            return False, 2
        logger.info(f'{getattr(media, "file_name", "NO_FILE")} is saved to database')
        return True, 1

    doc = {
        '_id': file_id,
        'file_ref': file_ref,
        'file_name': file_name,
        'file_size': media.file_size,
        'file_type': media.file_type,
        'mime_type': media.mime_type,
        'caption': media.caption.html if media.caption else None,
        'created_at': time.time(),
    }
    if await _advanced_duplicate_exists(doc):
        logger.warning('%s is already saved as an advanced duplicate', getattr(media, "file_name", "NO_FILE"))
        return False, 0

    with store.begin() as conn:
        exists = conn.execute(text("SELECT 1 FROM media WHERE file_id=:fid"), {"fid": file_id}).first()
        if exists:
            return False, 0
        conn.execute(
            text(
                "INSERT INTO media(file_id,file_ref,file_name,file_size,file_type,mime_type,caption) "
                "VALUES (:fid,:fref,:fname,:fsize,:ftype,:mtype,:caption)"
            ),
            {
                "fid": file_id,
                "fref": file_ref,
                "fname": file_name,
                "fsize": media.file_size,
                "ftype": media.file_type,
                "mtype": media.mime_type,
                "caption": media.caption.html if media.caption else None,
            },
        )
    _cache_doc(doc)
    if _disk_cache_enabled() and _DISK_CACHE_READY:
        await asyncio.to_thread(_disk_upsert_many_sync, [doc])
    _SEARCH_CACHE.clear()
    return True, 1


def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0

    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0

            r += bytes([i])

    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref

# SQL fast-path overrides

def _sql_row_to_doc(row):
    return SQLMediaDoc(
        dict(
            file_id=row[0],
            _id=row[0],
            file_ref=row[1],
            file_name=row[2],
            file_size=row[3],
            file_type=row[4],
            mime_type=row[5],
            caption=row[6],
            created_at=row[7],
        )
    )


def _build_mongo_search_filter(query, file_type=None):
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        boundary = r'[\.\+\-_\(\)\[\]\{\}\s]'
        raw_pattern = rf'(\b|{boundary}){re.escape(query)}(\b|{boundary})'
    else:
        raw_pattern = r'.*'.join(map(re.escape, query.split()))

    regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    if USE_CAPTION_FILTER:
        search_filter = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        search_filter = {'file_name': regex}

    if file_type:
        search_filter['file_type'] = file_type
    return search_filter


async def get_search_results(
    query,
    file_type=None,
    max_results=10,
    offset=0,
    filter=False,
    fast=False,
    return_time=False,
):
    """Return matching media files, next offset, total result count, and optionally elapsed time."""

    started_at = time.perf_counter()
    query = (query or '').strip()
    offset = max(int(offset or 0), 0)
    max_results = max(int(max_results or 0), 0)

    if max_results == 0:
        return _finish_search([], '', 0, started_at, return_time)

    cache_key = (query.lower(), file_type, max_results, offset, bool(USE_CAPTION_FILTER), bool(USE_MONGO), fast)
    cached = _cache_get(cache_key)
    if cached is not None:
        files, next_offset, total_results = cached
        return _finish_search(files, next_offset, total_results, started_at, return_time)

    if not USE_MONGO:
        terms = [t for t in query.split() if t]
        where = []
        params = {"offset": offset, "limit": max_results}

        if file_type:
            where.append("file_type = :file_type")
            params["file_type"] = file_type

        if terms:
            term_sql = []
            for idx, term in enumerate(terms):
                key = f"term_{idx}"
                params[key] = f"%{term}%"
                if USE_CAPTION_FILTER:
                    term_sql.append(f"(file_name ILIKE :{key} OR COALESCE(caption, '') ILIKE :{key})")
                else:
                    term_sql.append(f"file_name ILIKE :{key}")
            where.append(" AND ".join(term_sql))

        where_clause = " AND ".join(where) if where else "TRUE"

        with store.begin() as conn:
            if fast:
                total_results = None
                params["limit"] = max_results + 1
            else:
                total_results = int(conn.execute(text(f"SELECT COUNT(*) FROM media WHERE {where_clause}"), params).scalar() or 0)
            rows = conn.execute(
                text(
                    f"""
                    SELECT file_id, file_ref, file_name, file_size, file_type, mime_type, caption, created_at
                    FROM media
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    OFFSET :offset LIMIT :limit
                    """
                ),
                params,
            ).fetchall()

        files = [_sql_row_to_doc(row) for row in rows]
        has_more = fast and len(files) > max_results
        if has_more:
            files = files[:max_results]

        next_offset = offset + max_results
        if fast:
            total_results = offset + len(files) + (1 if has_more else 0)
            if not has_more:
                next_offset = ''
        elif next_offset >= total_results:
            next_offset = ''
        result = (files, next_offset, total_results)
        _cache_set(cache_key, result)
        return _finish_search(*result, started_at, return_time)

    try:
        search_filter = _build_mongo_search_filter(query, file_type=file_type)
    except Exception:
        return _finish_search([], '', 0, started_at, return_time)

    projection = {
        'file_ref': 1,
        'file_name': 1,
        'file_size': 1,
        'file_type': 1,
        'mime_type': 1,
        'caption': 1,
        'created_at': 1,
    }

    if _MEDIA_CACHE_COMPLETE and _MEDIA_CACHE:
        result = _search_media_cache(search_filter, max_results, offset, fast)
        _cache_set(cache_key, result)
        return _finish_search(*result, started_at, return_time)

    if _DISK_CACHE_COMPLETE:
        result = await asyncio.to_thread(_disk_search_sync, query, file_type, max_results, offset, fast)
        _cache_set(cache_key, result)
        return _finish_search(*result, started_at, return_time)

    if MONGO_SHARD_COUNT == 1:
        col = _mongo_collections[0]
        docs_limit = max_results + 1 if fast else max_results
        docs_task = (
            col.find(search_filter, projection)
            .sort('created_at', -1)
            .skip(offset)
            .limit(docs_limit)
            .to_list(length=docs_limit)
        )
        if fast:
            docs = await docs_task
            has_more = len(docs) > max_results
            files = [_as_media_doc(d) for d in docs[:max_results]]
            total_results = offset + len(files) + (1 if has_more else 0)
            next_offset = offset + max_results if has_more else ''
        else:
            count_task = col.count_documents(search_filter)
            total_results, docs = await asyncio.gather(count_task, docs_task)
            next_offset = offset + max_results
            if next_offset >= total_results:
                next_offset = ''
            files = [_as_media_doc(d) for d in docs]
        result = (files, next_offset, total_results)
        _cache_set(cache_key, result)
        return _finish_search(*result, started_at, return_time)

    fetch_limit = offset + max_results + (1 if fast else 0)

    async def _fetch(col):
        docs = await (
            col.find(search_filter, projection)
            .sort('created_at', -1)
            .limit(fetch_limit)
            .to_list(length=fetch_limit)
        )
        return [_as_media_doc(d) for d in docs]

    fetch_task = asyncio.gather(*[_fetch(col) for col in _mongo_collections])
    if fast:
        parts = await fetch_task
        total_results = None
    else:
        count_task = asyncio.gather(*[col.count_documents(search_filter) for col in _mongo_collections])
        counts, parts = await asyncio.gather(count_task, fetch_task)
        total_results = sum(counts)

    files = [d for part in parts for d in part]
    files.sort(key=lambda d: d.get('created_at', 0), reverse=True)
    page_files = files[offset: offset + max_results + (1 if fast else 0)]
    has_more = fast and len(page_files) > max_results
    files = page_files[:max_results]

    next_offset = offset + max_results
    if fast:
        total_results = offset + len(files) + (1 if has_more else 0)
        if not has_more:
            next_offset = ''
    elif next_offset >= total_results:
        next_offset = ''

    result = (files, next_offset, total_results)
    _cache_set(cache_key, result)
    return _finish_search(*result, started_at, return_time)


async def get_file_details(query):
    if not USE_MONGO:
        with store.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT file_id, file_ref, file_name, file_size, file_type, mime_type, caption, created_at "
                    "FROM media WHERE file_id=:file_id LIMIT 1"
                ),
                {"file_id": query},
            ).first()
        return [_sql_row_to_doc(row)] if row else []

    search_filter = {'_id': query}
    if MONGO_SHARD_COUNT == 1:
        filedetails = await _mongo_collections[0].find(search_filter).limit(1).to_list(length=1)
        return [_as_media_doc(filedetails[0])] if filedetails else []

    primary_col = _target_collection(query)
    filedetails = await primary_col.find(search_filter).limit(1).to_list(length=1)
    if filedetails:
        return [_as_media_doc(filedetails[0])]

    fallback_cols = [col for col in _mongo_collections if col is not primary_col]
    fallback_results = await asyncio.gather(
        *[col.find(search_filter).limit(1).to_list(length=1) for col in fallback_cols]
    )
    for filedetails in fallback_results:
        if filedetails:
            return [_as_media_doc(filedetails[0])]
    return []


async def get_movie_list(limit=20):
    if not USE_MONGO:
        with store.begin() as conn:
            rows = conn.execute(text("SELECT file_name FROM media ORDER BY created_at DESC LIMIT 300")).fetchall()
        results = []
        for row in rows:
            name = row[0] or ""
            if not re.search(r"(s\d{1,2}|season\s*\d+).*?(e\d{1,2}|episode\s*\d+)", name, re.I):
                results.append(name)
            if len(results) >= limit:
                break
        return results

    cursor = Media.find().sort("$natural", -1).limit(100)
    files = await cursor.to_list(length=100)
    results = []

    for file in files:
        name = getattr(file, "file_name", "")
        if not re.search(r"(s\d{1,2}|season\s*\d+).*?(e\d{1,2}|episode\s*\d+)", name, re.I):
            results.append(name)
        if len(results) >= limit:
            break
    return results


async def get_series_grouped(limit=30):
    if not USE_MONGO:
        with store.begin() as conn:
            rows = conn.execute(text("SELECT file_name FROM media ORDER BY created_at DESC LIMIT 500")).fetchall()
        grouped = defaultdict(list)

        for row in rows:
            name = row[0] or ""
            match = re.search(r"(.*?)(?:S\d{1,2}|Season\s*\d+).*?(?:E|Ep|Episode)?(\d{1,2})", name, re.I)
            if match:
                title = match.group(1).strip().title()
                episode = int(match.group(2))
                grouped[title].append(episode)
            if len(grouped) >= limit:
                break

        return {
            title: sorted(set(eps))[:10]
            for title, eps in grouped.items() if eps
        }

    cursor = Media.find().sort("$natural", -1).limit(150)
    files = await cursor.to_list(length=150)
    grouped = defaultdict(list)

    for file in files:
        name = getattr(file, "file_name", "")
        match = re.search(r"(.*?)(?:S\d{1,2}|Season\s*\d+).*?(?:E|Ep|Episode)?(\d{1,2})", name, re.I)
        if match:
            title = match.group(1).strip().title()
            episode = int(match.group(2))
            grouped[title].append(episode)

    return {
        title: sorted(set(eps))[:10]
        for title, eps in grouped.items() if eps
    }
