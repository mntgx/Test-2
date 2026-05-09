import logging
import asyncio
import re

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

from database.ia_filterdb import Media, USE_MONGO
from info import ADMINS

logger = logging.getLogger(__name__)

BATCH_SIZE = 20
SLEEP_TIME = 2


@Client.on_message(filters.command("deletefiles") & filters.user(ADMINS))
async def deletemultiplefiles(bot: Client, message: Message):
    if message.chat.type != enums.ChatType.PRIVATE:
        return await message.reply_text(
            f"<b>Hey {message.from_user.mention}, this command won't work in groups. It only works in my PM!</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    try:
        keyword = message.text.split(" ", 1)[1].strip()
        if not keyword:
            raise IndexError
    except IndexError:
        return await message.reply_text(
            f"<b>Hey {message.from_user.mention}, give me a keyword along with the command to delete files.</b>\n"
            "Usage: <code>/deletefiles &lt;keyword&gt;</code>\n"
            "Example: <code>/deletefiles unwanted_movie</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    confirm_button = InlineKeyboardButton("Yes, Continue !", callback_data=f"confirm_delete_files#{keyword}")
    abort_button = InlineKeyboardButton("No, Abort operation !", callback_data="close_message")

    await message.reply_text(
        text=(
            f"<b>Are you sure? Do you want to continue deleting files with the keyword: '{keyword}'?\n\n"
            "Note: This is a destructive action and cannot be undone!</b>"
        ),
        reply_markup=InlineKeyboardMarkup([[confirm_button], [abort_button]]),
        parse_mode=enums.ParseMode.HTML,
        quote=True,
    )


@Client.on_callback_query(filters.regex(r'^confirm_delete_files#'))
async def confirm_and_delete_files_by_keyword(bot: Client, query: CallbackQuery):
    await query.answer()

    _, keyword = query.data.split("#", 1)

    # Build regex used for Mongo matching and count_documents
    raw_pattern = r'(\b|[\.\+\-_])' + re.escape(keyword) + r'(\b|[\.\+\-_])'
    regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    filter_query = {'file_name': regex}

    await query.message.edit_text(
        f"🔍 Searching for files containing <b>'{keyword}'</b> in their filenames...",
        parse_mode=enums.ParseMode.HTML,
    )

    # count_documents works for both Mongo (multi-shard aware) and SQL
    initial_count = await Media.count_documents(filter_query)
    if initial_count == 0:
        return await query.message.edit_text(
            f"❌ No files found with <b>'{keyword}'</b> in their filenames. Deletion aborted.",
            parse_mode=enums.ParseMode.HTML,
        )

    await query.message.edit_text(
        f"Found <code>{initial_count}</code> files containing <b>'{keyword}'</b>. Starting batch deletion...",
        parse_mode=enums.ParseMode.HTML,
    )

    deleted_count = 0

    if USE_MONGO:
        # ── Mongo path (single or multi-shard) ──────────────────────────────
        # Media.collection is MongoMergedCollection which fans out across all shards.
        # find() is async; we await it, then chain limit/to_list on the cursor.
        while True:
            cursor = await Media.collection.find(filter_query, {"_id": 1})
            docs = await cursor.limit(BATCH_SIZE).to_list(length=BATCH_SIZE)

            if not docs:
                break

            ids_to_delete = [doc["_id"] for doc in docs]
            # delete_many fans out across all shards automatically
            result = await Media.collection.delete_many({"_id": {"$in": ids_to_delete}})
            deleted_in_batch = result.deleted_count
            deleted_count += deleted_in_batch

            await query.message.edit_text(
                f"🗑️ Deleted <code>{deleted_in_batch}</code> in this batch. "
                f"Total: <code>{deleted_count}</code> / <code>{initial_count}</code>",
                parse_mode=enums.ParseMode.HTML,
            )

            if deleted_count >= initial_count or deleted_in_batch == 0:
                break

            await asyncio.sleep(SLEEP_TIME)

    else:
        # ── SQL (PostgreSQL) path ────────────────────────────────────────────
        # Use ILIKE for efficient server-side filtering instead of loading the
        # entire media table into memory on every batch iteration.
        from database.sql_store import store
        from sqlalchemy import text as sa_text

        sql_pattern = f"%{keyword}%"

        while True:
            # Fetch + delete inside one transaction per batch so no rows are
            # left behind if the loop is interrupted.
            deleted_in_batch = 0
            with store.begin() as conn:
                rows = conn.execute(
                    sa_text(
                        "SELECT file_id FROM media WHERE file_name ILIKE :pat LIMIT :lim"
                    ),
                    {"pat": sql_pattern, "lim": BATCH_SIZE},
                ).fetchall()

                if rows:
                    ids = [r[0] for r in rows]
                    conn.execute(
                        sa_text(
                            "DELETE FROM media WHERE file_id = ANY(:ids)"
                        ),
                        {"ids": ids},
                    )
                    deleted_in_batch = len(ids)

            if not deleted_in_batch:
                break

            deleted_count += deleted_in_batch

            await query.message.edit_text(
                f"🗑️ Deleted <code>{deleted_in_batch}</code> in this batch. "
                f"Total: <code>{deleted_count}</code> / <code>{initial_count}</code>",
                parse_mode=enums.ParseMode.HTML,
            )

            if deleted_count >= initial_count:
                break

            await asyncio.sleep(SLEEP_TIME)

    await query.message.edit_text(
        f"✅ Finished! Keyword: <b>'{keyword}'</b> — "
        f"Total files deleted: <code>{deleted_count}</code>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r'^close_message$'))
async def close_message(bot: Client, query: CallbackQuery):
    await query.answer()
    await query.message.delete()

# ─── duplicate cleanup ───────────────────────────────────────────────────────
DUP_SCAN_CACHE = {}
DUP_SCAN_LOCK = asyncio.Lock()
DUP_SCAN_BATCH = 2000
DUP_BATCH_DELETE = 2000
DUP_PROGRESS_EVERY = 8
SIZE_BUCKET_BYTES = 10 * 1024 * 1024
LANG_ALIASES = {
    "malayalam": "mal", "mal": "mal", "ml": "mal",
    "tamil": "tam", "tam": "tam", "ta": "tam",
    "hindi": "hin", "hin": "hin", "hi": "hin",
    "english": "eng", "eng": "eng", "en": "eng",
    "telugu": "tel", "tel": "tel", "te": "tel",
    "kannada": "kan", "kan": "kan", "kn": "kan",
}
LANG_RE = re.compile(r"\b(" + "|".join(map(re.escape, sorted(LANG_ALIASES, key=len, reverse=True))) + r")\b", re.I)
SERIES_TOKEN_RE = re.compile(
    r"(?:\bS(?P<s1>\d{1,2})\s*E(?P<e1>\d{1,3})\b|\bSeason\s*(?P<s2>\d{1,2}).*?\b(?:Episode|Ep|E)\s*(?P<e2>\d{1,3})\b)",
    re.I,
)
DROP_WORDS_RE = re.compile(
    r"\b(\d{3,4}p|4k|x264|x265|hevc|h\.?264|h\.?265|aac|ddp?\d?\.\d|web[- ]?dl|webrip|hdrip|bluray|brrip|dvdrip|proper|repack|esub|multi|org|original|uncut)\b",
    re.I,
)


def _clean_dup_name(name: str) -> str:
    raw = str(name or "").lower()
    raw = re.sub(r"\.[a-z0-9]{2,4}$", " ", raw)
    raw = re.sub(r"@\w+", " ", raw)
    raw = re.sub(r"https?://\S+|www\.\S+", " ", raw)
    raw = re.sub(
        r"^[\[\(]([^\]\)]{1,25})[\]\)]\s*",
        lambda m: f" {m.group(1)} " if SERIES_TOKEN_RE.search(m.group(1)) else " ",
        raw,
    )
    raw = DROP_WORDS_RE.sub(" ", raw)
    raw = re.sub(r"[._+\-]+", " ", raw)
    raw = re.sub(r"[^a-z0-9\s]", " ", raw)
    return re.sub(r"\s+", " ", raw).strip()


def _detect_lang(clean_name: str) -> str:
    found = []
    for match in LANG_RE.finditer(clean_name):
        code = LANG_ALIASES.get(match.group(1).lower())
        if code and code not in found:
            found.append(code)
    return "+".join(found) if found else "unknown"


def _series_parts(clean_name: str):
    match = SERIES_TOKEN_RE.search(clean_name)
    if not match:
        return None
    season = int(match.group('s1') or match.group('s2') or 0)
    episode = int(match.group('e1') or match.group('e2') or 0)
    before = clean_name[:match.start()].strip()
    after = clean_name[match.end():].strip()
    title = before if len(before) >= 3 else after
    title = LANG_RE.sub(" ", title)
    title = re.sub(r"\b(19|20)\d{2}\b", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title, season, episode


def _movie_title(clean_name: str):
    title = SERIES_TOKEN_RE.sub(" ", clean_name)
    title = LANG_RE.sub(" ", title)
    return re.sub(r"\s+", " ", title).strip()


def _duplicate_key(doc):
    clean_name = _clean_dup_name(doc.get('file_name'))
    lang = _detect_lang(clean_name)
    series = _series_parts(clean_name)
    if series:
        title, season, episode = series
        if not title:
            title = _movie_title(clean_name)
        return ("series", title, lang, season, episode)
    return ("movie", _movie_title(clean_name), lang)


def _same_size(left: int, right: int) -> bool:
    if not left or not right:
        return True
    return abs(int(left) - int(right)) <= max(10 * 1024 * 1024, int(min(int(left), int(right)) * 0.02))


def _pick_keeper(docs):
    return max(docs, key=lambda d: (int(d.get('file_size') or 0), float(d.get('created_at') or 0)))


async def _safe_edit_duplicate_status(message, text, **kwargs):
    while True:
        try:
            return await message.edit_text(text, **kwargs)
        except FloodWait as fw:
            wait_time = getattr(fw, 'value', 0) + 1
            logger.warning("FloodWait while updating duplicate cleanup status; sleeping %ss", wait_time)
            await asyncio.sleep(wait_time)
        except Exception:
            logger.exception("Failed to update duplicate cleanup status")
            return None


def _delete_op_for_doc(doc):
    if USE_MONGO and doc.get('_col') is not None:
        return ('shard', int(doc['_col']), doc['_id'])
    return ('id', doc['_id'])


def _mark_duplicate(doc, duplicate_ops, duplicate_seen):
    op = _delete_op_for_doc(doc)
    if op not in duplicate_seen:
        duplicate_seen.add(op)
        duplicate_ops.append(op)


def _add_duplicate_candidate(doc, groups, duplicate_ops, duplicate_seen):
    key = _duplicate_key(doc)
    if not key or not key[1]:
        return 0

    file_size = int(doc.get('file_size') or 0)
    size_bucket = 0 if file_size <= 0 else file_size // SIZE_BUCKET_BYTES
    first_free_key = None

    for near_bucket in (size_bucket, size_bucket - 1, size_bucket + 1):
        slot = 0
        while True:
            grouped_key = (*key, near_bucket, slot)
            bucket = groups.get(grouped_key)
            if not bucket:
                if first_free_key is None and near_bucket == size_bucket:
                    first_free_key = grouped_key
                break
            if _same_size(file_size, bucket['keeper'].get('file_size')):
                was_single = bucket['count'] == 1
                current_keeper = bucket['keeper']
                better = _pick_keeper([current_keeper, doc])
                if better['_id'] == doc['_id']:
                    _mark_duplicate(current_keeper, duplicate_ops, duplicate_seen)
                    bucket['keeper'] = doc
                else:
                    _mark_duplicate(doc, duplicate_ops, duplicate_seen)
                bucket['count'] += 1
                return 1 if was_single else 0
            slot += 1

    groups[first_free_key or (*key, size_bucket, 0)] = {'keeper': doc, 'count': 1}
    return 0


async def _scan_duplicate_ops(status=None):
    groups = {}
    seen_ids = set()
    duplicate_ops = []
    duplicate_seen = set()
    checked = duplicate_sets = 0
    last_edit = 0

    async def _progress(force=False):
        nonlocal last_edit
        now = asyncio.get_running_loop().time()
        if not status or (not force and now - last_edit < DUP_PROGRESS_EVERY):
            return
        last_edit = now
        try:
            await _safe_edit_duplicate_status(
                status,
                "🔍 Scanning DB for duplicate files...\n\n"
                f"Checked: <code>{checked}</code>\n"
                f"Duplicate groups: <code>{duplicate_sets}</code>\n"
                f"Duplicate files found: <code>{len(duplicate_ops)}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

    def _make_doc(doc, col_idx=None):
        fid = doc.get('_id') or doc.get('file_id')
        if not fid:
            return None
        return {
            '_id': fid,
            '_col': col_idx,
            'file_name': doc.get('file_name') or '',
            'file_size': int(doc.get('file_size') or 0),
            'created_at': float(doc.get('created_at') or 0),
        }

    def _handle_doc(doc, col_idx=None):
        nonlocal checked, duplicate_sets
        item = _make_doc(doc, col_idx=col_idx)
        if not item:
            return
        checked += 1
        fid = item['_id']
        if fid in seen_ids:
            # Same file_id duplicated across Mongo shards: delete only this shard copy.
            _mark_duplicate(item, duplicate_ops, duplicate_seen)
            return
        seen_ids.add(fid)
        duplicate_sets += _add_duplicate_candidate(item, groups, duplicate_ops, duplicate_seen)

    if USE_MONGO:
        import database.ia_filterdb as media_db
        projection = {'file_name': 1, 'file_size': 1, 'created_at': 1}
        for col_idx, col in enumerate(media_db._mongo_collections):
            cursor = col.find({}, projection).batch_size(DUP_SCAN_BATCH)
            async for doc in cursor:
                _handle_doc(doc, col_idx=col_idx)
                if checked % DUP_SCAN_BATCH == 0:
                    await _progress()
                    await asyncio.sleep(0)
    else:
        from database.sql_store import store
        from sqlalchemy import text as sa_text

        with store.begin() as conn:
            result = conn.execute(sa_text("SELECT file_id, file_name, file_size, created_at FROM media"))
            while True:
                rows = result.fetchmany(DUP_SCAN_BATCH)
                if not rows:
                    break
                for row in rows:
                    _handle_doc({'_id': row[0], 'file_name': row[1], 'file_size': row[2], 'created_at': row[3]})
                await _progress()
                await asyncio.sleep(0)

    await _progress(force=True)
    return checked, duplicate_ops, duplicate_sets


@Client.on_message(filters.command("deleteduplicates") & filters.user(ADMINS))
async def delete_duplicate_files(bot: Client, message: Message):
    if message.chat.type != enums.ChatType.PRIVATE:
        return await message.reply_text("<b>This command only works in my PM.</b>", parse_mode=enums.ParseMode.HTML)

    if DUP_SCAN_LOCK.locked():
        return await message.reply_text("⚠️ Duplicate cleanup is already running. Please wait until it finishes.")

    status = await message.reply_text("🔍 Scanning DB for duplicate files...", quote=True)
    async with DUP_SCAN_LOCK:
        checked, duplicate_ops, duplicate_sets = await _scan_duplicate_ops(status)

    if not duplicate_ops:
        return await _safe_edit_duplicate_status(status, f"✅ Scan complete. Checked <code>{checked}</code> files. No duplicates found.")

    DUP_SCAN_CACHE[message.from_user.id] = duplicate_ops
    await _safe_edit_duplicate_status(
        status,
        "⚠️ Duplicate scan complete.\n\n"
        f"Checked files: <code>{checked}</code>\n"
        f"Duplicate groups: <code>{duplicate_sets}</code>\n"
        f"Duplicate files to delete: <code>{len(duplicate_ops)}</code>\n\n"
        "Language-aware matching is enabled, so Malayalam/Tamil/etc. versions are kept separately.\n"
        "Continue deletion?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Delete duplicates", callback_data=f"dupedel:yes:{message.from_user.id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"dupedel:no:{message.from_user.id}")],
        ]),
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^dupedel:(yes|no):(\d+)$"))
async def duplicate_delete_callback(bot: Client, query: CallbackQuery):
    action, owner = query.matches[0].group(1), int(query.matches[0].group(2))
    if query.from_user.id != owner:
        return await query.answer("This duplicate cleanup is not for you.", show_alert=True)
    ids = DUP_SCAN_CACHE.pop(owner, [])
    if action == "no":
        await query.answer("Cancelled")
        return await _safe_edit_duplicate_status(query.message, "❌ Duplicate deletion cancelled.")
    if not ids:
        return await _safe_edit_duplicate_status(query.message, "No cached duplicate scan found. Run /deleteduplicates again.")

    await query.answer("Deleting duplicates...")
    deleted = 0
    total = len(ids)
    id_ops = [op[1] for op in ids if op[0] == 'id']
    shard_ops = [op for op in ids if op[0] == 'shard']

    last_delete_edit = 0

    async def _delete_progress(force=False):
        nonlocal last_delete_edit
        now = asyncio.get_running_loop().time()
        if not force and now - last_delete_edit < DUP_PROGRESS_EVERY:
            return
        last_delete_edit = now
        await _safe_edit_duplicate_status(
            query.message,
            f"🗑️ Deleted <code>{deleted}</code>/<code>{total}</code> duplicate files...",
            parse_mode=enums.ParseMode.HTML,
        )

    if id_ops and USE_MONGO:
        for start in range(0, len(id_ops), DUP_BATCH_DELETE):
            batch = id_ops[start:start + DUP_BATCH_DELETE]
            result = await Media.collection.delete_many({'_id': {'$in': batch}})
            deleted += result.deleted_count
            await _delete_progress()
            await asyncio.sleep(0)
    elif id_ops:
        from database.sql_store import store
        from sqlalchemy import text as sa_text
        import database.ia_filterdb as media_db

        for start in range(0, len(id_ops), DUP_BATCH_DELETE):
            batch = id_ops[start:start + DUP_BATCH_DELETE]
            with store.begin() as conn:
                result = conn.execute(sa_text("DELETE FROM media WHERE file_id = ANY(:ids)"), {"ids": batch})
                deleted += result.rowcount or 0
            for fid in batch:
                media_db._uncache_doc(fid)
            if media_db._disk_cache_enabled() and media_db._DISK_CACHE_READY:
                await asyncio.to_thread(media_db._disk_delete_many_sync, batch)
            media_db._SEARCH_CACHE.clear()
            await _delete_progress()
            await asyncio.sleep(0)

    if shard_ops and USE_MONGO:
        import database.ia_filterdb as media_db
        shard_batches = {}
        for _, col_idx, file_id in shard_ops:
            shard_batches.setdefault(col_idx, []).append(file_id)
        for col_idx, file_ids in shard_batches.items():
            col = media_db._mongo_collections[col_idx]
            for start in range(0, len(file_ids), DUP_BATCH_DELETE):
                batch = file_ids[start:start + DUP_BATCH_DELETE]
                result = await col.delete_many({'_id': {'$in': batch}})
                deleted += result.deleted_count
                await _delete_progress()
                await asyncio.sleep(0)

    await _safe_edit_duplicate_status(
        query.message,
        f"✅ Duplicate cleanup finished. Deleted <code>{deleted}</code> files.",
        parse_mode=enums.ParseMode.HTML,
    )
