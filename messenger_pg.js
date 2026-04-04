'use strict';

const { Pool } = require('pg');

let pool = null;
let enabled = false;

function env(name, def = '') {
  const v = process.env[name];
  if (v == null) return def;
  const t = String(v).trim();
  return t.length ? t : def;
}

function sortedPair(a, b) {
  const x = String(a);
  const y = String(b);
  return x < y ? [x, y] : [y, x];
}

function directChatId(a, b) {
  const [u1, u2] = sortedPair(a, b);
  if (!u1 || !u2) return '';
  return `dm:${u1}::${u2}`;
}

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id VARCHAR(120) PRIMARY KEY,
      username VARCHAR(64) NOT NULL DEFAULT '',
      display_name VARCHAR(200) NOT NULL DEFAULT '',
      avatar_url TEXT NOT NULL DEFAULT '',
      password_hash TEXT,
      status VARCHAR(500) NOT NULL DEFAULT '',
      last_seen BIGINT NOT NULL DEFAULT 0,
      blacklist_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      blacklist_meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      friend_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      online BOOLEAN NOT NULL DEFAULT false,
      updated_at BIGINT NOT NULL DEFAULT 0
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS settings (
      user_id VARCHAR(120) PRIMARY KEY,
      who_can_write VARCHAR(16) NOT NULL DEFAULT 'all',
      who_can_call VARCHAR(16) NOT NULL DEFAULT 'all',
      who_can_see_profile VARCHAR(16) NOT NULL DEFAULT 'all',
      CONSTRAINT settings_write_chk CHECK (who_can_write IN ('all','friends','nobody')),
      CONSTRAINT settings_call_chk CHECK (who_can_call IN ('all','friends','nobody')),
      CONSTRAINT settings_profile_chk CHECK (who_can_see_profile IN ('all','friends','nobody'))
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS chats (
      id VARCHAR(220) PRIMARY KEY,
      user1_id VARCHAR(120) NOT NULL,
      user2_id VARCHAR(120) NOT NULL,
      last_message_id VARCHAR(100),
      last_message_preview JSONB,
      meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at BIGINT NOT NULL DEFAULT 0,
      CONSTRAINT chats_pair_uniq UNIQUE (user1_id, user2_id)
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id VARCHAR(100) PRIMARY KEY,
      chat_id VARCHAR(220) NOT NULL,
      sender_id VARCHAR(120) NOT NULL,
      recipient_id VARCHAR(120) NOT NULL,
      text TEXT,
      type VARCHAR(32) NOT NULL DEFAULT 'text',
      file_url TEXT,
      audio_mime VARCHAR(80) NOT NULL DEFAULT '',
      image_mime VARCHAR(80) NOT NULL DEFAULT '',
      duration_ms INT NOT NULL DEFAULT 0,
      reply_to VARCHAR(100) NOT NULL DEFAULT '',
      forwarded_from VARCHAR(100) NOT NULL DEFAULT '',
      delivered_by JSONB NOT NULL DEFAULT '[]'::jsonb,
      read_by JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at BIGINT NOT NULL,
      edited_at BIGINT NOT NULL DEFAULT 0,
      deleted_at BIGINT NOT NULL DEFAULT 0
    )
  `);

  // На случай уже существующей таблицы (если у вас код обновился, а БД нет).
  await pool.query(`ALTER TABLE messages ADD COLUMN IF NOT EXISTS image_mime VARCHAR(80) NOT NULL DEFAULT ''`);
  await pool.query(`ALTER TABLE messages ADD COLUMN IF NOT EXISTS delivered_by JSONB NOT NULL DEFAULT '[]'::jsonb`);
  await pool.query(`ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_by JSONB NOT NULL DEFAULT '[]'::jsonb`);

  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, created_at) WHERE deleted_at = 0'
  );
  await pool.query('CREATE INDEX IF NOT EXISTS idx_chats_u1 ON chats(user1_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_chats_u2 ON chats(user2_id)');
}

async function initMessengerPostgres() {
  const conn = env('DATABASE_URL');
  if (!conn) {
    console.warn('[messenger_pg] DATABASE_URL not set');
    return false;
  }
  try {
    const poolOpts = {
      connectionString: conn,
      max: Number(env('PG_POOL_MAX', '10')) || 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: Number(env('PG_CONNECT_TIMEOUT_MS', '20000')) || 20000
    };
    if (env('DATABASE_SSL') === '0') {
      poolOpts.ssl = false;
    } else if (/sslmode=require|sslmode=verify-full/i.test(conn) || env('PG_SSL') === '1') {
      poolOpts.ssl = { rejectUnauthorized: env('PG_SSL_REJECT_UNAUTHORIZED') === '1' };
    }
    pool = new Pool(poolOpts);
    await pool.query('SELECT 1');
    await ensureTables();
    enabled = true;
    console.log('[messenger_pg] connected (PostgreSQL)');
    return true;
  } catch (err) {
    console.error('[messenger_pg] init failed:', err && err.message);
    try {
      if (pool) await pool.end();
    } catch (_) {}
    pool = null;
    enabled = false;
    return false;
  }
}

function isEnabled() {
  return enabled && pool;
}

function parseJsonCol(val, fallback) {
  if (val == null) return fallback;
  if (typeof val === 'object') return val;
  if (typeof val === 'string') return safeJson(val, fallback);
  return fallback;
}

function rowToServerProfile(userId, userRow, settingsRow) {
  if (!userRow) return null;
  const bl = parseJsonCol(userRow.blacklist_json, []);
  const blm = parseJsonCol(userRow.blacklist_meta_json, {});
  const friends = parseJsonCol(userRow.friend_ids_json, []);
  return {
    id: userId,
    name: userRow.display_name || '',
    avatar: userRow.avatar_url || '',
    username: userRow.username || '',
    statusText: userRow.status || '',
    blacklist: Array.isArray(bl) ? bl : [],
    blacklistMeta: blm && typeof blm === 'object' ? blm : {},
    friendIds: Array.isArray(friends) ? friends : [],
    online: !!userRow.online,
    lastSeenAt: Number(userRow.last_seen) || 0,
    privacy: {
      canWrite: settingsRow?.who_can_write || 'all',
      canCall: settingsRow?.who_can_call || 'all',
      canViewProfile: settingsRow?.who_can_see_profile || 'all'
    }
  };
}

function safeJson(s, def) {
  try {
    return JSON.parse(s);
  } catch {
    return def;
  }
}

async function getSettingsRow(userId) {
  const { rows } = await pool.query(
    'SELECT who_can_write, who_can_call, who_can_see_profile FROM settings WHERE user_id = $1 LIMIT 1',
    [userId]
  );
  return rows[0] || null;
}

async function getProfile(userId) {
  const { rows } = await pool.query('SELECT * FROM users WHERE id = $1 LIMIT 1', [userId]);
  const userRow = rows[0];
  const s = await getSettingsRow(userId);
  return rowToServerProfile(userId, userRow, s);
}

async function upsertProfile(userId, patch) {
  const now = Date.now();
  const existing = await getProfile(userId);
  const merged = {
    name: patch.name != null ? patch.name : existing?.name || '',
    avatar: patch.avatar != null ? patch.avatar : existing?.avatar || '',
    username: patch.username != null ? patch.username : existing?.username || '',
    statusText: patch.statusText != null ? patch.statusText : existing?.statusText || '',
    blacklist: patch.blacklist != null ? patch.blacklist : existing?.blacklist || [],
    blacklistMeta: patch.blacklistMeta != null ? patch.blacklistMeta : existing?.blacklistMeta || {},
    friendIds: patch.friendIds != null ? patch.friendIds : existing?.friendIds || [],
    online: patch.online != null ? !!patch.online : !!existing?.online,
    lastSeenAt: patch.lastSeenAt != null ? patch.lastSeenAt : existing?.lastSeenAt || 0
  };
  await pool.query(
    `INSERT INTO users (id, username, display_name, avatar_url, password_hash, status, last_seen, blacklist_json, blacklist_meta_json, friend_ids_json, online, updated_at)
     VALUES ($1,$2,$3,$4,NULL,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11)
     ON CONFLICT (id) DO UPDATE SET
       username = EXCLUDED.username,
       display_name = EXCLUDED.display_name,
       avatar_url = EXCLUDED.avatar_url,
       status = EXCLUDED.status,
       last_seen = EXCLUDED.last_seen,
       blacklist_json = EXCLUDED.blacklist_json,
       blacklist_meta_json = EXCLUDED.blacklist_meta_json,
       friend_ids_json = EXCLUDED.friend_ids_json,
       online = EXCLUDED.online,
       updated_at = EXCLUDED.updated_at`,
    [
      userId,
      merged.username,
      merged.name,
      merged.avatar,
      merged.statusText,
      merged.lastSeenAt,
      JSON.stringify(merged.blacklist),
      JSON.stringify(merged.blacklistMeta),
      JSON.stringify(merged.friendIds),
      merged.online,
      now
    ]
  );
  if (patch.privacy) {
    await upsertSettings(userId, patch.privacy);
  }
  return getProfile(userId);
}

async function upsertSettings(userId, privacy) {
  const w = privacy.canWrite || privacy.whoCanWrite || 'all';
  const c = privacy.canCall || privacy.whoCanCall || 'all';
  const p = privacy.canViewProfile || privacy.whoCanSeeProfile || 'all';
  await pool.query(
    `INSERT INTO settings (user_id, who_can_write, who_can_call, who_can_see_profile)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (user_id) DO UPDATE SET
       who_can_write = EXCLUDED.who_can_write,
       who_can_call = EXCLUDED.who_can_call,
       who_can_see_profile = EXCLUDED.who_can_see_profile`,
    [userId, w, c, p]
  );
}

async function listAllUserIds() {
  const { rows } = await pool.query('SELECT id FROM users');
  return rows.map((r) => r.id);
}

async function findDirectChat(a, b) {
  const [u1, u2] = sortedPair(String(a), String(b));
  const { rows } = await pool.query(
    'SELECT id, user1_id, user2_id, last_message_id, last_message_preview, updated_at, meta_json FROM chats WHERE user1_id = $1 AND user2_id = $2 LIMIT 1',
    [u1, u2]
  );
  if (!rows[0]) return null;
  return rowToChat(rows[0], u1, u2);
}

async function getOrCreateChat(a, b) {
  const got = await findDirectChat(a, b);
  if (got) return got;
  const [u1, u2] = sortedPair(String(a), String(b));
  const id = directChatId(u1, u2);
  const now = Date.now();
  const meta = {
    clearedBy: {},
    removedBy: {},
    blockedBy: {},
    pinnedBy: {},
    archivedBy: {},
    mutedBy: {},
    typingBy: {}
  };
  await pool.query(
    'INSERT INTO chats (id, user1_id, user2_id, last_message_id, last_message_preview, updated_at, meta_json) VALUES ($1,$2,$3,NULL,NULL,$4,$5::jsonb)',
    [id, u1, u2, now, JSON.stringify(meta)]
  );
  return { id, members: [u1, u2], meta, lastMessage: null, updatedAt: now };
}

function rowToChat(row, u1, u2) {
  const meta = parseJsonCol(row.meta_json, {});
  const m = {
    clearedBy: meta.clearedBy || {},
    removedBy: meta.removedBy || {},
    blockedBy: meta.blockedBy || {},
    pinnedBy: meta.pinnedBy || {},
    archivedBy: meta.archivedBy || {},
    mutedBy: meta.mutedBy || {},
    typingBy: meta.typingBy || {}
  };
  let lastMessage = null;
  const preview = row.last_message_preview;
  if (preview && typeof preview === 'object') {
    lastMessage = preview;
  } else if (typeof preview === 'string') {
    try {
      lastMessage = JSON.parse(preview);
    } catch {
      lastMessage = null;
    }
  }
  return {
    id: row.id,
    kind: 'direct',
    members: [row.user1_id || u1, row.user2_id || u2],
    createdAt: Number(row.updated_at) || Date.now(),
    meta: m,
    lastMessage,
    updatedAt: Number(row.updated_at) || 0
  };
}

async function updateChatMeta(chatId, meta) {
  await pool.query('UPDATE chats SET meta_json = $1::jsonb, updated_at = $2 WHERE id = $3', [
    JSON.stringify(meta),
    Date.now(),
    chatId
  ]);
}

async function updateLastMessagePreview(chatId, lastMessageObj, updatedAt) {
  const lm = lastMessageObj == null ? null : JSON.stringify(lastMessageObj);
  const mid = lastMessageObj && lastMessageObj.id ? lastMessageObj.id : null;
  await pool.query(
    'UPDATE chats SET last_message_preview = $1::jsonb, last_message_id = $2, updated_at = $3 WHERE id = $4',
    [lm, mid, updatedAt, chatId]
  );
}

function rowToMessage(row) {
  const dbType = row.type;
  const isVoice = dbType === 'audio';
  const isImage = dbType === 'image';
  const isVideoNote = dbType === 'video_note';
  const isVideo = dbType === 'video';
  const createdAt = Number(row.created_at) || 0;
  const base = {
    id: row.id,
    chatId: row.chat_id,
    fromId: row.sender_id,
    toId: row.recipient_id,
    createdAt,
    text: row.text || '',
    messageKind: isVoice ? 'voice' : isImage ? 'image' : isVideoNote ? 'video_note' : isVideo ? 'video' : 'text',
    replyTo: row.reply_to || '',
    forwardedFromMessageId: row.forwarded_from || '',
    editedAt: Number(row.edited_at) || 0,
    deletedAt: Number(row.deleted_at) || 0,
    mimeType: '',
    deliveredBy: Array.isArray(row.delivered_by) ? row.delivered_by : [],
    readBy: Array.isArray(row.read_by) ? row.read_by : []
  };
  if (isVoice) {
    base.audioBase64 = row.file_url || '';
    base.audioMime = row.audio_mime || 'audio/webm';
    base.durationMs = Number(row.duration_ms) || 0;
  } else {
    base.audioMime = '';
    base.audioBase64 = '';
    base.durationMs = isVideoNote ? Number(row.duration_ms) || 0 : 0;
  }
  if (isImage) {
    base.imageBase64 = row.file_url || '';
    base.mimeType = row.image_mime || 'image/jpeg';
  }
  if (isVideo || isVideoNote) {
    base.videoBase64 = row.file_url || '';
    base.videoMime = row.audio_mime || 'video/mp4';
  }
  return base;
}

async function insertMessage(msg) {
  const mk = msg.messageKind || msg.kind;
  const isVoice = mk === 'voice' || mk === 'audio';
  const isImage = mk === 'image';
  const isVideoNote = mk === 'video_note';
  const isVideo = mk === 'video';
  const type = isVoice ? 'audio' : isImage ? 'image' : isVideoNote ? 'video_note' : isVideo ? 'video' : 'text';
  const fileUrl = isVoice
    ? msg.audioBase64 || ''
    : isImage
      ? msg.imageBase64 || ''
      : isVideo || isVideoNote
        ? msg.videoBase64 || ''
        : '';
  const mimeCol = isVoice ? msg.audioMime || '' : isVideo || isVideoNote ? msg.videoMime || 'video/mp4' : '';
  const imageMimeCol = isImage ? msg.mimeType || msg.imageMime || 'image/jpeg' : '';
  const createdAt = Number(msg.createdAt || msg.at || Date.now());
  const deliveredBy = Array.isArray(msg.deliveredBy) ? msg.deliveredBy.map(String) : [];
  const readBy = Array.isArray(msg.readBy) ? msg.readBy.map(String) : [];
  await pool.query(
    `INSERT INTO messages (id, chat_id, sender_id, recipient_id, text, type, file_url, duration_ms, audio_mime, image_mime, reply_to, forwarded_from, delivered_by, read_by, created_at, edited_at, deleted_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15,$16,$17)`,
    [
      msg.id,
      msg.chatId,
      msg.fromId,
      msg.toId,
      msg.text || '',
      type,
      fileUrl,
      msg.durationMs || 0,
      mimeCol,
      imageMimeCol,
      msg.replyTo || '',
      msg.forwardedFromMessageId || msg.forwardedFrom || '',
      JSON.stringify(deliveredBy),
      JSON.stringify(readBy),
      createdAt,
      msg.editedAt || 0,
      msg.deletedAt || 0
    ]
  );
}

async function getLatestMessageInChatAfter(chatId, clearedAfterTs = 0) {
  const t = Math.max(0, Number(clearedAfterTs) || 0);
  const { rows } = await pool.query(
    'SELECT * FROM messages WHERE chat_id = $1 AND deleted_at = 0 AND created_at >= $2 ORDER BY created_at DESC LIMIT 1',
    [chatId, t]
  );
  return rows[0] ? rowToMessage(rows[0]) : null;
}

async function listMessagesForChat(chatId, clearedAfterTs = 0, limit = 500) {
  const lim = Math.min(Math.max(Number(limit) || 500, 1), 2000);
  const cleared = Math.max(0, Number(clearedAfterTs) || 0);
  const { rows } = await pool.query(
    `SELECT * FROM messages WHERE chat_id = $1 AND deleted_at = 0 AND created_at >= $2 ORDER BY created_at ASC LIMIT $3`,
    [chatId, cleared, lim]
  );
  return rows.map(rowToMessage);
}

async function getMessageById(id) {
  const { rows } = await pool.query('SELECT * FROM messages WHERE id = $1 LIMIT 1', [id]);
  return rows[0] ? rowToMessage(rows[0]) : null;
}

async function deleteMessageByIdHard(id) {
  await pool.query('DELETE FROM messages WHERE id = $1', [id]);
}

async function addMessageReadBy(messageId, readerUserId) {
  const mid = String(messageId || '').trim();
  const rid = String(readerUserId || '').trim();
  if (!mid || !rid) return false;
  const { rows } = await pool.query('SELECT read_by FROM messages WHERE id = $1 LIMIT 1', [mid]);
  const prev = rows[0] && Array.isArray(rows[0].read_by) ? rows[0].read_by : [];
  const already = prev.some((x) => String(x) === rid);
  if (already) return false;
  const next = [...prev.map(String), rid];
  await pool.query('UPDATE messages SET read_by = $1::jsonb WHERE id = $2', [JSON.stringify(next), mid]);
  return true;
}

async function updateMessageFields(id, patch) {
  const sets = [];
  const vals = [];
  let n = 1;
  if (patch.text !== undefined) {
    sets.push(`text = $${n++}`);
    vals.push(patch.text);
  }
  if (patch.editedAt !== undefined) {
    sets.push(`edited_at = $${n++}`);
    vals.push(patch.editedAt);
  }
  if (patch.deletedAt !== undefined) {
    sets.push(`deleted_at = $${n++}`);
    vals.push(patch.deletedAt);
  }
  if (!sets.length) return;
  vals.push(id);
  await pool.query(`UPDATE messages SET ${sets.join(', ')} WHERE id = $${n}`, vals);
}

async function getChatById(chatId) {
  const { rows } = await pool.query(
    'SELECT id, user1_id, user2_id, last_message_id, last_message_preview, updated_at, meta_json FROM chats WHERE id = $1 LIMIT 1',
    [chatId]
  );
  if (!rows[0]) return null;
  return rowToChat(rows[0]);
}

async function loadChatMeta(chatId) {
  const ch = await getChatById(chatId);
  return ch?.meta || null;
}

async function listChatsForUser(userId) {
  const uid = String(userId);
  const { rows } = await pool.query(
    `SELECT id, user1_id, user2_id, last_message_id, last_message_preview, updated_at, meta_json FROM chats
     WHERE user1_id = $1 OR user2_id = $1 ORDER BY updated_at DESC`,
    [uid]
  );
  return rows.map((r) => rowToChat(r));
}

async function deleteChatRow(chatId) {
  await pool.query('DELETE FROM messages WHERE chat_id = $1', [chatId]);
  await pool.query('DELETE FROM chats WHERE id = $1', [chatId]);
}

async function setUserOnlineFlags(userId, online) {
  const now = Date.now();
  await pool.query(
    'UPDATE users SET online = $1, last_seen = $2, updated_at = $3 WHERE id = $4',
    [online, now, now, userId]
  );
}

const initMessengerMysql = initMessengerPostgres;

module.exports = {
  initMessengerPostgres,
  initMessengerMysql,
  isEnabled,
  getProfile,
  upsertProfile,
  upsertSettings,
  listAllUserIds,
  findDirectChat,
  getOrCreateChat,
  getChatById,
  loadChatMeta,
  updateChatMeta,
  updateLastMessagePreview,
  insertMessage,
  listMessagesForChat,
  getLatestMessageInChatAfter,
  getMessageById,
  deleteMessageByIdHard,
  updateMessageFields,
  listChatsForUser,
  deleteChatRow,
  setUserOnlineFlags,
  directChatId,
  addMessageReadBy
};
