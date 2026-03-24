'use strict';

const mysql = require('mysql2/promise');

let pool = null;
let enabled = false;

function env(name, def = '') {
  const v = process.env[name];
  return v != null && String(v).length ? String(v) : def;
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
    CREATE TABLE IF NOT EXISTS chats (
      id VARCHAR(220) NOT NULL PRIMARY KEY,
      user1_id VARCHAR(120) NOT NULL,
      user2_id VARCHAR(120) NOT NULL,
      last_message TEXT NULL,
      updated_at BIGINT NOT NULL DEFAULT 0,
      meta_json JSON NULL,
      UNIQUE KEY uniq_users (user1_id, user2_id),
      KEY idx_u1 (user1_id),
      KEY idx_u2 (user2_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id VARCHAR(100) NOT NULL PRIMARY KEY,
      chat_id VARCHAR(220) NOT NULL,
      from_id VARCHAR(120) NOT NULL,
      to_id VARCHAR(120) NOT NULL,
      text MEDIUMTEXT NULL,
      type VARCHAR(32) NOT NULL DEFAULT 'text',
      file_url MEDIUMTEXT NULL,
      duration_ms INT NOT NULL DEFAULT 0,
      audio_mime VARCHAR(80) NULL DEFAULT '',
      reply_to VARCHAR(100) NOT NULL DEFAULT '',
      forwarded_from VARCHAR(100) NOT NULL DEFAULT '',
      created_at BIGINT NOT NULL,
      edited_at BIGINT NOT NULL DEFAULT 0,
      deleted_at BIGINT NOT NULL DEFAULT 0,
      KEY idx_chat_time (chat_id, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS settings (
      user_id VARCHAR(120) NOT NULL PRIMARY KEY,
      who_can_write ENUM('all','friends','nobody') NOT NULL DEFAULT 'all',
      who_can_call ENUM('all','friends','nobody') NOT NULL DEFAULT 'all',
      who_can_see_profile ENUM('all','friends','nobody') NOT NULL DEFAULT 'all'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS messenger_profiles (
      user_id VARCHAR(120) NOT NULL PRIMARY KEY,
      display_name VARCHAR(200) NOT NULL DEFAULT '',
      avatar_url MEDIUMTEXT NOT NULL,
      username VARCHAR(64) NOT NULL DEFAULT '',
      status_text VARCHAR(200) NOT NULL DEFAULT '',
      blacklist_json JSON NULL,
      blacklist_meta_json JSON NULL,
      friend_ids_json JSON NULL,
      online TINYINT(1) NOT NULL DEFAULT 0,
      last_seen_ms BIGINT NOT NULL DEFAULT 0,
      updated_at BIGINT NOT NULL DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);
  try {
    await pool.query('ALTER TABLE messenger_profiles MODIFY COLUMN avatar_url MEDIUMTEXT NOT NULL');
  } catch (_) {}
}

async function initMessengerMysql() {
  const database = env('DB_NAME') || env('MYSQL_DATABASE', '');
  if (!database) {
    console.warn('[messenger_mysql] DB_NAME / MYSQL_DATABASE not set — messenger DB disabled');
    return false;
  }
  const host = env('DB_HOST') || env('MYSQL_HOST', '127.0.0.1');
  const port = Number(env('DB_PORT') || env('MYSQL_PORT', '3306')) || 3306;
  const user = env('DB_USER') || env('MYSQL_USER', 'root');
  const password = env('DB_PASSWORD') || env('MYSQL_PASSWORD', '');
  const safeDb = database.replace(/`/g, '');
  const allowCreateDb = env('MESSENGER_CREATE_DATABASE', '') === '1';
  try {
    if (allowCreateDb) {
      const admin = await mysql.createConnection({ host, port, user, password });
      await admin.query(
        `CREATE DATABASE IF NOT EXISTS \`${safeDb}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
      );
      await admin.end();
    }

    pool = mysql.createPool({
      host,
      port,
      user,
      password,
      database: safeDb,
      waitForConnections: true,
      connectionLimit: 10,
      namedPlaceholders: true,
      charset: 'utf8mb4',
    });
    await pool.query('SELECT 1');
    await ensureTables();
    enabled = true;
    console.log('[messenger_mysql] connected:', host, safeDb);
    return true;
  } catch (err) {
    console.error('[messenger_mysql] init failed:', err && err.message);
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

/** Профиль в формате server.js (getUserProfile / chats.json users). */
function rowToServerProfile(userId, row, settingsRow) {
  if (!row) return null;
  const bl = parseJsonCol(row.blacklist_json, []);
  const blm = parseJsonCol(row.blacklist_meta_json, {});
  const friends = parseJsonCol(row.friend_ids_json, []);
  return {
    id: userId,
    name: row.display_name || '',
    avatar: row.avatar_url || '',
    username: row.username || '',
    statusText: row.status_text || '',
    blacklist: Array.isArray(bl) ? bl : [],
    blacklistMeta: blm && typeof blm === 'object' ? blm : {},
    friendIds: Array.isArray(friends) ? friends : [],
    online: !!row.online,
    lastSeenAt: Number(row.last_seen_ms) || 0,
    privacy: {
      canWrite: settingsRow?.who_can_write || 'all',
      canCall: settingsRow?.who_can_call || 'all',
      canViewProfile: settingsRow?.who_can_see_profile || 'all',
    },
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
  const [rows] = await pool.query(
    'SELECT who_can_write, who_can_call, who_can_see_profile FROM settings WHERE user_id = ? LIMIT 1',
    [userId]
  );
  return rows[0] || null;
}

async function getProfile(userId) {
  const [rows] = await pool.query(
    'SELECT * FROM messenger_profiles WHERE user_id = ? LIMIT 1',
    [userId]
  );
  const row = rows[0];
  const s = await getSettingsRow(userId);
  return rowToServerProfile(userId, row, s);
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
    lastSeenAt: patch.lastSeenAt != null ? patch.lastSeenAt : existing?.lastSeenAt || 0,
  };
  await pool.query(
    `INSERT INTO messenger_profiles (user_id, display_name, avatar_url, username, status_text, blacklist_json, blacklist_meta_json, friend_ids_json, online, last_seen_ms, updated_at)
     VALUES (:userId, :name, :avatar, :username, :status, :bl, :blm, :friends, :online, :lastSeen, :now)
     ON DUPLICATE KEY UPDATE
       display_name = VALUES(display_name),
       avatar_url = VALUES(avatar_url),
       username = VALUES(username),
       status_text = VALUES(status_text),
       blacklist_json = VALUES(blacklist_json),
       blacklist_meta_json = VALUES(blacklist_meta_json),
       friend_ids_json = VALUES(friend_ids_json),
       online = VALUES(online),
       last_seen_ms = VALUES(last_seen_ms),
       updated_at = VALUES(updated_at)`,
    {
      userId,
      name: merged.name,
      avatar: merged.avatar,
      username: merged.username,
      status: merged.statusText,
      bl: JSON.stringify(merged.blacklist),
      blm: JSON.stringify(merged.blacklistMeta),
      friends: JSON.stringify(merged.friendIds),
      online: merged.online ? 1 : 0,
      lastSeen: merged.lastSeenAt,
      now,
    }
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
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE who_can_write = VALUES(who_can_write), who_can_call = VALUES(who_can_call), who_can_see_profile = VALUES(who_can_see_profile)`,
    [userId, w, c, p]
  );
}

async function listAllUserIds() {
  const [rows] = await pool.query('SELECT user_id FROM messenger_profiles');
  return rows.map((r) => r.user_id);
}

async function findDirectChat(a, b) {
  const [u1, u2] = sortedPair(String(a), String(b));
  const [rows] = await pool.query(
    'SELECT id, user1_id, user2_id, last_message, updated_at, meta_json FROM chats WHERE user1_id = ? AND user2_id = ? LIMIT 1',
    [u1, u2]
  );
  if (!rows[0]) return null;
  return rowToChat(rows[0], u1, u2);
}

async function getOrCreateChat(a, b) {
  const [u1, u2] = sortedPair(String(a), String(b));
  const [rows] = await pool.query(
    'SELECT id, user1_id, user2_id, last_message, updated_at, meta_json FROM chats WHERE user1_id = ? AND user2_id = ? LIMIT 1',
    [u1, u2]
  );
  if (rows[0]) {
    return rowToChat(rows[0], u1, u2);
  }
  const id = directChatId(u1, u2);
  const now = Date.now();
  const meta = {
    clearedBy: {},
    removedBy: {},
    blockedBy: {},
    pinnedBy: {},
    archivedBy: {},
    mutedBy: {},
    typingBy: {},
  };
  await pool.query(
    'INSERT INTO chats (id, user1_id, user2_id, last_message, updated_at, meta_json) VALUES (?, ?, ?, NULL, ?, ?)',
    [id, u1, u2, now, JSON.stringify(meta)]
  );
  return { id, members: [u1, u2], meta, lastMessage: null, updatedAt: now };
}

function rowToChat(row, u1, u2) {
  const meta = row.meta_json && typeof row.meta_json === 'object' ? row.meta_json : safeJson(row.meta_json, {});
  const m = {
    clearedBy: meta.clearedBy || {},
    removedBy: meta.removedBy || {},
    blockedBy: meta.blockedBy || {},
    pinnedBy: meta.pinnedBy || {},
    archivedBy: meta.archivedBy || {},
    mutedBy: meta.mutedBy || {},
    typingBy: meta.typingBy || {},
  };
  let lastMessage = null;
  if (row.last_message) {
    try {
      lastMessage = JSON.parse(row.last_message);
    } catch {
      lastMessage = { kind: 'text', text: String(row.last_message), at: row.updated_at || Date.now() };
    }
  }
  return {
    id: row.id,
    kind: 'direct',
    members: [row.user1_id || u1, row.user2_id || u2],
    createdAt: Number(row.updated_at) || Date.now(),
    meta: m,
    lastMessage,
    updatedAt: Number(row.updated_at) || 0,
  };
}

async function updateChatMeta(chatId, meta) {
  await pool.query('UPDATE chats SET meta_json = ?, updated_at = ? WHERE id = ?', [JSON.stringify(meta), Date.now(), chatId]);
}

async function updateLastMessagePreview(chatId, lastMessageObj, updatedAt) {
  const lm = lastMessageObj == null ? null : JSON.stringify(lastMessageObj);
  await pool.query('UPDATE chats SET last_message = ?, updated_at = ? WHERE id = ?', [lm, updatedAt, chatId]);
}

function rowToMessage(row) {
  const dbType = row.type;
  const isVoice = dbType === 'audio';
  const isImage = dbType === 'image';
  const isVideo = dbType === 'video';
  const createdAt = Number(row.created_at) || 0;
  const base = {
    id: row.id,
    chatId: row.chat_id,
    fromId: row.from_id,
    toId: row.to_id,
    createdAt,
    text: row.text || '',
    messageKind: isVoice ? 'voice' : isImage ? 'image' : isVideo ? 'video' : 'text',
    replyTo: row.reply_to || '',
    forwardedFromMessageId: row.forwarded_from || '',
    editedAt: Number(row.edited_at) || 0,
    deletedAt: Number(row.deleted_at) || 0,
  };
  if (isVoice) {
    base.audioBase64 = row.file_url || '';
    base.audioMime = row.audio_mime || 'audio/webm';
    base.durationMs = Number(row.duration_ms) || 0;
  } else {
    base.audioMime = '';
    base.audioBase64 = '';
    base.durationMs = 0;
  }
  if (isImage) {
    base.imageBase64 = row.file_url || '';
  }
  if (isVideo) {
    base.videoBase64 = row.file_url || '';
    base.videoMime = row.audio_mime || 'video/mp4';
  }
  return base;
}

async function insertMessage(msg) {
  const mk = msg.messageKind || msg.kind;
  const isVoice = mk === 'voice' || mk === 'audio';
  const isImage = mk === 'image';
  const isVideo = mk === 'video';
  const type = isVoice ? 'audio' : isImage ? 'image' : isVideo ? 'video' : 'text';
  const fileUrl = isVoice ? msg.audioBase64 || '' : isImage ? msg.imageBase64 || '' : isVideo ? msg.videoBase64 || '' : '';
  const mimeCol = isVoice ? msg.audioMime || '' : isVideo ? msg.videoMime || 'video/mp4' : '';
  const createdAt = Number(msg.createdAt || msg.at || Date.now());
  await pool.query(
    `INSERT INTO messages (id, chat_id, from_id, to_id, text, type, file_url, duration_ms, audio_mime, reply_to, forwarded_from, created_at, edited_at, deleted_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      msg.replyTo || '',
      msg.forwardedFromMessageId || msg.forwardedFrom || '',
      createdAt,
      msg.editedAt || 0,
      msg.deletedAt || 0,
    ]
  );
}

async function getLatestMessageInChatAfter(chatId, clearedAfterTs = 0) {
  const t = Math.max(0, Number(clearedAfterTs) || 0);
  const [rows] = await pool.query(
    'SELECT * FROM messages WHERE chat_id = ? AND deleted_at = 0 AND created_at >= ? ORDER BY created_at DESC LIMIT 1',
    [chatId, t]
  );
  return rows[0] ? rowToMessage(rows[0]) : null;
}

async function listMessagesForChat(chatId, clearedAfterTs = 0, limit = 500) {
  const lim = Math.min(Math.max(Number(limit) || 500, 1), 2000);
  const cleared = Math.max(0, Number(clearedAfterTs) || 0);
  const [rows] = await pool.query(
    `SELECT * FROM messages WHERE chat_id = ? AND deleted_at = 0 AND created_at >= ? ORDER BY created_at ASC LIMIT ${lim}`,
    [chatId, cleared]
  );
  return rows.map(rowToMessage);
}

async function getMessageById(id) {
  const [rows] = await pool.query('SELECT * FROM messages WHERE id = ? LIMIT 1', [id]);
  return rows[0] ? rowToMessage(rows[0]) : null;
}

async function deleteMessageByIdHard(id) {
  await pool.query('DELETE FROM messages WHERE id = ?', [id]);
}

async function updateMessageFields(id, patch) {
  const sets = [];
  const vals = [];
  if (patch.text !== undefined) {
    sets.push('text = ?');
    vals.push(patch.text);
  }
  if (patch.editedAt !== undefined) {
    sets.push('edited_at = ?');
    vals.push(patch.editedAt);
  }
  if (patch.deletedAt !== undefined) {
    sets.push('deleted_at = ?');
    vals.push(patch.deletedAt);
  }
  if (!sets.length) return;
  vals.push(id);
  await pool.query(`UPDATE messages SET ${sets.join(', ')} WHERE id = ?`, vals);
}

async function getChatById(chatId) {
  const [rows] = await pool.query(
    'SELECT id, user1_id, user2_id, last_message, updated_at, meta_json FROM chats WHERE id = ? LIMIT 1',
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
  const [rows] = await pool.query(
    `SELECT id, user1_id, user2_id, last_message, updated_at, meta_json FROM chats
     WHERE user1_id = ? OR user2_id = ? ORDER BY updated_at DESC`,
    [uid, uid]
  );
  return rows.map((r) => rowToChat(r));
}

async function deleteChatRow(chatId) {
  await pool.query('DELETE FROM messages WHERE chat_id = ?', [chatId]);
  await pool.query('DELETE FROM chats WHERE id = ?', [chatId]);
}

async function setUserOnlineFlags(userId, online) {
  const now = Date.now();
  await pool.query(
    'UPDATE messenger_profiles SET online = ?, last_seen_ms = ?, updated_at = ? WHERE user_id = ?',
    [online ? 1 : 0, now, now, userId]
  );
}

module.exports = {
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
};
