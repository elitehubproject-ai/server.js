const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;
const DEFAULT_ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
    { urls: 'stun:stun.cloudflare.com:3478' },
    { urls: 'stun:global.stun.twilio.com:3478' },
    { urls: 'turn:openrelay.metered.ca:80', username: 'openrelayproject', credential: 'openrelayproject' },
    { urls: 'turn:openrelay.metered.ca:443', username: 'openrelayproject', credential: 'openrelayproject' },
    { urls: 'turn:openrelay.metered.ca:443?transport=tcp', username: 'openrelayproject', credential: 'openrelayproject' },
    { urls: 'turns:openrelay.metered.ca:443?transport=tcp', username: 'openrelayproject', credential: 'openrelayproject' }
];

// ВАЖНО: для Render нужно слушать на 0.0.0.0, а не на 127.0.0.1
const wss = new WebSocket.Server({ host: '0.0.0.0', port: PORT, perMessageDeflate: false, maxPayload: 512 * 1024 });
const rooms = new Map();
const userSessions = new Map();
const RECONNECT_GRACE_MS = process.env.RECONNECT_GRACE_MS ? parseInt(process.env.RECONNECT_GRACE_MS, 10) : 15000;
const pendingDisconnects = new Map();
const DATA_DIR = path.join(__dirname, 'data');
const CHATS_DB_PATH = path.join(DATA_DIR, 'chats.json');
const MESSAGES_DB_PATH = path.join(DATA_DIR, 'messages.json');
const USERS_DB_PATH = path.join(DATA_DIR, 'users.json');
const FRIENDS_STORE_PATH = path.join(__dirname, 'friends_store.json');
const messengerMysql = require('./messenger_mysql');
const mysqlBoot = messengerMysql.initMessengerMysql().then((ok) => {
    console.log('[messenger] storage backend:', ok ? 'mysql' : 'json');
    return ok;
}).catch((err) => {
    console.error('[messenger] mysql init failed, using json:', err && err.message);
    return false;
});
/** @type {Map<string, object>} */
const messengerProfileMem = new Map();

function ensureMessengerDataFiles() {
    try {
        if (!fs.existsSync(DATA_DIR)) {
            fs.mkdirSync(DATA_DIR, { recursive: true });
        }
        if (!fs.existsSync(CHATS_DB_PATH)) {
            fs.writeFileSync(CHATS_DB_PATH, JSON.stringify({ chats: [] }, null, 2), 'utf8');
        }
        if (!fs.existsSync(MESSAGES_DB_PATH)) {
            fs.writeFileSync(MESSAGES_DB_PATH, JSON.stringify({ messages: [] }, null, 2), 'utf8');
        }
        if (!fs.existsSync(USERS_DB_PATH)) {
            fs.writeFileSync(USERS_DB_PATH, JSON.stringify({ users: {} }, null, 2), 'utf8');
        }
    } catch (err) {
        console.error('[messenger] ensureMessengerDataFiles', err && err.message);
    }
}

console.log(`✅ WebSocket server running on ws://0.0.0.0:${PORT}`);

// Health check сервер - тоже слушаем на 0.0.0.0
const healthServer = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('WebSocket server is running');
});

// Используем другой порт для health check
const HEALTH_PORT = parseInt(PORT) + 1;
healthServer.listen(HEALTH_PORT, '0.0.0.0', () => console.log(`✅ Health check on http://0.0.0.0:${HEALTH_PORT}`));

function safeSend(ws, payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    try {
        ws.send(JSON.stringify(payload));
    } catch (_) {}
}

function ensureJsonFile(filePath, fallback) {
    try {
        if (!fs.existsSync(DATA_DIR)) {
            fs.mkdirSync(DATA_DIR, { recursive: true });
        }
        if (!fs.existsSync(filePath)) {
            fs.writeFileSync(filePath, JSON.stringify(fallback, null, 2), 'utf8');
            return;
        }
        const raw = fs.readFileSync(filePath, 'utf8');
        const parsed = JSON.parse(raw);
        if (typeof parsed === 'undefined' || parsed === null) {
            fs.writeFileSync(filePath, JSON.stringify(fallback, null, 2), 'utf8');
        }
    } catch (_) {
        try {
            fs.writeFileSync(filePath, JSON.stringify(fallback, null, 2), 'utf8');
        } catch (_) {}
    }
}

function readJson(filePath, fallback) {
    try {
        const raw = fs.readFileSync(filePath, 'utf8');
        const parsed = JSON.parse(raw);
        return parsed ?? fallback;
    } catch (_) {
        return fallback;
    }
}

function writeJson(filePath, value) {
    try {
        fs.writeFileSync(filePath, JSON.stringify(value, null, 2), 'utf8');
        return true;
    } catch (_) {
        return false;
    }
}

ensureMessengerDataFiles();
ensureJsonFile(CHATS_DB_PATH, { chats: [] });
ensureJsonFile(MESSAGES_DB_PATH, { messages: [] });
ensureJsonFile(USERS_DB_PATH, { users: {} });
console.log('[messenger] chats db:', CHATS_DB_PATH);
console.log('[messenger] messages db:', MESSAGES_DB_PATH);
console.log('[messenger] users db:', USERS_DB_PATH);
console.log('[messenger] friends store:', FRIENDS_STORE_PATH);

function normalizeAccountId(value) {
    return typeof value === 'string' ? value.trim().slice(0, 120) : '';
}

function normalizeUsername(value) {
    return typeof value === 'string' ? value.trim().replace(/^@+/, '').slice(0, 64) : '';
}

function normalizeText(value, max = 4000) {
    if (typeof value !== 'string') return '';
    return value.trim().slice(0, max);
}

function createDirectChatId(a, b) {
    const pair = [normalizeAccountId(a), normalizeAccountId(b)].filter(Boolean).sort();
    if (pair.length !== 2) return '';
    return `dm:${pair[0]}::${pair[1]}`;
}

function loadChatsDb() {
    const db = readJson(CHATS_DB_PATH, { chats: [] });
    if (!Array.isArray(db.chats)) db.chats = [];
    return db;
}

function loadMessagesDb() {
    const db = readJson(MESSAGES_DB_PATH, { messages: [] });
    if (!Array.isArray(db.messages)) db.messages = [];
    return db;
}

function saveChatsDb(db) {
    return writeJson(CHATS_DB_PATH, db);
}

function saveMessagesDb(db) {
    return writeJson(MESSAGES_DB_PATH, db);
}

function loadUsersDb() {
    const db = readJson(USERS_DB_PATH, { users: {} });
    if (!db.users || typeof db.users !== 'object' || Array.isArray(db.users)) {
        db.users = {};
    }
    return db;
}

function saveUsersDb(db) {
    return writeJson(USERS_DB_PATH, db);
}

function computeUserInitials(name, accountId) {
    const n = normalizeText(name || '', 120);
    if (n) {
        const parts = n.split(/\s+/).filter(Boolean);
        if (parts.length >= 2) {
            const a = parts[0].charAt(0);
            const b = parts[1].charAt(0);
            return `${a}${b}`.toUpperCase().replace(/[^A-ZА-ЯЁ0-9]/gi, '') || n.slice(0, 2).toUpperCase();
        }
        return n.slice(0, 2).toUpperCase();
    }
    const id = normalizeAccountId(accountId);
    const alnum = id.replace(/[^a-zA-Z0-9]/g, '');
    if (alnum.length >= 2) return alnum.slice(0, 2).toUpperCase();
    if (id.length >= 2) return id.slice(0, 2).toUpperCase();
    return id ? id.charAt(0).toUpperCase() : '·';
}

/**
 * Единый объект пользователя для UI: имя/username/аватар из users.json + chats + friends_store.
 */
function getFormattedUser(userId) {
    const id = normalizeAccountId(userId);
    if (!id) {
        return {
            id: '',
            name: '',
            displayName: '',
            username: '',
            avatar: '',
            initials: '·',
            online: false,
            lastSeenAt: 0,
            statusText: ''
        };
    }
    const usersFile = loadUsersDb();
    const rowFile = usersFile.users[id] || {};
    const memRow = messengerProfileMem.get(id);
    const chatsDb = loadChatsDb();
    const rowChats = memRow
        ? {
              name: memRow.name,
              avatar: memRow.avatar,
              username: memRow.username,
              statusText: memRow.statusText || '',
              online: !!memRow.online,
              lastSeenAt: Number(memRow.lastSeenAt || 0)
          }
        : chatsDb?.users?.[id] || {};
    const friends = getUserProfileFromFriendsStore(id);
    const name = normalizeText(rowFile.name || rowChats.name || (friends && friends.name) || '', 120);
    const username = normalizeUsername(rowFile.username || rowChats.username || (friends && friends.username) || '');
    const avatar = normalizeText(rowFile.avatar || rowChats.avatar || (friends && friends.avatar) || '', 1000);
    const statusText = normalizeText(rowChats.statusText || '', 160);
    const online = !!rowChats.online;
    const lastSeenAt = Number(rowChats.lastSeenAt || 0);
    let displayName = name;
    if (!displayName && username) displayName = `@${username}`;
    if (!displayName) displayName = id;
    const initials = computeUserInitials(name || username, id);
    return {
        id,
        name: name || '',
        displayName,
        username,
        avatar,
        initials,
        online,
        lastSeenAt,
        statusText
    };
}

/** Все сообщения из JSON (после перезапуска сервера остаются на диске). */
function loadMessages() {
    return loadMessagesDb().messages;
}

/**
 * Немедленная запись одного сообщения в messages.json (полный перезапись файла, как у друзей через PHP).
 */
function saveMessage(message) {
    try {
        const db = loadMessagesDb();
        db.messages.push(message);
        const ok = saveMessagesDb(db);
        if (!ok) console.error('[messenger] saveMessage: writeJson failed', MESSAGES_DB_PATH);
        return ok;
    } catch (err) {
        console.error('[messenger] saveMessage error', err && err.message);
        return false;
    }
}

function findChatById(chatId) {
    const chatsDb = loadChatsDb();
    return chatsDb.chats.find((chat) => chat.id === chatId) || null;
}

function getOrCreateDirectChat(firstUserId, secondUserId) {
    const a = normalizeAccountId(firstUserId);
    const b = normalizeAccountId(secondUserId);
    if (!a || !b || a === b) return null;
    const chatId = createDirectChatId(a, b);
    const chatsDb = loadChatsDb();
    let chat = chatsDb.chats.find((item) => item.id === chatId);
    if (!chat) {
        chat = {
            id: chatId,
            kind: 'direct',
            members: [a, b],
            createdAt: Date.now(),
            updatedAt: Date.now(),
            meta: { clearedBy: {}, removedBy: {}, blockedBy: {} }
        };
        chatsDb.chats.push(chat);
        saveChatsDb(chatsDb);
    }
    return chat;
}

function upsertUserPresenceProfileJson(appUserId, profile) {
    const userId = normalizeAccountId(appUserId);
    if (!userId) return;
    const chatsDb = loadChatsDb();
    if (!chatsDb.users || typeof chatsDb.users !== 'object') chatsDb.users = {};
    const prev = chatsDb.users[userId] || {};
    chatsDb.users[userId] = {
        ...prev,
        id: userId,
        name: normalizeText(profile?.name || prev.name || '', 120),
        avatar: normalizeText(profile?.avatar || prev.avatar || '', 1000),
        username: normalizeUsername(profile?.username || prev.username || ''),
        statusText: normalizeText(profile?.statusText || prev.statusText || '', 160),
        online: true,
        lastSeenAt: Date.now(),
        privacy: {
            canWrite: ['all', 'friends', 'nobody'].includes(profile?.privacy?.canWrite) ? profile.privacy.canWrite : (prev.privacy?.canWrite || 'all'),
            canCall: ['all', 'friends', 'nobody'].includes(profile?.privacy?.canCall) ? profile.privacy.canCall : (prev.privacy?.canCall || 'all'),
            canViewProfile: ['all', 'friends', 'nobody'].includes(profile?.privacy?.canViewProfile) ? profile.privacy.canViewProfile : (prev.privacy?.canViewProfile || 'all')
        },
        blacklist: Array.isArray(profile?.blacklist) ? profile.blacklist.map((v) => normalizeAccountId(v)).filter(Boolean) : (Array.isArray(prev.blacklist) ? prev.blacklist : []),
        blacklistMeta: typeof profile?.blacklistMeta === 'object' && profile.blacklistMeta ? profile.blacklistMeta : (typeof prev.blacklistMeta === 'object' && prev.blacklistMeta ? prev.blacklistMeta : {}),
        friendIds: Array.isArray(profile?.friendIds) ? profile.friendIds.map((v) => normalizeAccountId(v)).filter(Boolean) : (Array.isArray(prev.friendIds) ? prev.friendIds : [])
    };
    saveChatsDb(chatsDb);
    const pu = chatsDb.users[userId];
    const udb = loadUsersDb();
    udb.users[userId] = {
        ...(udb.users[userId] || {}),
        id: userId,
        name: (pu && pu.name) || '',
        username: (pu && pu.username) || '',
        avatar: (pu && pu.avatar) || '',
        updatedAt: Date.now()
    };
    saveUsersDb(udb);
}

async function upsertUserPresenceProfileMysql(appUserId, profile) {
    const userId = normalizeAccountId(appUserId);
    if (!userId) return;
    const prev =
        messengerProfileMem.get(userId) ||
        (await messengerMysql.getProfile(userId)) ||
        {
            id: userId,
            name: '',
            avatar: '',
            username: '',
            statusText: '',
            online: false,
            lastSeenAt: 0,
            privacy: { canWrite: 'all', canCall: 'all', canViewProfile: 'all' },
            blacklist: [],
            blacklistMeta: {},
            friendIds: []
        };
    const next = {
        name: profile?.name != null ? normalizeText(String(profile.name), 120) : prev.name,
        avatar: profile?.avatar != null ? normalizeText(String(profile.avatar), 1000) : prev.avatar,
        username: profile?.username != null ? normalizeUsername(profile.username) : prev.username,
        statusText: profile?.statusText != null ? normalizeText(String(profile.statusText), 160) : prev.statusText,
        online: profile?.online !== undefined ? !!profile.online : true,
        lastSeenAt: profile?.lastSeenAt != null ? Number(profile.lastSeenAt) : Date.now(),
        privacy: {
            canWrite:
                profile?.privacy?.canWrite !== undefined && ['all', 'friends', 'nobody'].includes(profile.privacy.canWrite)
                    ? profile.privacy.canWrite
                    : prev.privacy?.canWrite || 'all',
            canCall:
                profile?.privacy?.canCall !== undefined && ['all', 'friends', 'nobody'].includes(profile.privacy.canCall)
                    ? profile.privacy.canCall
                    : prev.privacy?.canCall || 'all',
            canViewProfile:
                profile?.privacy?.canViewProfile !== undefined && ['all', 'friends', 'nobody'].includes(profile.privacy.canViewProfile)
                    ? profile.privacy.canViewProfile
                    : prev.privacy?.canViewProfile || 'all'
        },
        blacklist: Array.isArray(profile?.blacklist)
            ? profile.blacklist.map((v) => normalizeAccountId(v)).filter(Boolean)
            : Array.isArray(prev.blacklist)
              ? prev.blacklist
              : [],
        blacklistMeta:
            typeof profile?.blacklistMeta === 'object' && profile.blacklistMeta
                ? profile.blacklistMeta
                : typeof prev.blacklistMeta === 'object' && prev.blacklistMeta
                  ? prev.blacklistMeta
                  : {},
        friendIds: Array.isArray(profile?.friendIds)
            ? profile.friendIds.map((v) => normalizeAccountId(v)).filter(Boolean)
            : Array.isArray(prev.friendIds)
              ? prev.friendIds
              : []
    };
    const saved = await messengerMysql.upsertProfile(userId, {
        name: next.name,
        avatar: next.avatar,
        username: next.username,
        statusText: next.statusText,
        blacklist: next.blacklist,
        blacklistMeta: next.blacklistMeta,
        friendIds: next.friendIds,
        online: next.online,
        lastSeenAt: next.lastSeenAt,
        privacy: next.privacy
    });
    messengerProfileMem.set(userId, saved);
    const udb = loadUsersDb();
    udb.users[userId] = {
        ...(udb.users[userId] || {}),
        id: userId,
        name: saved.name || '',
        username: saved.username || '',
        avatar: saved.avatar || '',
        updatedAt: Date.now()
    };
    saveUsersDb(udb);
}

async function ensureProfilesLoaded(...ids) {
    const todo = [...new Set(ids.map((x) => normalizeAccountId(x)).filter(Boolean))];
    for (const uid of todo) {
        if (messengerProfileMem.has(uid)) continue;
        if (!messengerMysql.isEnabled()) continue;
        const p = await messengerMysql.getProfile(uid);
        if (p) messengerProfileMem.set(uid, p);
    }
}

function setUserOffline(appUserId) {
    const userId = normalizeAccountId(appUserId);
    if (!userId) return;
    void (async () => {
        try {
            await mysqlBoot;
        } catch (_) {}
        if (messengerMysql.isEnabled()) {
            await messengerMysql.setUserOnlineFlags(userId, false);
            const p = await messengerMysql.getProfile(userId);
            if (p) messengerProfileMem.set(userId, p);
            return;
        }
        const chatsDb = loadChatsDb();
        if (!chatsDb.users || typeof chatsDb.users !== 'object') return;
        if (!chatsDb.users[userId]) return;
        chatsDb.users[userId].online = false;
        chatsDb.users[userId].lastSeenAt = Date.now();
        saveChatsDb(chatsDb);
    })();
}

function getUserProfile(appUserId) {
    const userId = normalizeAccountId(appUserId);
    if (!userId) return null;
    if (messengerProfileMem.has(userId)) return messengerProfileMem.get(userId);
    const chatsDb = loadChatsDb();
    return chatsDb?.users?.[userId] || null;
}

function getUserProfileFromFriendsStore(targetUserId) {
    const targetId = normalizeAccountId(targetUserId);
    if (!targetId) return null;
    const store = readJson(FRIENDS_STORE_PATH, { users: [] });
    const list = Array.isArray(store?.users) ? store.users : [];
    const row = list.find((u) => normalizeAccountId(u?.id) === targetId);
    if (!row) return null;
    return {
        id: targetId,
        name: normalizeText(row.name || '', 120) || targetId,
        avatar: normalizeText(row.avatar || '', 1000),
        username: normalizeUsername(row.username || ''),
        statusText: '',
        online: false,
        lastSeenAt: 0,
        blacklist: [],
        blacklistMeta: {},
        privacy: { canWrite: 'all', canCall: 'all', canViewProfile: 'all' }
    };
}

function areFriendsForMessenger(userA, userB) {
    const a = normalizeAccountId(userA);
    const b = normalizeAccountId(userB);
    if (!a || !b) return false;
    const store = readJson(FRIENDS_STORE_PATH, { friends: [] });
    const list = Array.isArray(store?.friends) ? store.friends : [];
    const key = [a, b].sort().join('::');
    const inStoreFile = list.some((row) => {
        const left = normalizeAccountId(row?.a);
        const right = normalizeAccountId(row?.b);
        if (!left || !right) return false;
        return [left, right].sort().join('::') === key;
    });
    if (inStoreFile) return true;
    const pa = getUserProfile(a);
    const pb = getUserProfile(b);
    const af = Array.isArray(pa?.friendIds) ? pa.friendIds.map((v) => normalizeAccountId(v)).filter(Boolean) : [];
    const bf = Array.isArray(pb?.friendIds) ? pb.friendIds.map((v) => normalizeAccountId(v)).filter(Boolean) : [];
    return af.includes(b) || bf.includes(a);
}

/** Кто может писать кому: ЧС с обеих сторон + политика получателя + друзья из friends_store / friendIds на сервере. */
function directMessageGate(fromUserId, toUserId) {
    const fromId = normalizeAccountId(fromUserId);
    const toId = normalizeAccountId(toUserId);
    if (!fromId || !toId || fromId === toId) return { ok: false, code: 'invalid' };
    const fromP = getUserProfile(fromId);
    const toP = getUserProfile(toId);
    if (fromP && Array.isArray(fromP.blacklist) && fromP.blacklist.includes(toId)) {
        return { ok: false, code: 'blocked' };
    }
    if (toP && Array.isArray(toP.blacklist) && toP.blacklist.includes(fromId)) {
        return { ok: false, code: 'blocked' };
    }
    if (!toP) {
        return { ok: true, code: 'ok' };
    }
    const policy = toP.privacy?.canWrite || 'all';
    if (policy === 'all') return { ok: true, code: 'ok' };
    if (policy === 'nobody') return { ok: false, code: 'policy' };
    if (!areFriendsForMessenger(fromId, toId)) return { ok: false, code: 'friends' };
    return { ok: true, code: 'ok' };
}

function composeHintFromGate(gate) {
    if (!gate || gate.ok) return '';
    if (gate.code === 'blocked') return 'Вы не можете отправить сообщение этому пользователю';
    if (gate.code === 'policy') return 'Пользователь ограничил личные сообщения';
    if (gate.code === 'friends') return 'Пользователь принимает сообщения только от друзей';
    return 'Вы не можете отправить сообщение этому пользователю';
}

function canUserWriteTo(fromUserId, toUserId) {
    return directMessageGate(fromUserId, toUserId).ok;
}

function canUserViewProfile(viewerUserId, targetUserId) {
    const viewerId = normalizeAccountId(viewerUserId);
    const targetId = normalizeAccountId(targetUserId);
    if (!viewerId || !targetId) return false;
    if (viewerId === targetId) return true;
    const target = getUserProfile(targetId);
    if (!target) return true;
    const blocked = Array.isArray(target.blacklist) && target.blacklist.includes(viewerId);
    if (blocked) return false;
    const policy = target.privacy?.canViewProfile || 'all';
    if (policy === 'all') return true;
    if (policy === 'nobody') return false;
    return areFriendsForMessenger(viewerId, targetId);
}

function buildProfileViewFor(viewerUserId, targetUserId) {
    const viewerId = normalizeAccountId(viewerUserId);
    const targetId = normalizeAccountId(targetUserId);
    let target = getUserProfile(targetId);
    if (!target) {
        const fromFriends = getUserProfileFromFriendsStore(targetId);
        if (fromFriends) target = fromFriends;
    }
    if (!target) {
        return {
            ok: true,
            reason: 'minimal',
            profile: {
                id: targetId,
                name: targetId,
                displayName: targetId,
                initials: computeUserInitials('', targetId),
                avatar: '',
                username: '',
                statusText: '',
                online: false,
                lastSeenAt: 0
            }
        };
    }
    const isBlocked = Array.isArray(target.blacklist) && target.blacklist.includes(viewerId);
    if (isBlocked) {
        const fmtB = getFormattedUser(targetId);
        return {
            ok: false,
            reason: 'blocked',
            profile: {
                id: targetId,
                name: fmtB.displayName,
                displayName: fmtB.displayName,
                initials: fmtB.initials,
                avatar: fmtB.avatar,
                username: fmtB.username,
                statusText: target.blacklistMeta?.[viewerId] || 'Вас заблокировал этот аккаунт.',
                online: !!target.online
            }
        };
    }
    if (!canUserViewProfile(viewerId, targetId)) {
        return {
            ok: false,
            reason: 'private',
            profile: {
                id: targetId,
                name: 'Профиль закрыт',
                avatar: '',
                username: '',
                statusText: 'Пользователь закрыл профиль от публичного доступа.',
                online: false
            }
        };
    }
    const fmt = getFormattedUser(targetId);
    return {
        ok: true,
        reason: 'ok',
        profile: {
            id: fmt.id,
            name: fmt.displayName,
            displayName: fmt.displayName,
            initials: fmt.initials,
            avatar: fmt.avatar,
            username: fmt.username,
            statusText: target.statusText || '',
            online: !!target.online,
            lastSeenAt: Number(target.lastSeenAt || 0)
        }
    };
}

function getChatMessagesForUser(chatId, userId, limit = 120) {
    const messagesDb = loadMessagesDb();
    const chatsDb = loadChatsDb();
    const chat = chatsDb.chats.find((item) => item.id === chatId);
    if (!chat) return [];
    const clearedAt = Number(chat.meta?.clearedBy?.[userId] || 0);
    const rows = messagesDb.messages
        .filter((item) => item.chatId === chatId && Number(item.createdAt || 0) >= clearedAt)
        .sort((a, b) => Number(a.createdAt || 0) - Number(b.createdAt || 0));
    return rows.slice(Math.max(0, rows.length - limit));
}

function buildChatListForUser(appUserId) {
    const userId = normalizeAccountId(appUserId);
    const chatsDb = loadChatsDb();
    const messagesDb = loadMessagesDb();
    const all = chatsDb.chats.filter((chat) => Array.isArray(chat.members) && chat.members.includes(userId));
    return all.map((chat) => {
        const peerId = chat.members.find((item) => item !== userId) || userId;
        const fmt = getFormattedUser(peerId);
        const removed = !!chat.meta?.removedBy?.[userId];
        if (removed) return null;
        const clearedAt = Number(chat.meta?.clearedBy?.[userId] || 0);
        const lastMessage = messagesDb.messages
            .filter((item) => item.chatId === chat.id && Number(item.createdAt || 0) >= clearedAt)
            .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))[0] || null;
        return {
            id: chat.id,
            kind: chat.kind || 'direct',
            peer: {
                id: fmt.id,
                name: fmt.name,
                displayName: fmt.displayName,
                initials: fmt.initials,
                avatar: fmt.avatar,
                username: fmt.username,
                statusText: fmt.statusText || '',
                online: !!fmt.online,
                lastSeenAt: Number(fmt.lastSeenAt || 0)
            },
            updatedAt: Number(chat.updatedAt || chat.createdAt || Date.now()),
            lastMessage: lastMessage ? {
                id: lastMessage.id,
                text: lastMessage.text || '',
                fromId: lastMessage.fromId,
                createdAt: lastMessage.createdAt,
                editedAt: Number(lastMessage.editedAt || 0),
                messageKind: lastMessage.messageKind || 'text',
                audioBase64: lastMessage.audioBase64 || ''
            } : null
        };
    }).filter(Boolean).sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
}

async function buildChatListForUserMysql(appUserId) {
    const userId = normalizeAccountId(appUserId);
    const all = await messengerMysql.listChatsForUser(userId);
    const out = [];
    for (const chat of all) {
        const peerId = chat.members.find((item) => item !== userId) || userId;
        await ensureProfilesLoaded(peerId);
        const fmt = getFormattedUser(peerId);
        const removed = !!chat.meta?.removedBy?.[userId];
        if (removed) continue;
        const clearedAt = Number(chat.meta?.clearedBy?.[userId] || 0);
        let lastMessage = null;
        const cached = chat.lastMessage;
        if (cached && cached.id && Number(cached.createdAt || cached.at || 0) >= clearedAt) {
            lastMessage = {
                id: cached.id,
                text: cached.text || '',
                fromId: cached.fromId,
                createdAt: cached.createdAt || cached.at,
                editedAt: Number(cached.editedAt || 0),
                messageKind: cached.messageKind || 'text',
                audioBase64: cached.audioBase64 || ''
            };
        } else {
            const last = await messengerMysql.getLatestMessageInChatAfter(chat.id, clearedAt);
            if (last) {
                lastMessage = {
                    id: last.id,
                    text: last.text || '',
                    fromId: last.fromId,
                    createdAt: last.createdAt,
                    editedAt: last.editedAt,
                    messageKind: last.messageKind,
                    audioBase64: last.audioBase64 || ''
                };
            }
        }
        out.push({
            id: chat.id,
            kind: chat.kind || 'direct',
            peer: {
                id: fmt.id,
                name: fmt.name,
                displayName: fmt.displayName,
                initials: fmt.initials,
                avatar: fmt.avatar,
                username: fmt.username,
                statusText: fmt.statusText || '',
                online: !!fmt.online,
                lastSeenAt: Number(fmt.lastSeenAt || 0)
            },
            updatedAt: Number(chat.updatedAt || chat.createdAt || Date.now()),
            lastMessage
        });
    }
    return out.sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
}

function registerUserSession(appUserId, ws) {
    const id = normalizeAccountId(appUserId);
    if (!id || !ws) return;
    if (!userSessions.has(id)) userSessions.set(id, new Set());
    userSessions.get(id).add(ws);
}

function unregisterUserSession(appUserId, ws) {
    const id = normalizeAccountId(appUserId);
    if (!id || !ws || !userSessions.has(id)) return;
    const set = userSessions.get(id);
    set.delete(ws);
    if (set.size === 0) userSessions.delete(id);
}

function sendToUserSessions(appUserId, payload) {
    const id = normalizeAccountId(appUserId);
    const set = userSessions.get(id);
    if (!set) return;
    set.forEach((sessionWs) => safeSend(sessionWs, payload));
}

function emitMessengerSync(appUserId, reason = 'update') {
    void emitMessengerSyncAsync(appUserId, reason);
}

async function emitMessengerSyncAsync(appUserId, reason = 'update') {
    try {
        await mysqlBoot;
    } catch (_) {}
    const id = normalizeAccountId(appUserId);
    if (!id) return;
    let chats;
    if (messengerMysql.isEnabled()) {
        await ensureProfilesLoaded(id);
        chats = await buildChatListForUserMysql(id);
    } else {
        chats = buildChatListForUser(id);
    }
    sendToUserSessions(id, {
        type: 'messenger-sync',
        reason,
        userId: id,
        chats
    });
}

function sanitizeIceServer(item) {
    if (!item || typeof item !== 'object') return null;
    const urls = item.urls;
    const normalizedUrls = Array.isArray(urls)
        ? urls.filter((u) => typeof u === 'string' && u.trim())
        : typeof urls === 'string' && urls.trim()
            ? urls.trim()
            : null;
    if (!normalizedUrls) return null;
    const out = { urls: normalizedUrls };
    if (typeof item.username === 'string' && item.username) out.username = item.username;
    if (typeof item.credential === 'string' && item.credential) out.credential = item.credential;
    return out;
}

function buildIceServers() {
    const merged = [];
    const add = (item) => {
        const server = sanitizeIceServer(item);
        if (!server) return;
        const key = JSON.stringify(server);
        if (!merged.some((entry) => JSON.stringify(entry) === key)) {
            merged.push(server);
        }
    };
    DEFAULT_ICE_SERVERS.forEach(add);

    if (process.env.WEBRTC_ICE_SERVERS_JSON) {
        try {
            const parsed = JSON.parse(process.env.WEBRTC_ICE_SERVERS_JSON);
            if (Array.isArray(parsed)) parsed.forEach(add);
        } catch (_) {}
    }

    const turnUser = process.env.TURN_USERNAME || '';
    const turnCredential = process.env.TURN_CREDENTIAL || '';
    const turnUrls = String(process.env.TURN_URLS || '')
        .split(',')
        .map((u) => u.trim())
        .filter(Boolean);
    if (turnUrls.length && turnUser && turnCredential) {
        add({ urls: turnUrls, username: turnUser, credential: turnCredential });
    }
    return merged;
}

const ACTIVE_ICE_SERVERS = buildIceServers();

function serializeParticipant(id, participant) {
    return {
        id,
        userName: participant.userName,
        userAvatar: participant.userAvatar || '',
        appUserId: participant.appUserId || '',
        video: !!participant.video,
        audio: !!participant.audio,
        screen: !!participant.screen,
        speaking: !!participant.speaking,
        isAdmin: !!participant.isAdmin,
        cameraFacingMode: participant.cameraFacingMode === 'environment' ? 'environment' : 'user'
    };
}

function serializeJoinRequest(request) {
    return {
        id: request.id,
        userName: request.userName,
        userAvatar: request.userAvatar || '',
        requestedAt: request.requestedAt || Date.now()
    };
}

function ensureOwnerAdmin(room) {
    room.participants.forEach((participant, id) => {
        if (id === room.ownerId) {
            participant.isAdmin = true;
        }
    });
}

function isModerator(room, participantId) {
    if (!room || !participantId) return false;
    if (room.ownerId === participantId) return true;
    const participant = room.participants.get(participantId);
    return !!participant?.isAdmin;
}

function broadcastJoinRequestToModerators(room, payload) {
    room.participants.forEach((participant, id) => {
        if (!isModerator(room, id)) return;
        safeSend(participant.ws, payload);
    });
}

function broadcastRoomState(room) {
    const participants = Array.from(room.participants.entries()).map(([id, participant]) => serializeParticipant(id, participant));
    room.participants.forEach((participant, id) => {
        const canModerate = isModerator(room, id);
        const pendingJoinRequests = canModerate
            ? Array.from(room.joinRequests.values()).map(serializeJoinRequest)
            : [];
        safeSend(participant.ws, {
            type: 'room-state',
            roomId: room.id,
            myId: id,
            ownerId: room.ownerId,
            participants,
            watchParty: room.watchParty || null,
            isPrivate: !!room.isPrivate,
            pendingJoinRequests,
            iceServers: ACTIVE_ICE_SERVERS
        });
    });
}

function findParticipantIdByReconnectKey(room, reconnectKey) {
    if (!room || !reconnectKey) return null;
    for (const [id, participant] of room.participants.entries()) {
        if (participant?.reconnectKey === reconnectKey) {
            return id;
        }
    }
    return null;
}

function isInvitedFriendForRoom(room, appUserId) {
    if (!room || !room.isFriendCall) return false;
    const invited = typeof room.friendTargetAppUserId === 'string' ? room.friendTargetAppUserId.trim() : '';
    const current = typeof appUserId === 'string' ? appUserId.trim() : '';
    if (!invited || !current) return false;
    return invited === current;
}

function assignOwner(room, preferredOwnerId = null) {
    const prevOwnerId = room.ownerId || null;
    if (preferredOwnerId && room.participants.has(preferredOwnerId)) {
        room.ownerId = preferredOwnerId;
    } else {
        const first = room.participants.keys().next();
        room.ownerId = first.done ? null : first.value;
    }
    ensureOwnerAdmin(room);
    if (prevOwnerId !== room.ownerId && room.ownerId) {
        room.participants.forEach((participant) => {
            safeSend(participant.ws, {
                type: 'owner-changed',
                ownerId: room.ownerId,
                previousOwnerId: prevOwnerId
            });
        });
    }
}

function closeRoom(room, closedById = null, closedByName = '') {
    if (!room) return;
    const roomId = room.id;
    clearRoomPendingDisconnects(roomId);
    const participants = Array.from(room.participants.values());
    const pending = Array.from(room.joinRequests.values());
    rooms.delete(roomId);
    participants.forEach((participant) => {
        safeSend(participant.ws, {
            type: 'room-closed',
            roomId,
            byId: closedById,
            byName: closedByName || ''
        });
    });
    pending.forEach((request) => {
        safeSend(request.ws, {
            type: 'room-closed',
            roomId,
            byId: closedById,
            byName: closedByName || ''
        });
    });
    participants.forEach((participant) => {
        try { participant.ws.close(); } catch (_) {}
    });
    pending.forEach((request) => {
        try { request.ws.close(); } catch (_) {}
    });
}

function getPendingDisconnectKey(roomId, participantId) {
    return `${roomId}::${participantId}`;
}

function clearPendingDisconnect(roomId, participantId) {
    if (!roomId || !participantId) return;
    const key = getPendingDisconnectKey(roomId, participantId);
    const timerId = pendingDisconnects.get(key);
    if (!timerId) return;
    clearTimeout(timerId);
    pendingDisconnects.delete(key);
}

function clearRoomPendingDisconnects(roomId) {
    if (!roomId) return;
    const prefix = `${roomId}::`;
    Array.from(pendingDisconnects.entries()).forEach(([key, timerId]) => {
        if (!key.startsWith(prefix)) return;
        clearTimeout(timerId);
        pendingDisconnects.delete(key);
    });
}

function finalizeParticipantDisconnect(clientId, roomId) {
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    const participant = room.participants.get(clientId);
    if (!participant) return;

    clearPendingDisconnect(roomId, clientId);
    console.log(`❌ User left: ${participant.userName} from room ${roomId}`);
    
    room.participants.delete(clientId);
    const shouldStopWatch = room.watchParty && room.watchParty.ownerId === clientId;
    if (shouldStopWatch) {
        room.watchParty = null;
    }
    if (room.isFriendCall) {
        closeRoom(room, clientId, participant.userName || '');
        return;
    }
    
    if (room.participants.size === 0) {
        room.joinRequests.forEach((request) => {
            safeSend(request.ws, {
                type: 'room-closed',
                roomId
            });
            try { request.ws.close(); } catch (_) {}
        });
        room.joinRequests.clear();
        clearRoomPendingDisconnects(roomId);
        rooms.delete(roomId);
        console.log(`🏠 Room closed: ${roomId}`);
    } else {
        const ownerLeft = room.ownerId === clientId || !room.participants.has(room.ownerId);
        if (ownerLeft) {
            assignOwner(room);
        } else {
            ensureOwnerAdmin(room);
        }

        room.participants.forEach((p) => {
            safeSend(p.ws, { 
                type: 'guest-left', 
                from: participant.userName,
                fromId: clientId,
                ownerId: room.ownerId
            });
            if (shouldStopWatch) {
                safeSend(p.ws, {
                    type: 'watch-stopped',
                    previousWatch: null,
                    from: participant.userName,
                    fromId: clientId,
                    ownerId: room.ownerId
                });
            }
        });
        broadcastRoomState(room);
    }
}

wss.on('connection', (ws) => {
    const clientId = uuidv4();
    console.log(`📱 Client connected: ${clientId.substring(0, 8)}`);

    let currentRoom = null;
    let userName = '';
    let currentAppUserId = '';

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const senderId = ws.__participantId || clientId;

            switch (data.type) {
                case 'ping':
                    safeSend(ws, { type: 'pong', ts: Date.now() });
                    break;
                case 'messenger-register':
                    {
                        const accountId = normalizeAccountId(data.appUserId);
                        if (!accountId) {
                            safeSend(ws, { type: 'error', message: 'appUserId required for messenger-register' });
                            return;
                        }
                        currentAppUserId = accountId;
                        ws.__appUserId = accountId;
                        registerUserSession(accountId, ws);
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            const patch = {
                                name: data.userName || data.name || '',
                                avatar: data.userAvatar || data.avatar || '',
                                username: data.username || '',
                                statusText: data.statusText || '',
                                privacy: data.privacy || null,
                                blacklist: data.blacklist || null
                            };
                            if (messengerMysql.isEnabled()) {
                                await upsertUserPresenceProfileMysql(accountId, patch);
                            } else {
                                upsertUserPresenceProfileJson(accountId, patch);
                            }
                            emitMessengerSync(accountId, 'init');
                        })();
                    }
                    break;
                case 'messenger-sync':
                    if (!currentAppUserId) return;
                    emitMessengerSync(currentAppUserId, 'manual-sync');
                    break;
                case 'messenger-open-chat':
                    {
                        if (!currentAppUserId) return;
                        const withUserId = normalizeAccountId(data.withUserId);
                        if (!withUserId || withUserId === currentAppUserId) return;
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            await ensureProfilesLoaded(currentAppUserId, withUserId);
                            let chat;
                            if (messengerMysql.isEnabled()) {
                                chat = await messengerMysql.getOrCreateChat(currentAppUserId, withUserId);
                                const meta = {
                                    ...chat.meta,
                                    removedBy: { ...(chat.meta?.removedBy || {}), [currentAppUserId]: false }
                                };
                                await messengerMysql.updateChatMeta(chat.id, meta);
                                chat.meta = meta;
                            } else {
                                chat = getOrCreateDirectChat(currentAppUserId, withUserId);
                                if (!chat) return;
                                const chatsDb = loadChatsDb();
                                const chatIndex = chatsDb.chats.findIndex((item) => item.id === chat.id);
                                if (chatIndex >= 0) {
                                    if (!chatsDb.chats[chatIndex].meta) {
                                        chatsDb.chats[chatIndex].meta = { clearedBy: {}, removedBy: {}, blockedBy: {} };
                                    }
                                    chatsDb.chats[chatIndex].meta.removedBy[currentAppUserId] = false;
                                    saveChatsDb(chatsDb);
                                }
                            }
                            if (!chat) return;
                            const clearedAt = Number(chat.meta?.clearedBy?.[currentAppUserId] || 0);
                            const messages = messengerMysql.isEnabled()
                                ? await messengerMysql.listMessagesForChat(chat.id, clearedAt, 250)
                                : getChatMessagesForUser(chat.id, currentAppUserId, 250);
                            const gate = directMessageGate(currentAppUserId, withUserId);
                            safeSend(ws, {
                                type: 'messenger-chat-history',
                                chatId: chat.id,
                                withUserId,
                                messages,
                                composeBlocked: !gate.ok,
                                composeHint: composeHintFromGate(gate)
                            });
                            emitMessengerSync(currentAppUserId, 'chat-opened');
                        })();
                    }
                    break;
                case 'messenger-friends-sync':
                    if (!currentAppUserId) return;
                    {
                        const ids = Array.isArray(data.friendIds) ? data.friendIds.map((v) => normalizeAccountId(v)).filter(Boolean) : [];
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            if (messengerMysql.isEnabled()) {
                                await upsertUserPresenceProfileMysql(currentAppUserId, { friendIds: ids });
                            } else {
                                const cdb = loadChatsDb();
                                if (!cdb.users || typeof cdb.users !== 'object') cdb.users = {};
                                const uid = currentAppUserId;
                                const prevRow = cdb.users[uid] || { id: uid };
                                cdb.users[uid] = { ...prevRow, id: uid, friendIds: ids };
                                saveChatsDb(cdb);
                            }
                        })();
                    }
                    break;
                case 'messenger-send':
                case 'sendMessage':
                    {
                        if (!currentAppUserId) return;
                        const toUserId = normalizeAccountId(data.toUserId || data.to);
                        if (!toUserId || toUserId === currentAppUserId) return;
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            await ensureProfilesLoaded(currentAppUserId, toUserId);
                            const gate = directMessageGate(currentAppUserId, toUserId);
                            if (!gate.ok) {
                                safeSend(ws, {
                                    type: 'messenger-error',
                                    code: 'write_forbidden',
                                    message: composeHintFromGate(gate)
                                });
                                return;
                            }
                            let chat;
                            if (messengerMysql.isEnabled()) {
                                chat = await messengerMysql.getOrCreateChat(currentAppUserId, toUserId);
                            } else {
                                chat = getOrCreateDirectChat(currentAppUserId, toUserId);
                            }
                            if (!chat) return;
                            const text = normalizeText(data.text, 4000);
                            const audioRaw = typeof data.audioBase64 === 'string' ? data.audioBase64 : '';
                            const audioBase64 = audioRaw.replace(/[^a-zA-Z0-9+/=]/g, '').slice(0, 720000);
                            const isVoice = audioBase64.length > 32 && (data.messageKind === 'voice' || !!data.audioBase64);
                            if (!text && !isVoice) return;
                            const mimeRaw = typeof data.mimeType === 'string' ? data.mimeType : 'audio/webm';
                            const audioMime = /^audio\/(webm|ogg|mp4|mpeg|wav)$/i.test(mimeRaw) ? mimeRaw.slice(0, 80) : 'audio/webm';
                            const message = {
                                id: `msg_${uuidv4()}`,
                                chatId: chat.id,
                                fromId: currentAppUserId,
                                toId: toUserId,
                                text: text || (isVoice ? 'Голосовое сообщение' : ''),
                                messageKind: isVoice ? 'voice' : 'text',
                                audioMime: isVoice ? audioMime : '',
                                audioBase64: isVoice ? audioBase64 : '',
                                durationMs: isVoice ? Math.min(600000, Math.max(0, Number(data.durationMs || 0))) : 0,
                                createdAt: Date.now(),
                                editedAt: 0,
                                deletedAt: 0,
                                replyTo: normalizeText(data.replyTo || '', 64),
                                forwardedFromMessageId: normalizeText(data.forwardedFromMessageId || '', 64)
                            };
                            if (messengerMysql.isEnabled()) {
                                try {
                                    await messengerMysql.insertMessage(message);
                                    const meta = {
                                        ...chat.meta,
                                        removedBy: {
                                            ...(chat.meta?.removedBy || {}),
                                            [currentAppUserId]: false,
                                            [toUserId]: false
                                        }
                                    };
                                    await messengerMysql.updateChatMeta(chat.id, meta);
                                    const preview = {
                                        id: message.id,
                                        text: message.text,
                                        fromId: message.fromId,
                                        createdAt: message.createdAt,
                                        editedAt: 0,
                                        messageKind: message.messageKind,
                                        audioBase64: ''
                                    };
                                    await messengerMysql.updateLastMessagePreview(chat.id, preview, Date.now());
                                } catch (err) {
                                    console.error('[messenger] mysql insertMessage', err && err.message);
                                    safeSend(ws, { type: 'messenger-error', code: 'save_failed', message: 'Не удалось сохранить сообщение' });
                                    return;
                                }
                            } else {
                                if (!saveMessage(message)) {
                                    safeSend(ws, { type: 'messenger-error', code: 'save_failed', message: 'Не удалось сохранить сообщение' });
                                    return;
                                }
                                const chatsDb = loadChatsDb();
                                const chatIndex = chatsDb.chats.findIndex((item) => item.id === chat.id);
                                if (chatIndex >= 0) {
                                    chatsDb.chats[chatIndex].updatedAt = Date.now();
                                    if (!chatsDb.chats[chatIndex].meta) {
                                        chatsDb.chats[chatIndex].meta = { clearedBy: {}, removedBy: {}, blockedBy: {} };
                                    }
                                    chatsDb.chats[chatIndex].meta.removedBy[currentAppUserId] = false;
                                    chatsDb.chats[chatIndex].meta.removedBy[toUserId] = false;
                                }
                                saveChatsDb(chatsDb);
                            }
                            sendToUserSessions(currentAppUserId, { type: 'messenger-message', chatId: chat.id, message });
                            sendToUserSessions(toUserId, { type: 'messenger-message', chatId: chat.id, message });
                            emitMessengerSync(currentAppUserId, 'new-message');
                            emitMessengerSync(toUserId, 'new-message');
                        })();
                    }
                    break;
                case 'messenger-typing':
                    {
                        if (!currentAppUserId) return;
                        const toUserId = normalizeAccountId(data.toUserId);
                        if (!toUserId || toUserId === currentAppUserId) return;
                        sendToUserSessions(toUserId, {
                            type: 'messenger-typing',
                            fromUserId: currentAppUserId,
                            chatId: createDirectChatId(currentAppUserId, toUserId),
                            isTyping: !!data.isTyping,
                            ts: Date.now()
                        });
                    }
                    break;
                case 'messenger-edit':
                    {
                        if (!currentAppUserId) return;
                        const messageId = normalizeText(data.messageId || '', 80);
                        const nextText = normalizeText(data.text, 4000);
                        if (!messageId || !nextText) return;
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            let row;
                            if (messengerMysql.isEnabled()) {
                                row = await messengerMysql.getMessageById(messageId);
                                if (!row || row.fromId !== currentAppUserId || row.deletedAt) return;
                                await messengerMysql.updateMessageFields(messageId, { text: nextText, editedAt: Date.now() });
                                row = { ...row, text: nextText, editedAt: Date.now() };
                            } else {
                                const messagesDb = loadMessagesDb();
                                row = messagesDb.messages.find((item) => item.id === messageId);
                                if (!row || row.fromId !== currentAppUserId || row.deletedAt) return;
                                row.text = nextText;
                                row.editedAt = Date.now();
                                saveMessagesDb(messagesDb);
                            }
                            const chat = messengerMysql.isEnabled()
                                ? await messengerMysql.getChatById(row.chatId)
                                : findChatById(row.chatId);
                            if (!chat) return;
                            const peerId = chat.members.find((item) => item !== currentAppUserId) || '';
                            const payload = { type: 'messenger-message-updated', chatId: row.chatId, message: row };
                            sendToUserSessions(currentAppUserId, payload);
                            if (peerId) sendToUserSessions(peerId, payload);
                        })();
                    }
                    break;
                case 'messenger-delete':
                    {
                        if (!currentAppUserId) return;
                        const messageId = normalizeText(data.messageId || '', 80);
                        if (!messageId) return;
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            let row;
                            if (messengerMysql.isEnabled()) {
                                row = await messengerMysql.getMessageById(messageId);
                                if (!row || row.fromId !== currentAppUserId || row.deletedAt) return;
                                await messengerMysql.updateMessageFields(messageId, { deletedAt: Date.now() });
                            } else {
                                const messagesDb = loadMessagesDb();
                                row = messagesDb.messages.find((item) => item.id === messageId);
                                if (!row || row.fromId !== currentAppUserId || row.deletedAt) return;
                                row.deletedAt = Date.now();
                                saveMessagesDb(messagesDb);
                            }
                            const chat = messengerMysql.isEnabled()
                                ? await messengerMysql.getChatById(row.chatId)
                                : findChatById(row.chatId);
                            if (!chat) return;
                            const peerId = chat.members.find((item) => item !== currentAppUserId) || '';
                            const payload = { type: 'messenger-message-deleted', chatId: row.chatId, messageId };
                            sendToUserSessions(currentAppUserId, payload);
                            if (peerId) sendToUserSessions(peerId, payload);
                        })();
                    }
                    break;
                case 'messenger-clear-chat':
                    {
                        if (!currentAppUserId) return;
                        const chatId = normalizeText(data.chatId || '', 220);
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            if (messengerMysql.isEnabled()) {
                                const ch = await messengerMysql.getChatById(chatId);
                                if (!ch || !Array.isArray(ch.members) || !ch.members.includes(currentAppUserId)) return;
                                const meta = {
                                    ...ch.meta,
                                    clearedBy: { ...(ch.meta?.clearedBy || {}), [currentAppUserId]: Date.now() }
                                };
                                await messengerMysql.updateChatMeta(chatId, meta);
                            } else {
                                const chatsDb = loadChatsDb();
                                const index = chatsDb.chats.findIndex((item) => item.id === chatId);
                                if (index < 0) return;
                                if (!Array.isArray(chatsDb.chats[index].members) || !chatsDb.chats[index].members.includes(currentAppUserId)) return;
                                if (!chatsDb.chats[index].meta) chatsDb.chats[index].meta = { clearedBy: {}, removedBy: {}, blockedBy: {} };
                                chatsDb.chats[index].meta.clearedBy[currentAppUserId] = Date.now();
                                chatsDb.chats[index].updatedAt = Date.now();
                                saveChatsDb(chatsDb);
                            }
                            emitMessengerSync(currentAppUserId, 'chat-cleared');
                        })();
                    }
                    break;
                case 'messenger-delete-chat':
                    {
                        if (!currentAppUserId) return;
                        const chatId = normalizeText(data.chatId || '', 220);
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            if (messengerMysql.isEnabled()) {
                                const ch = await messengerMysql.getChatById(chatId);
                                if (!ch || !Array.isArray(ch.members) || !ch.members.includes(currentAppUserId)) return;
                                const meta = {
                                    ...ch.meta,
                                    removedBy: { ...(ch.meta?.removedBy || {}), [currentAppUserId]: true }
                                };
                                await messengerMysql.updateChatMeta(chatId, meta);
                            } else {
                                const chatsDb = loadChatsDb();
                                const index = chatsDb.chats.findIndex((item) => item.id === chatId);
                                if (index < 0) return;
                                if (!Array.isArray(chatsDb.chats[index].members) || !chatsDb.chats[index].members.includes(currentAppUserId)) return;
                                if (!chatsDb.chats[index].meta) chatsDb.chats[index].meta = { clearedBy: {}, removedBy: {}, blockedBy: {} };
                                chatsDb.chats[index].meta.removedBy[currentAppUserId] = true;
                                saveChatsDb(chatsDb);
                            }
                            emitMessengerSync(currentAppUserId, 'chat-removed');
                        })();
                    }
                    break;
                case 'messenger-block-user':
                    {
                        if (!currentAppUserId) return;
                        const targetId = normalizeAccountId(data.targetUserId);
                        if (!targetId || targetId === currentAppUserId) return;
                        void (async () => {
                            try {
                                await mysqlBoot;
                            } catch (_) {}
                            await ensureProfilesLoaded(currentAppUserId);
                            const profile = getUserProfile(currentAppUserId) || { blacklist: [] };
                            const set = new Set(Array.isArray(profile.blacklist) ? profile.blacklist : []);
                            const blacklistMeta =
                                typeof profile.blacklistMeta === 'object' && profile.blacklistMeta ? profile.blacklistMeta : {};
                            if (!!data.blocked) {
                                set.add(targetId);
                                const note = normalizeText(data.comment || '', 180);
                                if (note) blacklistMeta[targetId] = note;
                            } else {
                                set.delete(targetId);
                                delete blacklistMeta[targetId];
                            }
                            if (messengerMysql.isEnabled()) {
                                await upsertUserPresenceProfileMysql(currentAppUserId, {
                                    ...profile,
                                    blacklist: Array.from(set),
                                    blacklistMeta
                                });
                            } else {
                                upsertUserPresenceProfileJson(currentAppUserId, {
                                    ...profile,
                                    blacklist: Array.from(set),
                                    blacklistMeta
                                });
                            }
                            emitMessengerSync(currentAppUserId, 'blacklist-updated');
                        })();
                    }
                    break;
                case 'messenger-get-profile':
                    {
                        if (!currentAppUserId) return;
                        const targetId = normalizeAccountId(data.targetUserId);
                        if (!targetId) return;
                        safeSend(ws, {
                            type: 'messenger-profile',
                            targetUserId: targetId,
                            view: buildProfileViewFor(currentAppUserId, targetId)
                        });
                    }
                    break;
                case 'messenger-update-profile':
                    if (!currentAppUserId) return;
                    void (async () => {
                        try {
                            await mysqlBoot;
                        } catch (_) {}
                        const patch = {
                            name: data.name,
                            avatar: data.avatar,
                            username: data.username,
                            statusText: data.statusText
                        };
                        if (messengerMysql.isEnabled()) {
                            await upsertUserPresenceProfileMysql(currentAppUserId, patch);
                        } else {
                            upsertUserPresenceProfileJson(currentAppUserId, patch);
                        }
                        emitMessengerSync(currentAppUserId, 'profile-updated');
                    })();
                    break;
                case 'messenger-update-privacy':
                    if (!currentAppUserId) return;
                    void (async () => {
                        try {
                            await mysqlBoot;
                        } catch (_) {}
                        const patch = {
                            privacy: {
                                canWrite: data.canWrite,
                                canCall: data.canCall,
                                canViewProfile: data.canViewProfile
                            }
                        };
                        if (messengerMysql.isEnabled()) {
                            await upsertUserPresenceProfileMysql(currentAppUserId, patch);
                        } else {
                            upsertUserPresenceProfileJson(currentAppUserId, patch);
                        }
                        emitMessengerSync(currentAppUserId, 'privacy-updated');
                    })();
                    break;

                case 'create':
                case 'join':
                    currentRoom = data.roomId;
                    userName = data.userName;
                    const userAvatar = data.userAvatar || '';
                    const appUserId = typeof data.appUserId === 'string' ? data.appUserId.trim().slice(0, 80) : '';
                    const isCreating = data.type === 'create';
                    const privateRoomRequested = !!data.privateRoom;
                    const friendCallModeRequested = !!data.friendCallMode;
                    const friendTargetAppUserId = typeof data.friendTargetAppUserId === 'string' ? data.friendTargetAppUserId.trim().slice(0, 80) : '';
                    const reconnectKey = typeof data.reconnectKey === 'string' ? data.reconnectKey.trim().slice(0, 160) : '';

                    if (!currentRoom || typeof currentRoom !== 'string') {
                        safeSend(ws, { type: 'error', message: 'Некорректный идентификатор комнаты' });
                        return;
                    }

                    if (!rooms.has(currentRoom)) {
                        if (!isCreating) {
                            safeSend(ws, { type: 'error', message: 'Такой комнаты не существует' });
                            return;
                        }
                        rooms.set(currentRoom, {
                            id: currentRoom,
                            participants: new Map(),
                            joinRequests: new Map(),
                            ownerId: null,
                            watchParty: null,
                            isPrivate: privateRoomRequested,
                            isFriendCall: friendCallModeRequested,
                            friendTargetAppUserId
                        });
                    }
                    const room = rooms.get(currentRoom);
                    const reconnectTargetId = findParticipantIdByReconnectKey(room, reconnectKey);
                    if (isCreating && room.participants.size > 0 && !reconnectTargetId) {
                        safeSend(ws, { type: 'error', message: 'Комната уже используется' });
                        return;
                    }
                    if (!isCreating && room.isPrivate && room.participants.size > 0 && !reconnectTargetId) {
                        const invitedFriend = isInvitedFriendForRoom(room, appUserId);
                        if (room.isFriendCall && !invitedFriend) {
                            safeSend(ws, { type: 'error', message: 'Комната закрыта' });
                            return;
                        }
                        const shouldQueueJoinRequest = !room.isFriendCall || !invitedFriend;
                        if (shouldQueueJoinRequest) {
                            const request = {
                                id: clientId,
                                ws,
                                userName,
                                userAvatar,
                                appUserId,
                                reconnectKey,
                                requestedAt: Date.now()
                            };
                            room.joinRequests.set(clientId, request);
                            safeSend(ws, {
                                type: 'join-pending',
                                roomId: currentRoom
                            });
                            broadcastJoinRequestToModerators(room, {
                                type: 'join-request',
                                roomId: currentRoom,
                                request: serializeJoinRequest(request)
                            });
                            return;
                        }
                    }

                    if (reconnectTargetId && room.participants.has(reconnectTargetId)) {
                        const existing = room.participants.get(reconnectTargetId);
                        const oldWs = existing?.ws;
                        clearPendingDisconnect(currentRoom, reconnectTargetId);
                        const participantInfo = {
                            ...existing,
                            ws,
                            userName,
                            userAvatar,
                            appUserId: appUserId || existing?.appUserId || '',
                            reconnectKey: reconnectKey || existing?.reconnectKey || ''
                        };
                        room.participants.set(reconnectTargetId, participantInfo);
                        ws.__participantId = reconnectTargetId;
                        room.joinRequests.delete(clientId);
                        room.joinRequests.delete(reconnectTargetId);
                        ensureOwnerAdmin(room);
                        safeSend(ws, {
                            type: isCreating ? 'created' : 'joined',
                            roomId: currentRoom,
                            myId: reconnectTargetId,
                            ownerId: room.ownerId,
                            iceServers: ACTIVE_ICE_SERVERS
                        });
                        broadcastRoomState(room);
                        if (oldWs && oldWs !== ws) {
                            oldWs.__superseded = true;
                            try { oldWs.close(); } catch (_) {}
                        }
                        console.log(`🔁 User ${userName} reconnected in room: ${currentRoom}`);
                        break;
                    }

                    const participantInfo = {
                        ws,
                        userName,
                        userAvatar,
                        appUserId,
                        video: false,
                        audio: true,
                        screen: false,
                        speaking: false,
                        isAdmin: false,
                        cameraFacingMode: 'user',
                        reconnectKey
                    };

                    room.participants.set(clientId, participantInfo);
                    ws.__participantId = clientId;
                    room.joinRequests.delete(clientId);
                    if (!room.ownerId) {
                        room.ownerId = clientId;
                    }
                    ensureOwnerAdmin(room);

                    room.participants.forEach((participant, id) => {
                        if (id === clientId) return;
                        safeSend(participant.ws, {
                            type: 'guest-joined',
                            guest: serializeParticipant(clientId, participantInfo),
                            ownerId: room.ownerId,
                            iceServers: ACTIVE_ICE_SERVERS
                        });
                    });

                    safeSend(ws, {
                        type: isCreating ? 'created' : 'joined',
                        roomId: currentRoom,
                        myId: clientId,
                        ownerId: room.ownerId,
                        iceServers: ACTIVE_ICE_SERVERS
                    });

                    broadcastRoomState(room);

                    console.log(`🏠 User ${userName} ${isCreating ? 'created' : 'joined'} room: ${currentRoom}`);
                    break;

                case 'signal':
                case 'screen-signal':
                case 'video-signal':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        
                        const targetId = data.targetId || data.target;
                        if (targetId) {
                            const target = room.participants.get(targetId);
                            if (target) {
                                safeSend(target.ws, { 
                                    ...data, 
                                    from: userName, 
                                    fromId: senderId 
                                });
                            }
                        } else {
                            room.participants.forEach((p, id) => {
                                if (id !== senderId) {
                                    safeSend(p.ws, { 
                                        ...data, 
                                        from: userName, 
                                        fromId: senderId 
                                    });
                                }
                            });
                        }
                    }
                    break;

                case 'start-screen':
                case 'stop-screen':
                case 'toggle-video':
                case 'toggle-audio':
                case 'speaking':
                case 'camera-facing':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        const p = room.participants.get(senderId);
                        if (!p) return;

                        if (data.type === 'start-screen') p.screen = true;
                        if (data.type === 'stop-screen') p.screen = false;
                        if (data.type === 'toggle-video') p.video = data.enabled;
                        if (data.type === 'toggle-audio') p.audio = data.enabled;
                        if (data.type === 'speaking') p.speaking = !!data.isSpeaking;
                        if (data.type === 'camera-facing') p.cameraFacingMode = data.mode === 'environment' ? 'environment' : 'user';

                        room.participants.forEach((participant, id) => {
                            if (id !== senderId) {
                                safeSend(participant.ws, {
                                    ...data,
                                    from: userName,
                                    fromId: senderId
                                });
                            }
                        });

                        room.participants.forEach((participant, id) => {
                            if (id === clientId) return;
                            safeSend(participant.ws, {
                                type: 'participant-updated',
                                participantId: senderId,
                                ownerId: room.ownerId,
                                changes: {
                                    video: p.video,
                                    audio: p.audio,
                                    screen: p.screen,
                                    speaking: !!p.speaking,
                                    cameraFacingMode: p.cameraFacingMode === 'environment' ? 'environment' : 'user'
                                }
                            });
                        });
                    }
                    break;

                case 'request-video':
                case 'request-audio':
                case 'friend-request':
                case 'force-video-off':
                case 'force-audio-off':
                case 'make-admin':
                case 'remove-admin':
                case 'kick':
                case 'approve-join-request':
                case 'reject-join-request':
                case 'set-room-private':
                case 'close-room':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        const sender = room.participants.get(senderId);
                        if (!sender) return;
                        const senderIsOwner = room.ownerId === senderId;
                        const senderIsAdmin = !!sender.isAdmin;

                        if (data.type === 'set-room-private') {
                            if (!senderIsOwner && !senderIsAdmin) return;
                            room.isPrivate = !!data.enabled;
                            room.participants.forEach((participant) => {
                                safeSend(participant.ws, {
                                    type: 'room-privacy-updated',
                                    enabled: room.isPrivate,
                                    fromId: senderId,
                                    from: userName
                                });
                            });
                            broadcastRoomState(room);
                            return;
                        }

                        if (data.type === 'close-room') {
                            if (!senderIsOwner && !senderIsAdmin) return;
                            closeRoom(room, senderId, userName);
                            return;
                        }

                        if (data.type === 'approve-join-request' || data.type === 'reject-join-request') {
                            if (!senderIsOwner && !senderIsAdmin) return;
                            const requestId = data.requestId || data.targetId || data.target;
                            if (!requestId) return;
                            const request = room.joinRequests.get(requestId);
                            if (!request) return;
                            room.joinRequests.delete(requestId);
                            broadcastJoinRequestToModerators(room, {
                                type: 'join-request-cancelled',
                                requestId,
                                byModerator: true
                            });
                            if (data.type === 'reject-join-request') {
                                safeSend(request.ws, {
                                    type: 'join-rejected',
                                    roomId: room.id,
                                    byId: senderId,
                                    byName: userName
                                });
                                return;
                            }

                            const participantInfo = {
                                ws: request.ws,
                                userName: request.userName,
                                userAvatar: request.userAvatar || '',
                                appUserId: request.appUserId || '',
                                video: false,
                                audio: true,
                                screen: false,
                                speaking: false,
                                isAdmin: false,
                                cameraFacingMode: 'user',
                                reconnectKey: request.reconnectKey || ''
                            };
                            room.participants.set(requestId, participantInfo);
                            if (!room.ownerId) {
                                room.ownerId = requestId;
                            }
                            ensureOwnerAdmin(room);
                            room.participants.forEach((participant, id) => {
                                if (id === requestId) return;
                                safeSend(participant.ws, {
                                    type: 'guest-joined',
                                    guest: serializeParticipant(requestId, participantInfo),
                                    ownerId: room.ownerId,
                                    iceServers: ACTIVE_ICE_SERVERS
                                });
                            });
                            safeSend(request.ws, {
                                type: 'joined',
                                roomId: room.id,
                                myId: requestId,
                                ownerId: room.ownerId,
                                iceServers: ACTIVE_ICE_SERVERS
                            });
                            broadcastRoomState(room);
                            return;
                        }

                        const targetId = data.targetId || data.target;

                        if (!targetId) return;
                        const target = room.participants.get(targetId);
                        if (!target) return;

                        if ((data.type === 'make-admin' || data.type === 'remove-admin' || data.type === 'kick') && !senderIsOwner) {
                            return;
                        }

                        if (data.type === 'make-admin') {
                            target.isAdmin = true;
                            room.participants.forEach((participant) => {
                                safeSend(participant.ws, {
                                    type: 'participant-updated',
                                    participantId: targetId,
                                    ownerId: room.ownerId,
                                    changes: { isAdmin: true }
                                });
                            });
                            broadcastRoomState(room);
                            return;
                        }

                        if (data.type === 'remove-admin') {
                            if (targetId === room.ownerId) {
                                return;
                            }
                            target.isAdmin = false;
                            room.participants.forEach((participant) => {
                                safeSend(participant.ws, {
                                    type: 'participant-updated',
                                    participantId: targetId,
                                    ownerId: room.ownerId,
                                    changes: { isAdmin: false },
                                    from: userName,
                                    fromId: senderId
                                });
                            });
                            broadcastRoomState(room);
                            return;
                        }

                        if (data.type === 'kick') {
                            safeSend(target.ws, {
                                type: 'kicked',
                                from: userName,
                                fromId: senderId
                            });
                            try { target.ws.close(); } catch (_) {}
                            return;
                        }

                        safeSend(target.ws, {
                            ...data,
                            targetId,
                            from: userName,
                            fromId: senderId
                        });
                    }
                    break;

                case 'cancel-join-request':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        if (!room.joinRequests.has(senderId)) return;
                        room.joinRequests.delete(senderId);
                        broadcastJoinRequestToModerators(room, {
                            type: 'join-request-cancelled',
                            requestId: senderId,
                            byModerator: false
                        });
                    }
                    break;

                case 'start-watch':
                case 'stop-watch':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        const sender = room.participants.get(senderId);
                        if (!sender) return;

                        const senderIsOwner = room.ownerId === senderId;
                        const senderIsAdmin = !!sender.isAdmin;

                        if (data.type === 'start-watch') {
                            const url = String(data.url || '').trim();
                            if (!url) return;

                            const active = room.watchParty;
                            const canStart = !active || active.ownerId === senderId || senderIsOwner || senderIsAdmin;
                            if (!canStart) return;

                            room.watchParty = {
                                url,
                                ownerId: senderId,
                                ownerName: userName,
                                startedAt: Date.now()
                            };

                            room.participants.forEach((participant) => {
                                safeSend(participant.ws, {
                                    type: 'watch-started',
                                    watchParty: room.watchParty,
                                    from: userName,
                                    fromId: senderId,
                                    ownerId: room.ownerId
                                });
                            });
                            return;
                        }

                        if (!room.watchParty) return;
                        const canStop = room.watchParty.ownerId === senderId || senderIsOwner || senderIsAdmin;
                        if (!canStop) return;

                        const previousWatch = room.watchParty;
                        room.watchParty = null;
                        room.participants.forEach((participant) => {
                            safeSend(participant.ws, {
                                type: 'watch-stopped',
                                previousWatch,
                                from: userName,
                                fromId: senderId,
                                ownerId: room.ownerId
                            });
                        });
                    }
                    break;

                case 'leave':
                    {
                        const roomToLeave = currentRoom;
                        currentRoom = null;
                        handleDisconnect(senderId, roomToLeave, { allowGrace: false });
                    }
                    break;
            }
        } catch (error) {
            console.error('Error:', error);
        }
    });

    ws.on('close', () => {
        if (currentAppUserId) {
            unregisterUserSession(currentAppUserId, ws);
            if (!userSessions.has(currentAppUserId)) {
                setUserOffline(currentAppUserId);
            }
        }
        if (ws.__superseded) return;
        const roomToLeave = currentRoom;
        currentRoom = null;
        const participantId = ws.__participantId || clientId;
        handleDisconnect(participantId, roomToLeave, { allowGrace: true });
    });
});

function handleDisconnect(clientId, roomId, options = {}) {
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    if (room.joinRequests.has(clientId)) {
        room.joinRequests.delete(clientId);
        broadcastJoinRequestToModerators(room, {
            type: 'join-request-cancelled',
            requestId: clientId,
            byModerator: false
        });
        if (room.participants.size === 0) {
            room.joinRequests.forEach((request) => {
                safeSend(request.ws, {
                    type: 'room-closed',
                    roomId
                });
                try { request.ws.close(); } catch (_) {}
            });
            room.joinRequests.clear();
            clearRoomPendingDisconnects(roomId);
            rooms.delete(roomId);
            console.log(`🏠 Room closed: ${roomId}`);
        }
        return;
    }

    const participant = room.participants.get(clientId);
    if (!participant) return;
    clearPendingDisconnect(roomId, clientId);
    const allowGrace = !!options.allowGrace;
    if (allowGrace) {
        const key = getPendingDisconnectKey(roomId, clientId);
        const timerId = setTimeout(() => {
            pendingDisconnects.delete(key);
            finalizeParticipantDisconnect(clientId, roomId);
        }, Math.max(1000, RECONNECT_GRACE_MS));
        pendingDisconnects.set(key, timerId);
        return;
    }
    finalizeParticipantDisconnect(clientId, roomId);
}

// ============ АВТО-ПИНГ ДЛЯ ПРЕДОТВРАЩЕНИЯ ЗАСЫПАНИЯ ============
// Каждые 4 минуты пингуем сам себя, чтобы Render не уснул
const keepAlive = () => {
    const http = require('http');
    const options = {
        hostname: 'localhost',
        port: PORT,
        path: '/',
        method: 'GET',
        timeout: 5000
    };
    
    const req = http.request(options, (res) => {
        console.log(`🏓 Self-ping at ${new Date().toLocaleTimeString()} - Status: ${res.statusCode}`);
    });
    
    req.on('error', (err) => {
        // Тишина, просто не пишем ошибки
    });
    
    req.end();
};

// Запускаем авто-пинг каждые 4 минуты
setInterval(keepAlive, 4 * 60 * 1000);
console.log('✅ Keep-alive enabled (ping every 4 minutes)');
