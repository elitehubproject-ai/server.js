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
const RECONNECT_GRACE_MS = process.env.RECONNECT_GRACE_MS ? parseInt(process.env.RECONNECT_GRACE_MS, 10) : 15000;
const pendingDisconnects = new Map();
const MESSENGER_STORE_PATH = path.join(__dirname, 'messenger_store.json');
const FRIENDS_STORE_PATH = path.join(__dirname, 'friends_store.json');
const messengerSessions = new Map();

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

function readJsonFile(filePath, fallback) {
    try {
        if (!fs.existsSync(filePath)) return fallback;
        const raw = fs.readFileSync(filePath, 'utf8');
        if (!raw || !raw.trim()) return fallback;
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : fallback;
    } catch (_) {
        return fallback;
    }
}

function writeJsonFile(filePath, payload) {
    try {
        fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf8');
        return true;
    } catch (_) {
        return false;
    }
}

function normalizeText(value, maxLen = 2000) {
    const str = String(value ?? '').trim();
    if (!str) return '';
    return str.slice(0, maxLen);
}

function normalizeUserId(value) {
    return normalizeText(value, 120);
}

function nowMs() {
    return Date.now();
}

function createDefaultMessengerStore() {
    return {
        users: {},
        chats: {},
        messages: {},
        userChats: {},
        hiddenChats: {},
        chatIndex: [],
        messageOrder: {},
        blocks: [],
        settings: {},
        profiles: {}
    };
}

function loadMessengerStore() {
    const base = createDefaultMessengerStore();
    const parsed = readJsonFile(MESSENGER_STORE_PATH, base);
    return {
        users: parsed.users && typeof parsed.users === 'object' ? parsed.users : {},
        chats: parsed.chats && typeof parsed.chats === 'object' ? parsed.chats : {},
        messages: parsed.messages && typeof parsed.messages === 'object' ? parsed.messages : {},
        userChats: parsed.userChats && typeof parsed.userChats === 'object' ? parsed.userChats : {},
        hiddenChats: parsed.hiddenChats && typeof parsed.hiddenChats === 'object' ? parsed.hiddenChats : {},
        chatIndex: Array.isArray(parsed.chatIndex) ? parsed.chatIndex : [],
        messageOrder: parsed.messageOrder && typeof parsed.messageOrder === 'object' ? parsed.messageOrder : {},
        blocks: Array.isArray(parsed.blocks) ? parsed.blocks : [],
        settings: parsed.settings && typeof parsed.settings === 'object' ? parsed.settings : {},
        profiles: parsed.profiles && typeof parsed.profiles === 'object' ? parsed.profiles : {}
    };
}

const messengerStore = loadMessengerStore();

function persistMessengerStore() {
    return writeJsonFile(MESSENGER_STORE_PATH, messengerStore);
}

function getFriendsStore() {
    const parsed = readJsonFile(FRIENDS_STORE_PATH, { friends: [], users: [] });
    return {
        friends: Array.isArray(parsed.friends) ? parsed.friends : [],
        users: Array.isArray(parsed.users) ? parsed.users : []
    };
}

function syncUsersFromFriendsStore() {
    const source = getFriendsStore();
    const users = Array.isArray(source.users) ? source.users : [];
    let changed = false;
    users.forEach((row) => {
        const uid = normalizeUserId(row?.id);
        if (!uid) return;
        const before = messengerStore.users[uid] ? JSON.stringify(messengerStore.users[uid]) : '';
        ensureMessengerUser(uid, {
            name: row?.name || '',
            avatar: row?.avatar || '',
            username: row?.username || ''
        });
        const after = messengerStore.users[uid] ? JSON.stringify(messengerStore.users[uid]) : '';
        if (before !== after) changed = true;
    });
    return changed;
}

function areFriendsByUserId(firstId, secondId) {
    const a = normalizeUserId(firstId);
    const b = normalizeUserId(secondId);
    if (!a || !b || a === b) return false;
    const friendsStore = getFriendsStore();
    const friends = Array.isArray(friendsStore.friends) ? friendsStore.friends : [];
    return friends.some((row) => {
        const left = normalizeUserId(row?.a);
        const right = normalizeUserId(row?.b);
        return (left === a && right === b) || (left === b && right === a);
    });
}

function defaultPrivacySettings() {
    return {
        whoCanCall: 'all',
        whoCanWrite: 'all',
        whoCanViewProfile: 'all'
    };
}

function getUserSettings(userId) {
    const uid = normalizeUserId(userId);
    if (!uid) return defaultPrivacySettings();
    const current = messengerStore.settings[uid] || {};
    const base = defaultPrivacySettings();
    const allowed = ['all', 'friends', 'none'];
    if (allowed.includes(current.whoCanCall)) base.whoCanCall = current.whoCanCall;
    if (allowed.includes(current.whoCanWrite)) base.whoCanWrite = current.whoCanWrite;
    if (allowed.includes(current.whoCanViewProfile)) base.whoCanViewProfile = current.whoCanViewProfile;
    return base;
}

function setUserSettings(userId, partial) {
    const uid = normalizeUserId(userId);
    if (!uid) return getUserSettings(uid);
    const next = getUserSettings(uid);
    const allowed = ['all', 'friends', 'none'];
    if (allowed.includes(partial?.whoCanCall)) next.whoCanCall = partial.whoCanCall;
    if (allowed.includes(partial?.whoCanWrite)) next.whoCanWrite = partial.whoCanWrite;
    if (allowed.includes(partial?.whoCanViewProfile)) next.whoCanViewProfile = partial.whoCanViewProfile;
    messengerStore.settings[uid] = next;
    return next;
}

function normalizeUsername(username) {
    return normalizeText(username, 40).toLowerCase().replace(/[^a-z0-9_.-]/g, '').slice(0, 24);
}

function getProfile(userId) {
    const uid = normalizeUserId(userId);
    if (!uid) return null;
    const fallbackUser = messengerStore.users[uid] || {};
    const raw = messengerStore.profiles[uid] || {};
    return {
        id: uid,
        name: normalizeText(raw.name || fallbackUser.name || 'Пользователь', 80),
        username: normalizeUsername(raw.username || fallbackUser.username || ''),
        status: normalizeText(raw.status || '', 140),
        avatar: normalizeText(raw.avatar || fallbackUser.avatar || '', 300000),
        cover: normalizeText(raw.cover || '', 300000),
        updatedAt: Number(raw.updatedAt || 0) || 0
    };
}

function ensureMessengerUser(userId, payload = {}) {
    const uid = normalizeUserId(userId);
    if (!uid) return null;
    if (!messengerStore.users[uid]) {
        messengerStore.users[uid] = {
            id: uid,
            name: normalizeText(payload.name || 'Пользователь', 80),
            avatar: normalizeText(payload.avatar || '', 300000),
            username: normalizeUsername(payload.username || ''),
            status: normalizeText(payload.status || '', 140),
            lastSeenAt: nowMs()
        };
    } else {
        const user = messengerStore.users[uid];
        const nextName = normalizeText(payload.name || user.name || 'Пользователь', 80);
        const nextAvatar = normalizeText(payload.avatar || user.avatar || '', 300000);
        const nextUsername = normalizeUsername(payload.username || user.username || '');
        const nextStatus = normalizeText(payload.status || user.status || '', 140);
        user.name = nextName;
        user.avatar = nextAvatar;
        user.username = nextUsername;
        user.status = nextStatus;
        user.lastSeenAt = nowMs();
    }
    if (!messengerStore.userChats[uid]) messengerStore.userChats[uid] = [];
    if (!messengerStore.hiddenChats[uid] || typeof messengerStore.hiddenChats[uid] !== 'object') {
        messengerStore.hiddenChats[uid] = {};
    }
    const existingProfile = messengerStore.profiles[uid] || {};
    messengerStore.profiles[uid] = {
        id: uid,
        name: normalizeText(existingProfile.name || messengerStore.users[uid].name || 'Пользователь', 80),
        username: normalizeUsername(existingProfile.username || messengerStore.users[uid].username || ''),
        status: normalizeText(existingProfile.status || messengerStore.users[uid].status || '', 140),
        avatar: normalizeText(existingProfile.avatar || messengerStore.users[uid].avatar || '', 300000),
        cover: normalizeText(existingProfile.cover || '', 300000),
        updatedAt: Number(existingProfile.updatedAt || nowMs())
    };
    return messengerStore.users[uid];
}

function getBlockRecord(blockerId, targetId) {
    const blocker = normalizeUserId(blockerId);
    const target = normalizeUserId(targetId);
    if (!blocker || !target) return null;
    return messengerStore.blocks.find((row) => normalizeUserId(row?.blockerId) === blocker && normalizeUserId(row?.targetId) === target) || null;
}

function isBlocked(blockerId, targetId) {
    return !!getBlockRecord(blockerId, targetId);
}

function setBlock(blockerId, targetId, comment = '') {
    const blocker = normalizeUserId(blockerId);
    const target = normalizeUserId(targetId);
    if (!blocker || !target || blocker === target) return null;
    const cleanComment = normalizeText(comment, 200);
    const existing = getBlockRecord(blocker, target);
    if (existing) {
        existing.comment = cleanComment;
        existing.updatedAt = nowMs();
        return existing;
    }
    const row = {
        id: `blk_${uuidv4().slice(0, 12)}`,
        blockerId: blocker,
        targetId: target,
        comment: cleanComment,
        createdAt: nowMs(),
        updatedAt: nowMs()
    };
    messengerStore.blocks.push(row);
    return row;
}

function clearBlock(blockerId, targetId) {
    const blocker = normalizeUserId(blockerId);
    const target = normalizeUserId(targetId);
    if (!blocker || !target) return false;
    const before = messengerStore.blocks.length;
    messengerStore.blocks = messengerStore.blocks.filter((row) => {
        return !(normalizeUserId(row?.blockerId) === blocker && normalizeUserId(row?.targetId) === target);
    });
    return messengerStore.blocks.length !== before;
}

function canPerformByPrivacy(actorId, targetId, fieldName) {
    const actor = normalizeUserId(actorId);
    const target = normalizeUserId(targetId);
    if (!actor || !target) return false;
    if (actor === target) return true;
    if (isBlocked(target, actor) || isBlocked(actor, target)) return false;
    const settings = getUserSettings(target);
    const rule = settings[fieldName] || 'all';
    if (rule === 'none') return false;
    if (rule === 'friends') return areFriendsByUserId(actor, target);
    return true;
}

function buildDirectKey(firstId, secondId) {
    const left = normalizeUserId(firstId);
    const right = normalizeUserId(secondId);
    if (!left || !right) return '';
    return [left, right].sort().join('|');
}

function getOrCreateDirectChat(firstId, secondId) {
    const a = normalizeUserId(firstId);
    const b = normalizeUserId(secondId);
    if (!a || !b || a === b) return null;
    const directKey = buildDirectKey(a, b);
    const existingId = messengerStore.chatIndex.find((chatId) => {
        const chat = messengerStore.chats[chatId];
        return chat?.type === 'direct' && chat?.directKey === directKey;
    });
    if (existingId) return messengerStore.chats[existingId] || null;
    const chatId = `chat_${uuidv4().slice(0, 12)}`;
    const createdAt = nowMs();
    const chat = {
        id: chatId,
        type: 'direct',
        directKey,
        members: [a, b],
        createdAt,
        updatedAt: createdAt,
        lastMessageId: ''
    };
    messengerStore.chats[chatId] = chat;
    messengerStore.chatIndex.unshift(chatId);
    if (!messengerStore.userChats[a]) messengerStore.userChats[a] = [];
    if (!messengerStore.userChats[b]) messengerStore.userChats[b] = [];
    if (!messengerStore.userChats[a].includes(chatId)) messengerStore.userChats[a].unshift(chatId);
    if (!messengerStore.userChats[b].includes(chatId)) messengerStore.userChats[b].unshift(chatId);
    messengerStore.messageOrder[chatId] = [];
    return chat;
}

function touchChat(chatId, messageId = '') {
    const chat = messengerStore.chats[chatId];
    if (!chat) return;
    chat.updatedAt = nowMs();
    if (messageId) chat.lastMessageId = messageId;
    messengerStore.chatIndex = messengerStore.chatIndex.filter((id) => id !== chatId);
    messengerStore.chatIndex.unshift(chatId);
    (chat.members || []).forEach((uid) => {
        if (!messengerStore.userChats[uid]) messengerStore.userChats[uid] = [];
        messengerStore.userChats[uid] = messengerStore.userChats[uid].filter((id) => id !== chatId);
        messengerStore.userChats[uid].unshift(chatId);
    });
}

function getMessageList(chatId, limit = 120) {
    const ids = Array.isArray(messengerStore.messageOrder[chatId]) ? messengerStore.messageOrder[chatId] : [];
    const slice = ids.slice(-Math.max(1, limit));
    return slice
        .map((id) => messengerStore.messages[id])
        .filter(Boolean)
        .map((msg) => ({ ...msg }));
}

function buildChatSummaryForUser(chatId, userId) {
    const uid = normalizeUserId(userId);
    const chat = messengerStore.chats[chatId];
    if (!uid || !chat) return null;
    const hiddenMap = messengerStore.hiddenChats[uid] || {};
    if (hiddenMap[chatId]) return null;
    const members = Array.isArray(chat.members) ? chat.members : [];
    if (!members.includes(uid)) return null;
    const peerId = members.find((id) => id !== uid) || uid;
    const profile = getProfile(peerId);
    const lastMessage = chat.lastMessageId ? messengerStore.messages[chat.lastMessageId] || null : null;
    const online = messengerSessions.has(peerId);
    return {
        id: chat.id,
        type: chat.type,
        peerId,
        peerProfile: profile,
        online,
        lastMessage,
        updatedAt: Number(chat.updatedAt || chat.createdAt || 0),
        createdAt: Number(chat.createdAt || 0)
    };
}

function buildMessengerState(userId) {
    const uid = normalizeUserId(userId);
    if (!uid) return null;
    const list = Array.isArray(messengerStore.userChats[uid]) ? messengerStore.userChats[uid] : [];
    const chats = list
        .map((chatId) => buildChatSummaryForUser(chatId, uid))
        .filter(Boolean)
        .sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
    const blacklist = messengerStore.blocks
        .filter((row) => normalizeUserId(row?.blockerId) === uid)
        .map((row) => {
            const targetId = normalizeUserId(row?.targetId);
            return {
                id: normalizeText(row?.id, 60),
                targetId,
                comment: normalizeText(row?.comment, 200),
                createdAt: Number(row?.createdAt || 0),
                profile: getProfile(targetId)
            };
        });
    return {
        selfProfile: getProfile(uid),
        chats,
        settings: getUserSettings(uid),
        blacklist
    };
}

function subscribeMessengerSocket(userId, ws) {
    const uid = normalizeUserId(userId);
    if (!uid || !ws) return;
    if (!messengerSessions.has(uid)) {
        messengerSessions.set(uid, new Set());
    }
    messengerSessions.get(uid).add(ws);
    ws.__messengerUserId = uid;
}

function unsubscribeMessengerSocket(ws) {
    if (!ws?.__messengerUserId) return;
    const uid = normalizeUserId(ws.__messengerUserId);
    const bucket = messengerSessions.get(uid);
    if (bucket) {
        bucket.delete(ws);
        if (!bucket.size) messengerSessions.delete(uid);
    }
    ws.__messengerUserId = '';
}

function sendToMessengerUser(userId, payload) {
    const uid = normalizeUserId(userId);
    if (!uid) return;
    const bucket = messengerSessions.get(uid);
    if (!bucket || !bucket.size) return;
    bucket.forEach((socket) => safeSend(socket, payload));
}

function sendChatUpdate(chatId) {
    const chat = messengerStore.chats[chatId];
    if (!chat) return;
    (chat.members || []).forEach((uid) => {
        const summary = buildChatSummaryForUser(chatId, uid);
        if (!summary) return;
        sendToMessengerUser(uid, {
            type: 'messenger-chat-updated',
            chat: summary
        });
    });
}

function sendPresenceUpdate(userId) {
    const uid = normalizeUserId(userId);
    if (!uid) return;
    const online = messengerSessions.has(uid);
    const profile = getProfile(uid);
    Object.keys(messengerStore.userChats || {}).forEach((ownerId) => {
        const chats = messengerStore.userChats[ownerId] || [];
        const hasPeer = chats.some((chatId) => {
            const chat = messengerStore.chats[chatId];
            return !!chat && Array.isArray(chat.members) && chat.members.includes(uid);
        });
        if (!hasPeer) return;
        sendToMessengerUser(ownerId, {
            type: 'messenger-presence',
            userId: uid,
            online,
            profile
        });
    });
}

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

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const senderId = ws.__participantId || clientId;

            switch (data.type) {
                case 'messenger-auth':
                    {
                        const authUserId = normalizeUserId(data.appUserId || '');
                        if (!authUserId) {
                            safeSend(ws, { type: 'messenger-error', message: 'appUserId required' });
                            return;
                        }
                        const user = ensureMessengerUser(authUserId, {
                            name: data.name || '',
                            avatar: data.avatar || '',
                            username: data.username || '',
                            status: data.status || ''
                        });
                        subscribeMessengerSocket(authUserId, ws);
                        const usersSynced = syncUsersFromFriendsStore();
                        if (usersSynced) {
                            persistMessengerStore();
                        }
                        persistMessengerStore();
                        safeSend(ws, {
                            type: 'messenger-auth-ok',
                            userId: authUserId,
                            profile: getProfile(authUserId),
                            state: buildMessengerState(authUserId)
                        });
                        sendPresenceUpdate(authUserId);
                    }
                    break;

                case 'messenger-sync':
                    {
                        const syncUserId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        if (!syncUserId) return;
                        ensureMessengerUser(syncUserId, {});
                        if (syncUsersFromFriendsStore()) {
                            persistMessengerStore();
                        }
                        persistMessengerStore();
                        safeSend(ws, {
                            type: 'messenger-state',
                            state: buildMessengerState(syncUserId)
                        });
                    }
                    break;

                case 'messenger-search':
                    {
                        const searchUserId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const query = normalizeText(data.query || '', 80).toLowerCase();
                        if (!searchUserId) return;
                        ensureMessengerUser(searchUserId, {});
                        if (syncUsersFromFriendsStore()) {
                            persistMessengerStore();
                        }
                        const results = [];
                        if (query) {
                            Object.keys(messengerStore.users).forEach((uid) => {
                                if (uid === searchUserId || results.length >= 50) return;
                                const profile = getProfile(uid);
                                if (!profile) return;
                                const haystack = `${uid} ${profile.name || ''} ${profile.username || ''}`.toLowerCase();
                                if (!haystack.includes(query)) return;
                                const blockedByMe = isBlocked(searchUserId, uid);
                                const blockedMe = isBlocked(uid, searchUserId);
                                results.push({
                                    id: uid,
                                    profile,
                                    isFriend: areFriendsByUserId(searchUserId, uid),
                                    blockedByMe,
                                    blockedMe,
                                    canWrite: canPerformByPrivacy(searchUserId, uid, 'whoCanWrite'),
                                    canViewProfile: canPerformByPrivacy(searchUserId, uid, 'whoCanViewProfile')
                                });
                            });
                        }
                        safeSend(ws, {
                            type: 'messenger-search-results',
                            query,
                            results
                        });
                    }
                    break;

                case 'messenger-open-chat':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const targetId = normalizeUserId(data.targetId || '');
                        let chatId = normalizeUserId(data.chatId || '');
                        if (!actorId) return;
                        if (syncUsersFromFriendsStore()) {
                            persistMessengerStore();
                        }
                        ensureMessengerUser(actorId, {});
                        if (!chatId && targetId) {
                            ensureMessengerUser(targetId, {});
                            const chat = getOrCreateDirectChat(actorId, targetId);
                            chatId = chat?.id || '';
                            persistMessengerStore();
                            if (chatId) sendChatUpdate(chatId);
                        }
                        const chat = messengerStore.chats[chatId];
                        if (!chat || !Array.isArray(chat.members) || !chat.members.includes(actorId)) {
                            safeSend(ws, { type: 'messenger-error', message: 'Чат недоступен' });
                            return;
                        }
                        const peerId = chat.members.find((id) => id !== actorId) || actorId;
                        safeSend(ws, {
                            type: 'messenger-chat-opened',
                            chat: buildChatSummaryForUser(chat.id, actorId),
                            peerProfile: getProfile(peerId),
                            canWrite: canPerformByPrivacy(actorId, peerId, 'whoCanWrite'),
                            messages: getMessageList(chat.id, 160)
                        });
                    }
                    break;

                case 'messenger-send-message':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const targetId = normalizeUserId(data.targetId || '');
                        let chatId = normalizeUserId(data.chatId || '');
                        const text = normalizeText(data.text || '', 5000);
                        const replyToId = normalizeText(data.replyToId || '', 80);
                        const forwardedFromId = normalizeText(data.forwardedFromId || '', 80);
                        if (!actorId) return;
                        if (syncUsersFromFriendsStore()) {
                            persistMessengerStore();
                        }
                        ensureMessengerUser(actorId, {});
                        if (!chatId && targetId) {
                            ensureMessengerUser(targetId, {});
                            const autoChat = getOrCreateDirectChat(actorId, targetId);
                            chatId = autoChat?.id || '';
                        }
                        if (!chatId) {
                            safeSend(ws, { type: 'messenger-error', message: 'chatId required' });
                            return;
                        }
                        const chat = messengerStore.chats[chatId];
                        if (!chat || !Array.isArray(chat.members) || !chat.members.includes(actorId)) {
                            safeSend(ws, { type: 'messenger-error', message: 'Чат недоступен' });
                            return;
                        }
                        const recipientId = chat.members.find((id) => id !== actorId) || actorId;
                        if (!canPerformByPrivacy(actorId, recipientId, 'whoCanWrite')) {
                            safeSend(ws, { type: 'messenger-error', message: 'Пользователь запретил сообщения' });
                            return;
                        }
                        if (!text && !forwardedFromId) {
                            safeSend(ws, { type: 'messenger-error', message: 'Пустое сообщение' });
                            return;
                        }
                        const messageId = `msg_${uuidv4().slice(0, 12)}`;
                        const createdAt = nowMs();
                        const messagePayload = {
                            id: messageId,
                            chatId,
                            senderId: actorId,
                            text,
                            replyToId: replyToId || '',
                            forwardedFromId: forwardedFromId || '',
                            createdAt,
                            updatedAt: createdAt,
                            editedAt: 0,
                            deletedForEveryone: false
                        };
                        messengerStore.messages[messageId] = messagePayload;
                        if (!Array.isArray(messengerStore.messageOrder[chatId])) {
                            messengerStore.messageOrder[chatId] = [];
                        }
                        messengerStore.messageOrder[chatId].push(messageId);
                        touchChat(chatId, messageId);
                        persistMessengerStore();
                        (chat.members || []).forEach((uid) => {
                            const hidden = messengerStore.hiddenChats[uid] || {};
                            if (hidden[chatId]) delete hidden[chatId];
                            messengerStore.hiddenChats[uid] = hidden;
                            sendToMessengerUser(uid, {
                                type: 'messenger-message',
                                chatId,
                                message: { ...messagePayload }
                            });
                        });
                        sendChatUpdate(chatId);
                    }
                    break;

                case 'messenger-edit-message':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const messageId = normalizeText(data.messageId || '', 80);
                        const text = normalizeText(data.text || '', 5000);
                        if (!actorId || !messageId || !text) return;
                        const messageRow = messengerStore.messages[messageId];
                        if (!messageRow) return;
                        if (normalizeUserId(messageRow.senderId) !== actorId) return;
                        messageRow.text = text;
                        messageRow.updatedAt = nowMs();
                        messageRow.editedAt = nowMs();
                        const chatId = normalizeUserId(messageRow.chatId);
                        persistMessengerStore();
                        const chat = messengerStore.chats[chatId];
                        (chat?.members || []).forEach((uid) => {
                            sendToMessengerUser(uid, {
                                type: 'messenger-message-updated',
                                chatId,
                                message: { ...messageRow }
                            });
                        });
                        sendChatUpdate(chatId);
                    }
                    break;

                case 'messenger-delete-message':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const messageId = normalizeText(data.messageId || '', 80);
                        const mode = normalizeText(data.mode || 'self', 24);
                        if (!actorId || !messageId) return;
                        const messageRow = messengerStore.messages[messageId];
                        if (!messageRow) return;
                        const chatId = normalizeUserId(messageRow.chatId);
                        const chat = messengerStore.chats[chatId];
                        if (!chat || !Array.isArray(chat.members) || !chat.members.includes(actorId)) return;
                        if (normalizeUserId(messageRow.senderId) !== actorId) return;
                        if (mode === 'everyone') {
                            messageRow.deletedForEveryone = true;
                            messageRow.text = '';
                            messageRow.updatedAt = nowMs();
                        } else {
                            messageRow.deletedForEveryone = true;
                            messageRow.text = '';
                            messageRow.updatedAt = nowMs();
                        }
                        persistMessengerStore();
                        (chat.members || []).forEach((uid) => {
                            sendToMessengerUser(uid, {
                                type: 'messenger-message-updated',
                                chatId,
                                message: { ...messageRow }
                            });
                        });
                        sendChatUpdate(chatId);
                    }
                    break;

                case 'messenger-clear-chat':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const chatId = normalizeUserId(data.chatId || '');
                        if (!actorId || !chatId) return;
                        const chat = messengerStore.chats[chatId];
                        if (!chat || !Array.isArray(chat.members) || !chat.members.includes(actorId)) return;
                        const ids = Array.isArray(messengerStore.messageOrder[chatId]) ? messengerStore.messageOrder[chatId] : [];
                        ids.forEach((mid) => {
                            if (messengerStore.messages[mid]) {
                                messengerStore.messages[mid].deletedForEveryone = true;
                                messengerStore.messages[mid].text = '';
                                messengerStore.messages[mid].updatedAt = nowMs();
                            }
                        });
                        persistMessengerStore();
                        (chat.members || []).forEach((uid) => {
                            sendToMessengerUser(uid, {
                                type: 'messenger-chat-cleared',
                                chatId
                            });
                        });
                        sendChatUpdate(chatId);
                    }
                    break;

                case 'messenger-delete-chat':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const chatId = normalizeUserId(data.chatId || '');
                        if (!actorId || !chatId) return;
                        const chat = messengerStore.chats[chatId];
                        if (!chat || !Array.isArray(chat.members) || !chat.members.includes(actorId)) return;
                        if (!messengerStore.hiddenChats[actorId] || typeof messengerStore.hiddenChats[actorId] !== 'object') {
                            messengerStore.hiddenChats[actorId] = {};
                        }
                        messengerStore.hiddenChats[actorId][chatId] = nowMs();
                        persistMessengerStore();
                        sendToMessengerUser(actorId, {
                            type: 'messenger-chat-deleted',
                            chatId
                        });
                    }
                    break;

                case 'messenger-typing':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const chatId = normalizeUserId(data.chatId || '');
                        const isTyping = !!data.isTyping;
                        if (!actorId || !chatId) return;
                        const chat = messengerStore.chats[chatId];
                        if (!chat || !Array.isArray(chat.members) || !chat.members.includes(actorId)) return;
                        (chat.members || []).forEach((uid) => {
                            if (uid === actorId) return;
                            sendToMessengerUser(uid, {
                                type: 'messenger-typing',
                                chatId,
                                userId: actorId,
                                isTyping
                            });
                        });
                    }
                    break;

                case 'messenger-profile-get':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const targetId = normalizeUserId(data.targetId || actorId);
                        if (!actorId || !targetId) return;
                        if (syncUsersFromFriendsStore()) {
                            persistMessengerStore();
                        }
                        ensureMessengerUser(actorId, {});
                        ensureMessengerUser(targetId, {});
                        const blockedByTarget = getBlockRecord(targetId, actorId);
                        const blockedByMe = getBlockRecord(actorId, targetId);
                        if (blockedByTarget) {
                            safeSend(ws, {
                                type: 'messenger-profile-blocked',
                                targetId,
                                byId: targetId,
                                comment: normalizeText(blockedByTarget.comment || '', 200)
                            });
                            return;
                        }
                        if (!canPerformByPrivacy(actorId, targetId, 'whoCanViewProfile')) {
                            safeSend(ws, {
                                type: 'messenger-profile-closed',
                                targetId
                            });
                            return;
                        }
                        safeSend(ws, {
                            type: 'messenger-profile',
                            targetId,
                            profile: getProfile(targetId),
                            settings: getUserSettings(targetId),
                            blockedByMe: !!blockedByMe
                        });
                    }
                    break;

                case 'messenger-profile-update':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        if (!actorId) return;
                        const profile = getProfile(actorId) || {
                            id: actorId,
                            name: 'Пользователь',
                            username: '',
                            status: '',
                            avatar: '',
                            cover: '',
                            updatedAt: 0
                        };
                        const nextUsername = normalizeUsername(data.username ?? profile.username);
                        const usernameBusy = Object.keys(messengerStore.profiles).some((uid) => {
                            if (uid === actorId) return false;
                            const candidate = normalizeUsername(messengerStore.profiles[uid]?.username || '');
                            return !!nextUsername && candidate === nextUsername;
                        });
                        if (usernameBusy) {
                            safeSend(ws, { type: 'messenger-error', message: 'Username уже занят' });
                            return;
                        }
                        const nextProfile = {
                            ...profile,
                            name: normalizeText(data.name ?? profile.name, 80) || 'Пользователь',
                            username: nextUsername,
                            status: normalizeText(data.status ?? profile.status, 140),
                            avatar: normalizeText(data.avatar ?? profile.avatar, 300000),
                            cover: normalizeText(data.cover ?? profile.cover, 300000),
                            updatedAt: nowMs()
                        };
                        messengerStore.profiles[actorId] = nextProfile;
                        const userRow = ensureMessengerUser(actorId, {
                            name: nextProfile.name,
                            avatar: nextProfile.avatar,
                            username: nextProfile.username,
                            status: nextProfile.status
                        });
                        userRow.lastSeenAt = nowMs();
                        persistMessengerStore();
                        sendToMessengerUser(actorId, {
                            type: 'messenger-profile',
                            targetId: actorId,
                            profile: nextProfile,
                            settings: getUserSettings(actorId),
                            blockedByMe: false
                        });
                        sendPresenceUpdate(actorId);
                        const chats = messengerStore.userChats[actorId] || [];
                        chats.forEach((chatId) => sendChatUpdate(chatId));
                    }
                    break;

                case 'messenger-settings-update':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        if (!actorId) return;
                        const settings = setUserSettings(actorId, data.settings || {});
                        persistMessengerStore();
                        sendToMessengerUser(actorId, {
                            type: 'messenger-settings',
                            settings
                        });
                    }
                    break;

                case 'messenger-block-user':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const targetId = normalizeUserId(data.targetId || '');
                        if (!actorId || !targetId || actorId === targetId) return;
                        ensureMessengerUser(actorId, {});
                        ensureMessengerUser(targetId, {});
                        setBlock(actorId, targetId, data.comment || '');
                        persistMessengerStore();
                        sendToMessengerUser(actorId, {
                            type: 'messenger-state',
                            state: buildMessengerState(actorId)
                        });
                    }
                    break;

                case 'messenger-unblock-user':
                    {
                        const actorId = normalizeUserId(data.appUserId || ws.__messengerUserId || '');
                        const targetId = normalizeUserId(data.targetId || '');
                        if (!actorId || !targetId) return;
                        clearBlock(actorId, targetId);
                        persistMessengerStore();
                        sendToMessengerUser(actorId, {
                            type: 'messenger-state',
                            state: buildMessengerState(actorId)
                        });
                    }
                    break;

                case 'ping':
                    safeSend(ws, { type: 'pong', ts: Date.now() });
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
        const messengerUserId = normalizeUserId(ws.__messengerUserId || '');
        if (messengerUserId) {
            unsubscribeMessengerSocket(ws);
            sendPresenceUpdate(messengerUserId);
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
