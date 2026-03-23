const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const http = require('http');

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
const messengerClients = new Map();
const messengerProfiles = new Map();
const messengerThreads = new Map();
const MAX_THREAD_MESSAGES = 200;

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

function sanitizeChatText(value) {
    return String(value || '').trim().slice(0, 4000);
}

function normalizeMessengerId(value, max = 120) {
    return String(value || '').trim().slice(0, max);
}

function getMessengerThreadId(first, second) {
    const a = normalizeMessengerId(first);
    const b = normalizeMessengerId(second);
    if (!a || !b) return '';
    return a.localeCompare(b) <= 0 ? `${a}::${b}` : `${b}::${a}`;
}

function upsertMessengerProfile(appUserId, userName = '', userAvatar = '', username = '') {
    const id = normalizeMessengerId(appUserId);
    if (!id) return null;
    const existing = messengerProfiles.get(id) || {};
    const next = {
        appUserId: id,
        userName: normalizeMessengerId(userName, 120) || existing.userName || 'Пользователь',
        userAvatar: normalizeMessengerId(userAvatar, 500) || existing.userAvatar || '',
        username: normalizeMessengerId(username, 80) || existing.username || '',
        lastSeenAt: Date.now()
    };
    messengerProfiles.set(id, next);
    return next;
}

function getMessengerPresence(appUserId) {
    const id = normalizeMessengerId(appUserId);
    if (!id) return { online: false, lastSeenAt: 0 };
    const sockets = messengerClients.get(id);
    const profile = messengerProfiles.get(id);
    if (sockets && sockets.size > 0) {
        return { online: true, lastSeenAt: Date.now() };
    }
    return { online: false, lastSeenAt: profile?.lastSeenAt || 0 };
}

function getMessengerThreadMessages(currentUserId, peerUserId) {
    const threadId = getMessengerThreadId(currentUserId, peerUserId);
    if (!threadId) return [];
    const thread = messengerThreads.get(threadId);
    if (!thread) return [];
    return thread.messages.slice(-MAX_THREAD_MESSAGES).map((message) => ({ ...message }));
}

function buildMessengerChatSummaries(currentUserId) {
    const appUserId = normalizeMessengerId(currentUserId);
    if (!appUserId) return [];
    const summaries = [];
    messengerThreads.forEach((thread, threadId) => {
        if (!threadId.includes('::')) return;
        const parts = threadId.split('::');
        if (parts.length !== 2) return;
        const [first, second] = parts;
        if (first !== appUserId && second !== appUserId) return;
        const peerAppUserId = first === appUserId ? second : first;
        const peerProfile = messengerProfiles.get(peerAppUserId) || {};
        const lastMessage = thread.messages.length ? thread.messages[thread.messages.length - 1] : null;
        const presence = getMessengerPresence(peerAppUserId);
        summaries.push({
            peerAppUserId,
            peerUserName: peerProfile.userName || peerAppUserId,
            peerAvatar: peerProfile.userAvatar || '',
            peerUsername: peerProfile.username || '',
            online: presence.online,
            lastSeenAt: presence.lastSeenAt,
            lastMessage: lastMessage ? { ...lastMessage } : null,
            updatedAt: Number(thread.updatedAt || 0)
        });
    });
    summaries.sort((a, b) => {
        const timeA = Number(a.updatedAt || a.lastMessage?.createdAt || 0);
        const timeB = Number(b.updatedAt || b.lastMessage?.createdAt || 0);
        return timeB - timeA;
    });
    return summaries.slice(0, 150);
}

function safeSendToMessengerUser(appUserId, payload, excludeWs = null) {
    const id = normalizeMessengerId(appUserId);
    if (!id) return;
    const sockets = messengerClients.get(id);
    if (!sockets || sockets.size === 0) return;
    sockets.forEach((socket) => {
        if (excludeWs && socket === excludeWs) return;
        safeSend(socket, payload);
    });
}

function broadcastMessengerPresence(appUserId) {
    const id = normalizeMessengerId(appUserId);
    if (!id) return;
    const profile = messengerProfiles.get(id) || {};
    const presence = getMessengerPresence(id);
    const payload = {
        type: 'messenger-presence',
        appUserId: id,
        online: presence.online,
        lastSeenAt: presence.lastSeenAt,
        userName: profile.userName || '',
        userAvatar: profile.userAvatar || '',
        username: profile.username || ''
    };
    messengerClients.forEach((sockets) => {
        sockets.forEach((socket) => safeSend(socket, payload));
    });
}

function registerMessengerSocket(ws, appUserId, userName = '', userAvatar = '', username = '') {
    const id = normalizeMessengerId(appUserId);
    if (!id) return null;
    const profile = upsertMessengerProfile(id, userName, userAvatar, username);
    ws.__messengerAppUserId = id;
    if (!messengerClients.has(id)) {
        messengerClients.set(id, new Set());
    }
    messengerClients.get(id).add(ws);
    broadcastMessengerPresence(id);
    return profile;
}

function unregisterMessengerSocket(ws) {
    const appUserId = normalizeMessengerId(ws?.__messengerAppUserId || '');
    if (!appUserId) return;
    const sockets = messengerClients.get(appUserId);
    if (sockets) {
        sockets.delete(ws);
        if (sockets.size === 0) {
            messengerClients.delete(appUserId);
            const profile = messengerProfiles.get(appUserId);
            if (profile) {
                profile.lastSeenAt = Date.now();
                messengerProfiles.set(appUserId, profile);
            }
        }
    }
    ws.__messengerAppUserId = '';
    broadcastMessengerPresence(appUserId);
}

function sendMessengerBootstrap(ws, appUserId) {
    const id = normalizeMessengerId(appUserId);
    if (!id) return;
    safeSend(ws, {
        type: 'messenger-ready',
        appUserId: id,
        chats: buildMessengerChatSummaries(id)
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

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const senderId = ws.__participantId || clientId;

            switch (data.type) {
                case 'ping':
                    safeSend(ws, { type: 'pong', ts: Date.now() });
                    break;

                case 'messenger-auth':
                    {
                        const appUserId = normalizeMessengerId(data.appUserId, 120);
                        if (!appUserId) {
                            safeSend(ws, { type: 'error', message: 'appUserId required' });
                            return;
                        }
                        const profile = registerMessengerSocket(
                            ws,
                            appUserId,
                            normalizeMessengerId(data.userName, 120),
                            normalizeMessengerId(data.userAvatar, 500),
                            normalizeMessengerId(data.username, 80)
                        );
                        if (!profile) {
                            safeSend(ws, { type: 'error', message: 'Неверный профиль мессенджера' });
                            return;
                        }
                        sendMessengerBootstrap(ws, appUserId);
                    }
                    break;

                case 'messenger-refresh':
                    {
                        const appUserId = normalizeMessengerId(data.appUserId || ws.__messengerAppUserId, 120);
                        if (!appUserId) return;
                        sendMessengerBootstrap(ws, appUserId);
                    }
                    break;

                case 'chat-open':
                    {
                        const appUserId = normalizeMessengerId(data.appUserId || ws.__messengerAppUserId, 120);
                        const peerAppUserId = normalizeMessengerId(data.peerAppUserId, 120);
                        if (!appUserId || !peerAppUserId || appUserId === peerAppUserId) return;
                        const peerProfile = messengerProfiles.get(peerAppUserId) || {};
                        const presence = getMessengerPresence(peerAppUserId);
                        safeSend(ws, {
                            type: 'chat-history',
                            peerAppUserId,
                            peerProfile: {
                                appUserId: peerAppUserId,
                                userName: peerProfile.userName || peerAppUserId,
                                userAvatar: peerProfile.userAvatar || '',
                                username: peerProfile.username || ''
                            },
                            online: presence.online,
                            lastSeenAt: presence.lastSeenAt,
                            messages: getMessengerThreadMessages(appUserId, peerAppUserId)
                        });
                    }
                    break;

                case 'chat-message':
                    {
                        const fromAppUserId = normalizeMessengerId(data.appUserId || ws.__messengerAppUserId, 120);
                        const toAppUserId = normalizeMessengerId(data.toAppUserId, 120);
                        const text = sanitizeChatText(data.text);
                        const clientMsgId = normalizeMessengerId(data.clientMsgId, 120);
                        if (!fromAppUserId || !toAppUserId || fromAppUserId === toAppUserId || !text) return;
                        const threadId = getMessengerThreadId(fromAppUserId, toAppUserId);
                        if (!threadId) return;
                        const messageItem = {
                            id: uuidv4(),
                            clientMsgId: clientMsgId || '',
                            threadId,
                            fromAppUserId,
                            toAppUserId,
                            text,
                            createdAt: Date.now()
                        };
                        if (!messengerThreads.has(threadId)) {
                            messengerThreads.set(threadId, { messages: [], updatedAt: Date.now() });
                        }
                        const thread = messengerThreads.get(threadId);
                        thread.messages.push(messageItem);
                        if (thread.messages.length > MAX_THREAD_MESSAGES) {
                            thread.messages.splice(0, thread.messages.length - MAX_THREAD_MESSAGES);
                        }
                        thread.updatedAt = Date.now();
                        messengerThreads.set(threadId, thread);
                        safeSend(ws, { type: 'chat-message-sent', message: messageItem });
                        safeSendToMessengerUser(toAppUserId, { type: 'chat-message', message: messageItem }, ws);
                        safeSendToMessengerUser(fromAppUserId, { type: 'chat-list-updated', chats: buildMessengerChatSummaries(fromAppUserId) });
                        safeSendToMessengerUser(toAppUserId, { type: 'chat-list-updated', chats: buildMessengerChatSummaries(toAppUserId) });
                    }
                    break;

                case 'chat-typing':
                    {
                        const fromAppUserId = normalizeMessengerId(data.appUserId || ws.__messengerAppUserId, 120);
                        const toAppUserId = normalizeMessengerId(data.toAppUserId, 120);
                        if (!fromAppUserId || !toAppUserId || fromAppUserId === toAppUserId) return;
                        safeSendToMessengerUser(toAppUserId, {
                            type: 'chat-typing',
                            fromAppUserId,
                            isTyping: !!data.isTyping,
                            createdAt: Date.now()
                        }, ws);
                    }
                    break;

                case 'create':
                case 'join':
                    currentRoom = data.roomId;
                    userName = data.userName;
                    const userAvatar = data.userAvatar || '';
                    const appUserId = typeof data.appUserId === 'string' ? data.appUserId.trim().slice(0, 80) : '';
                    if (appUserId) {
                        upsertMessengerProfile(appUserId, userName, userAvatar, '');
                    }
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
        if (ws.__superseded) return;
        unregisterMessengerSocket(ws);
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
