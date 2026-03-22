const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const http = require('http');

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;

// ВАЖНО: для Render нужно слушать на 0.0.0.0, а не на 127.0.0.1
const wss = new WebSocket.Server({ host: '0.0.0.0', port: PORT });
const rooms = new Map();

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

function serializeParticipant(id, participant) {
    return {
        id,
        userName: participant.userName,
        userAvatar: participant.userAvatar || '',
        video: !!participant.video,
        audio: !!participant.audio,
        screen: !!participant.screen,
        isAdmin: !!participant.isAdmin
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
            pendingJoinRequests
        });
    });
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

wss.on('connection', (ws) => {
    const clientId = uuidv4();
    console.log(`📱 Client connected: ${clientId.substring(0, 8)}`);

    let currentRoom = null;
    let userName = '';

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                case 'create':
                case 'join':
                    currentRoom = data.roomId;
                    userName = data.userName;
                    const userAvatar = data.userAvatar || '';
                    const isCreating = data.type === 'create';

                    if (!currentRoom || typeof currentRoom !== 'string') {
                        safeSend(ws, { type: 'error', message: 'Некорректный идентификатор комнаты' });
                        return;
                    }

                    if (!rooms.has(currentRoom)) {
                        rooms.set(currentRoom, { id: currentRoom, participants: new Map(), joinRequests: new Map(), ownerId: null, watchParty: null, isPrivate: false });
                    }
                    const room = rooms.get(currentRoom);
                    if (isCreating && room.participants.size > 0) {
                        safeSend(ws, { type: 'error', message: 'Комната уже используется' });
                        return;
                    }
                    if (!isCreating && room.isPrivate && room.participants.size > 0) {
                        const request = {
                            id: clientId,
                            ws,
                            userName,
                            userAvatar,
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

                    const participantInfo = {
                        ws: ws,
                        userName: userName,
                        userAvatar: userAvatar,
                        video: false,
                        audio: true,
                        screen: false,
                        isAdmin: false
                    };

                    room.participants.set(clientId, participantInfo);
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
                            ownerId: room.ownerId
                        });
                    });

                    safeSend(ws, {
                        type: isCreating ? 'created' : 'joined',
                        roomId: currentRoom,
                        myId: clientId,
                        ownerId: room.ownerId
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
                                    fromId: clientId 
                                });
                            }
                        } else {
                            room.participants.forEach((p, id) => {
                                if (id !== clientId) {
                                    safeSend(p.ws, { 
                                        ...data, 
                                        from: userName, 
                                        fromId: clientId 
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
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        const p = room.participants.get(clientId);
                        if (!p) return;

                        if (data.type === 'start-screen') p.screen = true;
                        if (data.type === 'stop-screen') p.screen = false;
                        if (data.type === 'toggle-video') p.video = data.enabled;
                        if (data.type === 'toggle-audio') p.audio = data.enabled;

                        room.participants.forEach((participant, id) => {
                            if (id !== clientId) {
                                safeSend(participant.ws, {
                                    ...data,
                                    from: userName,
                                    fromId: clientId
                                });
                            }
                        });

                        room.participants.forEach((participant, id) => {
                            if (id === clientId) return;
                            safeSend(participant.ws, {
                                type: 'participant-updated',
                                participantId: clientId,
                                ownerId: room.ownerId,
                                changes: {
                                    video: p.video,
                                    audio: p.audio,
                                    screen: p.screen
                                }
                            });
                        });
                    }
                    break;

                case 'request-video':
                case 'request-audio':
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
                        const sender = room.participants.get(clientId);
                        if (!sender) return;
                        const senderIsOwner = room.ownerId === clientId;
                        const senderIsAdmin = !!sender.isAdmin;

                        if (data.type === 'set-room-private') {
                            if (!senderIsOwner && !senderIsAdmin) return;
                            room.isPrivate = !!data.enabled;
                            room.participants.forEach((participant) => {
                                safeSend(participant.ws, {
                                    type: 'room-privacy-updated',
                                    enabled: room.isPrivate,
                                    fromId: clientId,
                                    from: userName
                                });
                            });
                            broadcastRoomState(room);
                            return;
                        }

                        if (data.type === 'close-room') {
                            if (!senderIsOwner && !senderIsAdmin) return;
                            closeRoom(room, clientId, userName);
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
                                    byId: clientId,
                                    byName: userName
                                });
                                return;
                            }

                            const participantInfo = {
                                ws: request.ws,
                                userName: request.userName,
                                userAvatar: request.userAvatar || '',
                                video: false,
                                audio: true,
                                screen: false,
                                isAdmin: false
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
                                    ownerId: room.ownerId
                                });
                            });
                            safeSend(request.ws, {
                                type: 'joined',
                                roomId: room.id,
                                myId: requestId,
                                ownerId: room.ownerId
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
                                    fromId: clientId
                                });
                            });
                            broadcastRoomState(room);
                            return;
                        }

                        if (data.type === 'kick') {
                            safeSend(target.ws, {
                                type: 'kicked',
                                from: userName,
                                fromId: clientId
                            });
                            try { target.ws.close(); } catch (_) {}
                            return;
                        }

                        safeSend(target.ws, {
                            ...data,
                            targetId,
                            from: userName,
                            fromId: clientId
                        });
                    }
                    break;

                case 'cancel-join-request':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        if (!room.joinRequests.has(clientId)) return;
                        room.joinRequests.delete(clientId);
                        broadcastJoinRequestToModerators(room, {
                            type: 'join-request-cancelled',
                            requestId: clientId,
                            byModerator: false
                        });
                    }
                    break;

                case 'start-watch':
                case 'stop-watch':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        const sender = room.participants.get(clientId);
                        if (!sender) return;

                        const senderIsOwner = room.ownerId === clientId;
                        const senderIsAdmin = !!sender.isAdmin;

                        if (data.type === 'start-watch') {
                            const url = String(data.url || '').trim();
                            if (!url) return;

                            const active = room.watchParty;
                            const canStart = !active || active.ownerId === clientId || senderIsOwner || senderIsAdmin;
                            if (!canStart) return;

                            room.watchParty = {
                                url,
                                ownerId: clientId,
                                ownerName: userName,
                                startedAt: Date.now()
                            };

                            room.participants.forEach((participant) => {
                                safeSend(participant.ws, {
                                    type: 'watch-started',
                                    watchParty: room.watchParty,
                                    from: userName,
                                    fromId: clientId,
                                    ownerId: room.ownerId
                                });
                            });
                            return;
                        }

                        if (!room.watchParty) return;
                        const canStop = room.watchParty.ownerId === clientId || senderIsOwner || senderIsAdmin;
                        if (!canStop) return;

                        const previousWatch = room.watchParty;
                        room.watchParty = null;
                        room.participants.forEach((participant) => {
                            safeSend(participant.ws, {
                                type: 'watch-stopped',
                                previousWatch,
                                from: userName,
                                fromId: clientId,
                                ownerId: room.ownerId
                            });
                        });
                    }
                    break;

                case 'leave':
                    {
                        const roomToLeave = currentRoom;
                        currentRoom = null;
                        handleDisconnect(clientId, roomToLeave);
                    }
                    break;
            }
        } catch (error) {
            console.error('Error:', error);
        }
    });

    ws.on('close', () => {
        const roomToLeave = currentRoom;
        currentRoom = null;
        handleDisconnect(clientId, roomToLeave);
    });
});

function handleDisconnect(clientId, roomId) {
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
        if (room.participants.size === 0 && room.joinRequests.size === 0) {
            rooms.delete(roomId);
            console.log(`🏠 Room closed: ${roomId}`);
        }
        return;
    }

    const participant = room.participants.get(clientId);
    if (!participant) return;

    console.log(`❌ User left: ${participant.userName} from room ${roomId}`);
    
    room.participants.delete(clientId);
    const shouldStopWatch = room.watchParty && room.watchParty.ownerId === clientId;
    if (shouldStopWatch) {
        room.watchParty = null;
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
