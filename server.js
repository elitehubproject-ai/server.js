const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const http = require('http');

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;

const wss = new WebSocket.Server({ host: '127.0.0.1', port: PORT });
const rooms = new Map();

console.log(`✅ WebSocket server running on ws://127.0.0.1:${PORT}`);

const healthServer = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('WebSocket server is running');
});
healthServer.listen(PORT + 1, '127.0.0.1', () => console.log(`Health check on http://127.0.0.1:${PORT + 1}`));

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

                    if (!rooms.has(currentRoom)) {
                        if (!isCreating) {
                            ws.send(JSON.stringify({ type: 'error', message: 'Комната не найдена' }));
                            return;
                        }
                        rooms.set(currentRoom, {
                            id: currentRoom,
                            participants: new Map()
                        });
                    }

                    const room = rooms.get(currentRoom);
                    const participantInfo = {
                        ws: ws,
                        userName: userName,
                        userAvatar: userAvatar,
                        video: false,
                        audio: true,
                        screen: false,
                        isAdmin: isCreating
                    };
                    
                    room.participants.forEach((p, id) => {
                        p.ws.send(JSON.stringify({
                            type: 'guest-joined',
                            guestName: userName,
                            guestAvatar: userAvatar,
                            guestId: clientId,
                            guestVideo: participantInfo.video,
                            guestAudio: participantInfo.audio
                        }));
                        
                        ws.send(JSON.stringify({
                            type: 'creator-info',
                            creatorName: p.userName,
                            creatorAvatar: p.userAvatar || '',
                            creatorId: id,
                            creatorVideo: p.video,
                            creatorAudio: p.audio,
                            isAdmin: p.isAdmin,
                            myId: clientId
                        }));
                    });

                    room.participants.set(clientId, participantInfo);
                    
                    if (isCreating) {
                        ws.send(JSON.stringify({ type: 'created', roomId: currentRoom, myId: clientId }));
                    }

                    console.log(`🏠 User ${userName} ${isCreating ? 'created' : 'joined'} room: ${currentRoom}`);
                    break;

                case 'signal':
                case 'screen-signal':
                case 'video-signal':
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        
                        if (data.target) {
                            const target = room.participants.get(data.target);
                            if (target) {
                                target.ws.send(JSON.stringify({ 
                                    ...data, 
                                    from: userName, 
                                    fromId: clientId 
                                }));
                            }
                        } else {
                            room.participants.forEach((p, id) => {
                                if (id !== clientId) {
                                    p.ws.send(JSON.stringify({ 
                                        ...data, 
                                        from: userName, 
                                        fromId: clientId 
                                    }));
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

                        const msgType = data.type === 'start-screen' ? 'screen-started' :
                                      data.type === 'stop-screen' ? 'screen-stopped' :
                                      data.type === 'toggle-video' ? 'video-toggle' :
                                      data.type === 'toggle-audio' ? 'audio-toggle' : data.type;

                        room.participants.forEach((participant, id) => {
                            if (id !== clientId) {
                                participant.ws.send(JSON.stringify({ 
                                    ...data,
                                    type: msgType,
                                    from: userName, 
                                    fromId: clientId 
                                }));
                            }
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
                    {
                        const room = rooms.get(currentRoom);
                        if (!room) return;
                        const sender = room.participants.get(clientId);
                        if (!sender) return;

                        room.participants.forEach((p, id) => {
                            if (id !== clientId) {
                                p.ws.send(JSON.stringify({ 
                                    ...data, 
                                    from: userName, 
                                    fromId: clientId 
                                }));
                            }
                        });
                    }
                    break;

                case 'leave':
                    handleDisconnect(clientId, currentRoom);
                    break;
            }
        } catch (error) {
            console.error('Error:', error);
        }
    });

    ws.on('close', () => {
        handleDisconnect(clientId, currentRoom);
    });
});

function handleDisconnect(clientId, roomId) {
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    const participant = room.participants.get(clientId);
    if (!participant) return;

    console.log(`❌ User left: ${participant.userName} from room ${roomId}`);
    
    room.participants.delete(clientId);
    
    if (room.participants.size === 0) {
        rooms.delete(roomId);
        console.log(`🏠 Room closed: ${roomId}`);
    } else {
        room.participants.forEach((p) => {
            p.ws.send(JSON.stringify({ 
                type: 'guest-left', 
                from: participant.userName,
                fromId: clientId 
            }));
        });
    }
}
