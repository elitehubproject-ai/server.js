const F12_CONSOLE_MESSAGE = 'Кажется здесь нет ничего, зачем Вы сюда зашли?)';
        if (typeof console !== 'undefined') {
            const consoleMethods = ['log', 'info', 'warn', 'error', 'debug', 'trace'];
            for (const methodName of consoleMethods) {
                try {
                    const originalMethod = console[methodName];
                    console[methodName] = function (...args) {
                        if (typeof originalMethod === 'function') {
                            try {
                                originalMethod.call(console, F12_CONSOLE_MESSAGE);
                            } catch (_) {
                                // ignore
                            }
                        }
                    };
                } catch (_) {
                    // ignore
                }
            }
        }

        const WS_URL = 'wss://server-js-qenx.onrender.com';
        // Free hosting providers can auto-delete large files with SW/Push patterns.
        // Keep messenger stable by disabling those integrations in one-file build.
        const HOSTING_SAFE_MODE = true;
        const API_BASE = `${window.location.origin}${getBasePath().replace(/\/$/, '')}`;
        const TELEGRAM_INVITE_API = `${API_BASE}/backend/telegram_send_invite.php`;
        const TELEGRAM_CONTACTS_API = `${API_BASE}/backend/telegram_contacts.php`;
        let FRIENDS_API = '';
        try {
            FRIENDS_API = new URL('backend/friends_api.php', window.location.href).toString();
        } catch (_) {
            FRIENDS_API = `${API_BASE}/backend/friends_api.php`;
        }
        const FRIENDS_API_FALLBACKS = (() => {
            const list = [];
            const push = (value) => {
                const s = String(value || '').trim();
                if (!s || list.includes(s)) return;
                list.push(s);
            };
            try {
                const saved = String(localStorage.getItem('seych-friends-api-url') || '').trim();
                if (saved) push(saved);
            } catch (_) {}
            push(FRIENDS_API);
            try {
                const base = `${window.location.origin}${getBasePath().replace(/\/$/, '')}/`;
                push(new URL('backend/friends_api.php', base).toString());
            } catch (_) {}
            try {
                push(new URL('backend/friends_api.php', `${window.location.origin}/`).toString());
            } catch (_) {}
            push(`${API_BASE}/backend/friends_api.php`);
            return list;
        })();
        const LINK_PREVIEW_API = `${API_BASE}/backend/link_preview.php`;
        const VK_PROXY_API = `${API_BASE}/backend/vk_proxy.php`;
        const AVATAR_PROXY_API = `${API_BASE}/backend/vk_proxy.php?avatar=1&url=`;
        let QR_AUTH_API = '';
        try {
            QR_AUTH_API = new URL('backend/qr_auth_api.php', window.location.href).toString();
        } catch (_) {
            QR_AUTH_API = `${API_BASE}/backend/qr_auth_api.php`;
        }
        const DEVICE_SESSION_KEY = 'seych-device-session-id';
        let qrAuthPollTimer = null;
        let qrAuthCurrentToken = '';
        let qrScannerInstance = null;
        let authShowClassicProviders = false;
        let deviceSessionWatchTimer = null;
        const TELEGRAM_BOT_USERNAME = 'seych_call_bot';
        const VK_CLIENT_ID = '54525607';
        const VK_REDIRECT_URL = 'https://seych-call.gt.tc';
        const VK_API_VERSION = '5.131';
        const RECONNECT_KEY_STORAGE = 'vk_call_reconnect_key';
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
        
        let roomId, localStream, ws;
        let isCreator = false, userName = '', remoteName = '';
        let userAvatar = '', remoteAvatar = '';
        let videoEnabled = false, audioEnabled = true;
        let remoteVideo = false, remoteAudio = true;
        let isScreenSharing = false, remoteScreen = false;
        let isSpeaking = false, remoteSpeaking = false;
        let animationId;
        let callStartTime = null;
        let callTimerInterval = null;
        let isGuestAdmin = false;
        let isConnected = false;
        let myId = null;
        
        let peers = new Map();
        let participants = new Map();
        let participantAvatars = new Map();
        let participantStates = new Map();
        let screenConnMap = new Map();
        let localScreenShareId = null;
        let ownerId = null;
        let currentContextTargetId = null;

        let videoTiles = new Map();
        let screenTiles = new Map();
        let remoteMediaStreams = new Map();
        let remoteAudioEls = new Map();
        let audioContextRef = null;
        let screenStreamLocal = null;
        
        let videoTrack = null;
        let cameraSourceTrack = null;
        let selfPreviewTrack = null;
        let outgoingTrackCleanup = null;
        let cameraFacingMode = 'user';
        let cameraSwitchInProgress = false;
        let videoPrewarmPromise = null;
        let authProfile = null;
        let pendingRoomJoin = null;
        let pendingQrLoginToken = '';
        let appUserId = '';
        let telegramContacts = [];
        let vkContacts = [];
        let vkCustomContacts = [];
        let vkHiddenContactIds = [];
        let friendsState = { friends: [], incomingRequests: [], outgoingRequests: [], incomingCalls: [], outgoingCalls: [] };
        let friendsSearchResults = [];
        let friendsSearchValue = '';
        let friendsActiveTab = 'friends';
        let friendsPanelOpenMobile = false;
        let friendsNotificationsEnabled = true;
        let systemNotifyPermissionAsked = false;
        let pushRegistration = null;
        let pushInitPromise = null;
        let friendsPollTimer = null;
        let incomingFriendModal = null;
        let incomingCallModal = null;
        let incomingCallSound = null;
        let incomingCallSoundRetryTimer = null;
        let knownIncomingCallIds = new Set();
        let knownOutgoingCallStatuses = new Map();
        let outgoingFriendCallSession = null;
        let outgoingFriendCallTimeout = null;
        let incomingCallAutoDeclineTimeout = null;
        let audioPlaybackUnlocked = false;
        
        // Call audio settings.
        let echoCancellationEnabled = true;
        let autoGainControlEnabled = true;
        let connectingAudioParticipants = new Set();
        let selectedMicDeviceId = '';
        let selectedSpeakerDeviceId = '';
        let rawMicTrack = null;
        let watchPartyState = null;
        let durakGameState = null;
        let durakUiTickTimer = null;
        let durakShowdownDismissed = false;
        let durakDragCard = null;
        let watchPartyTile = null;
        let watchPartyMediaElement = null;
        let watchPartySupportsVolume = false;
        let watchPartyVolume = 80;
        let watchPartyVolumeApplier = null;
        let watchFocusEnabled = false;
        let watchFocusIdleTimer = null;
        let roomIsPrivate = false;
        let pendingJoinRequests = [];
        let joinPendingModal = null;
        let roomSettingsMenu = null;
        let participantConnectionQuality = new Map();
        let connectionQualityBusy = false;
        let rtcIceServers = [...DEFAULT_ICE_SERVERS];
        let avPeerRecoverTimers = new Map();
        let iceRestartTimers = new Map();
        let connectionQualityTimer = null;
        const WS_SEND_BUFFER_HIGH_WATER = 256 * 1024;
        let wsReconnectTimer = null;
        let wsReconnectAttempts = 0;
        let wsLastInitialMsg = null;
        let wsConnectSessionId = 0;
        let currentWsType = '';
        let wsLastActivityAt = 0;
        let wsLastPingAt = 0;
        let wsLastPongAt = 0;
        let wsLastForcedReconnectAt = 0;
        const EMPTY_CHAT_PHRASES = [
            'Похоже здесь пусто, как думаете этому пользователю не одиноко?',
            'Здесь же совсем ничего нет, хотите начать новую историю?',
            'К сожалению, здесь вообще ничего нет, сделаем легендарное дуо?',
            'Тишина… А ведь первое слово может изменить всё',
            'Пусто, как в космосе. Запустим диалог?',
            'Ни одного сообщения. Пока не поздно — начните!',
            'Этот чат ждёт своего героя. Им будете вы?',
            'Пустой чат — чистый лист. Напишите первую главу!',
            'Здесь так тихо, что слышно эхо. Скажите что-нибудь!',
            'Ноль сообщений. Это вызов — примете его?',
            'Ваш шанс начать что-то великое прямо сейчас',
            'Диалог ещё не начался. Будьте первопроходцем!',
            'Пустота — это возможность. Воспользуйтесь ей!',
            'Кто-то должен написать первым. Почему не вы?',
            'Этот чат ещё спит. Разбудите его сообщением!',
            'Ни единого слова. Самое время стать первым!',
            'Пустой чат — как необитаемый остров. Высадимся?',
            'Здесь нет ни одного сообщения… пока что!',
            'Молчание — золото, но сообщения — бесценны',
            'Чат пуст. Но не для вас, правда?',
            'Здесь можно начать что-то особенное',
            'Первое сообщение — как первый шаг на луну',
            'Пока тут пусто, но вы можете это изменить',
            'Чат без сообщений — как вечеринка без гостей',
            'Одиноко тут… Напишите что-нибудь!',
            'Пусто, но потенциал безграничен. Начнём?',
            'Здесь только вы и тишина. Побейте её!',
            'Ни одного сообщения — это вызов судьбе!',
            'Чат пуст, но зато весь ваш. Напишите!',
            'Первое сообщение — самый сложный шаг. Попробуйте!',
            'Пустой чат — как незаполненный холст. Рискуйте!',
            'Тут ничего нет, разорвите эту тишину!',
            'Молчание — не всегда знак согласия. Напишите!',
            'Чат ждёт вашего первого слова. Не заставляйте ждать!',
            'Пустота здесь — временна. Начните диалог!',
            'Ни единого сообщения. Это ваш момент!',
            'Пустой чат — приглашение к действию',
            'Тишина здесь оглушительна. Скажите что-нибудь!',
            'Этот чат — чистый лист. Что вы напишете?',
            'Ноль сообщений — это просто начало чего-то великого',
            'Пусто? Значит, вы можете быть первым во всём!',
            'Чат без истории. Создайте её прямо сейчас!',
            'Здесь пока ничего нет, но всё в ваших руках',
            'Первый шаг — самое сложное. Но вы справитесь!',
            'Пустой чат — это не конец, это начало!',
            'Тишина здесь — ваш шанс заговорить первым',
            'Ни слова ещё не сказано. Будьте первым!',
            'Пустота — это свобода. Напишите что хотите!',
            'Чат пуст, но ваше сообщение всё изменит',
            'Здесь пока тихо. Подкиньте искру!',
            'Пустой чат — как книга без единой буквы. Напишите первую!',
            'Сюда ещё не долетело ни одно сообщение. Отправьте!',
            'Вакуум. Но вы можете его нарушить!',
            'Тут пусто, но зато никакого спама!',
            'Чат свежий, как утренний воздух. Напишите!',
            'Ни одного сообщения — и это ваш шанс быть первым',
            'Пустота зовёт. Ответьте ей сообщением!',
            'Этот диалог ещё не начался. Станьте инициатором!',
            'Пустой чат — как неразгаданная тайна. Разгадайте!',
            'Тишина — это скучно. Нарушьте её!',
            'Здесь ничего нет, но вы можете создать всё',
            'Чат без сообщений — как концерт без музыки',
            'Первое слово — самое важное. Начните!',
            'Пусто? Это просто сцена без актёров. Выходите!',
            'Ни единого сообщения. Время менять ситуацию!',
            'Чат пуст, но потенциал огромный. Действуйте!',
            'Здесь пока только эхо. Скажите что-то!',
            'Пустой чат — ваш холст. Творите!',
            'Ноль сообщений — это ноль ограничений!',
            'Молчание — это уютно, но диалог — это жизнь',
            'Пустота здесь — временная. Начните!',
            'Чат ждёт первого сообщения как рассвет',
            'Ни слова. Это ваш шанс написать легенду!',
            'Пустой чат — как старый телефон. Позвоните!',
            'Тишина здесь — не навсегда. Напишите!',
            'Чат пуст, но ваше слово наполнит его смыслом',
            'Здесь ничего нет. Пока вы не решите иначе!',
            'Первое сообщение — как ключ к двери. Откройте!',
            'Пустота — это начало всех великих историй',
            'Ни одного сообщения. Но это легко исправить!',
            'Чат без слов — как море без волн. Встряхните!',
            'Пусто, но это не приговор. Это приглашение!',
            'Тишина здесь — ваш друг. Но диалог — лучше!',
            'Ноль сообщений. Ноль проблем. Начните общение!',
            'Пустой чат — как замок без ключа. Вы — ключ!',
            'Здесь пока тихо, но громкость в ваших руках',
            'Чат пуст. Но первое слово всё изменит!',
            'Ни единого сообщения. Это ваш звёздный час!',
            'Пустота — это чистый старт. Поехали!',
            'Молчание — это пауза перед великим диалогом',
            'Чат без сообщений — как небо без звёзд. Зажгите!',
            'Пусто? Отлично! Никаких стереотипов!',
            'Первое сообщение — как первый кирпич. Стройте!',
            'Здесь ничего нет, и это прекрасно. Свобода!',
            'Чат пуст, но ваше сообщение станет легендарным',
            'Тишина — это момент перед бурей. Создайте бурю!',
            'Ни слова ещё. Но вы можете изменить это!',
            'Пустой чат — как пустая сцена. Ваш выход!',
            'Здесь пусто, но ваша искра зажжёт диалог!'
        ];

        let emptyChatPhraseIndex = Math.floor(Math.random() * EMPTY_CHAT_PHRASES.length);
        let emptyChatCurrentPhrase = EMPTY_CHAT_PHRASES[emptyChatPhraseIndex];
        let emptyChatPhraseTimer = null;
        let emptyChatPhraseFading = false;

        function animatePhraseTransition() {
            const el = document.getElementById('emptyChatPhrase');
            if (!el || emptyChatPhraseFading) return;
            emptyChatPhraseFading = true;
            el.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
            el.style.opacity = '0';
            el.style.transform = 'translateY(-8px)';
            setTimeout(() => {
                emptyChatPhraseIndex = (emptyChatPhraseIndex + 1) % EMPTY_CHAT_PHRASES.length;
                emptyChatCurrentPhrase = EMPTY_CHAT_PHRASES[emptyChatPhraseIndex];
                el.textContent = emptyChatCurrentPhrase;
                el.style.transition = 'none';
                el.style.opacity = '0';
                el.style.transform = 'translateY(8px)';
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => {
                        el.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
                        el.style.opacity = '0.88';
                        el.style.transform = 'translateY(0)';
                        setTimeout(() => { emptyChatPhraseFading = false; }, 650);
                    });
                });
            }, 520);
        }

        function startEmptyChatPhraseRotation() {
            if (emptyChatPhraseTimer) return;
            emptyChatPhraseTimer = setInterval(() => {
                const el = document.getElementById('emptyChatPhrase');
                if (!el) { stopEmptyChatPhraseRotation(); return; }
                animatePhraseTransition();
            }, 5000);
        }

        function stopEmptyChatPhraseRotation() {
            if (emptyChatPhraseTimer) { clearInterval(emptyChatPhraseTimer); emptyChatPhraseTimer = null; }
        }

        function getInitialEmptyChatPhrase() {
            return emptyChatCurrentPhrase;
        }

        function openBlacklistModal() {
            const bl = messengerProfile.blacklist || [];
            const listHtml = bl.length
                ? bl.map((bid) => {
                    const b = String(bid || '');
                    const dn = resolvePeerDisplay(b).displayName || b;
                    return `<div class="contact-item"><div class="contact-name">${escapeHtml(dn)}</div><div class="contact-name" style="font-size:11px;opacity:.65;">${escapeHtml(b)}</div><button class="contact-btn delete" onclick="removeUserFromBlacklist('${escapeHtml(b)}'); openBlacklistModal()"><i class="fas fa-times"></i></button></div>`;
                }).join('')
                : '<div class="friends-empty">Черный список пуст</div>';
            const overlay = document.createElement('div');
            overlay.className = 'blacklist-modal-overlay';
            overlay.id = 'blacklistModalOverlay';
            overlay.onclick = (e) => { if (e.target === overlay) closeBlacklistModal(); };
            overlay.innerHTML = `
                <div class="blacklist-modal">
                    <div class="blacklist-modal-header">
                        <h3>Черный список</h3>
                        <button type="button" class="blacklist-modal-close" onclick="closeBlacklistModal()"><i class="fas fa-times"></i></button>
                    </div>
                    <div class="blacklist-modal-list">${listHtml}</div>
                </div>`;
            document.body.appendChild(overlay);
        }

        function closeBlacklistModal() {
            const overlay = document.getElementById('blacklistModalOverlay');
            if (overlay) overlay.remove();
        }

        let wsReconnectInProgress = false;
        let reconnectKey = '';
        let detectLoopTimer = null;
        let wsHeartbeatTimer = null;
        let callAudioHealTimer = null;
        let joinSoundEffect = null;
        let leaveSoundEffect = null;
        let kickSoundEffect = null;
        let audioRecoverCooldown = new Map();
        let connectionNoticeCooldown = new Map();
        let pendingLegacyAppUserId = '';
        let startupLoaderTicker = null;
        let messengerView = 'chats';
        let messengerChats = [];
        let messengerAutoResyncTimer = null;
        let messengerAutoResyncAttempts = 0;
        const messengerLinkPreviewCache = new Map();
        const messengerLinkPreviewPromises = new Map();
        let messengerActiveChatId = '';
        let messengerActivePeerId = '';
        let messengerMessages = new Map();
        let messengerTypingByUser = new Map();
        let messengerTypingTimersByUser = new Map();
        let messengerTypingByChat = new Map();
        let messengerTypingTimersByChat = new Map();
        const pendingMentionProfileOpens = new Set();
        // Счётчик непрочитанных сообщений в сайдбаре (на каждую беседу).
        const messengerUnreadCounts = new Map();
        // Чтобы не накручивать счётчик повторно при приходе апдейтов/замен по id.
        const messengerUnreadMessageIds = new Set();
        const messengerReadAckedMessageIds = new Set();
        // Скролл истории: ставим в true только когда надо автопрокрутить вниз.
        let messengerShouldAutoScroll = true;

        function scheduleMessengerAutoResync(reason = '') {
            if (messengerAutoResyncTimer) return;
            if (messengerAutoResyncAttempts >= 6) return;
            const delay = Math.min(12000, Math.floor(900 * Math.pow(1.9, messengerAutoResyncAttempts)));
            messengerAutoResyncAttempts += 1;
            messengerAutoResyncTimer = setTimeout(() => {
                messengerAutoResyncTimer = null;
                if (!getMessengerSocketReady()) {
                    scheduleMessengerAutoResync(reason);
                    return;
                }
                sendMessengerEvent({ type: 'messenger-sync' });
            }, delay);
        }

        function clearMessengerAutoResync() {
            if (messengerAutoResyncTimer) {
                clearTimeout(messengerAutoResyncTimer);
                messengerAutoResyncTimer = null;
            }
            messengerAutoResyncAttempts = 0;
        }
        function getMessengerUnreadForChat(chatId) {
            return messengerUnreadCounts.get(chatId) || 0;
        }
        function setMessengerUnreadForChat(chatId, value) {
            const id = String(chatId || '');
            if (!id) return;
            const v = Math.max(0, Number(value) || 0);
            if (v) messengerUnreadCounts.set(id, v);
            else messengerUnreadCounts.delete(id);
        }
        function getMessengerUnreadTotal() {
            let sum = 0;
            messengerUnreadCounts.forEach((v) => {
                sum += Math.max(0, Number(v) || 0);
            });
            return sum;
        }

        function syncChatLastMessagePreviewFromMessages(chatId) {
            const cid = String(chatId || '').trim();
            if (!cid) return;
            const msgs = messengerMessages.get(cid) || [];
            const chat = findMessengerChatById(cid);
            const frozenAt = Math.max(0, Number(getGroupLeaveStateClient(chat, authProfile?.appUserId || '')?.frozenAt || 0)) || 0;
            const last = [...msgs].reverse().find((m) => {
                if (!m || m.deletedAt) return false;
                if (frozenAt && Number(m.createdAt || 0) > frozenAt) return false;
                return true;
            }) || null;
            const previewTextFromMessage = (m) => {
                if (!m) return '';
                const kind = String(m.messageKind || '');
                if (kind === 'system') return String(m.text || '');
                const groupEvent = parseGroupEventPayload(m.text || '');
                if (groupEvent) {
                    const t = String(groupEvent.type || '').trim();
                    if (t === 'group-call-ended') return 'Звонок завершён';
                    if (t === 'group-call-created') return 'Групповой звонок';
                    const title = String(groupEvent.title || '').trim();
                    return title || 'Событие';
                }
                return String(m.text || '');
            };
            const preview = last
                ? {
                      id: last.id,
                      text: previewTextFromMessage(last),
                      fromId: last.fromId || '',
                      createdAt: Number(last.createdAt || 0) || Date.now(),
                      editedAt: Number(last.editedAt || 0) || 0,
                      messageKind: last.messageKind || 'text',
                      audioBase64: ''
                  }
                : null;
            messengerChats = (messengerChats || []).map((c) => {
                if (String(c.id || '') !== cid) return c;
                let nextUpdatedAt = preview ? Number(preview.createdAt || Date.now()) : Number(c.updatedAt || 0);
                if (frozenAt) nextUpdatedAt = Math.min(nextUpdatedAt || frozenAt, frozenAt);
                return { ...c, lastMessage: preview, updatedAt: nextUpdatedAt };
            });
            messengerChats = (messengerChats || []).slice().sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
        }

        function getChatHistoryDistFromBottom() {
            const hist = document.querySelector('.chat-history');
            if (!hist) return 0;
            return hist.scrollHeight - hist.scrollTop - hist.clientHeight;
        }

        function updateMessengerNewWhileScrolledFabUI() {
            const wrap = document.getElementById('scrollToBottomFabWrap');
            const badge = document.getElementById('scrollToBottomFabBadge');
            if (!wrap || !badge) return;
            const c = Math.max(0, Number(messengerNewWhileScrolledCount) || 0);
            const dist = getChatHistoryDistFromBottom();
            const shouldShow = dist > 120;
            wrap.style.display = shouldShow ? 'flex' : 'none';
            if (c > 0) {
                badge.textContent = c > 99 ? '99+' : String(c);
                badge.style.display = 'flex';
            } else {
                badge.textContent = '0';
                badge.style.display = 'none';
            }
        }

        function scrollMessengerHistoryToBottom() {
            const hist = document.querySelector('.chat-history');
            if (!hist) return;
            messengerNewWhileScrolledCount = 0;
            updateMessengerNewWhileScrolledFabUI();
            try {
                hist.scrollTop = hist.scrollHeight;
            } catch (_) {}
            updateMessengerNewWhileScrolledFabUI();
        }
        const messengerMentionUnreadCounts = new Map();
        const messengerPendingMentionIdsByChat = new Map();
        let messengerMentionWhileScrolledCount = 0;
        let messengerMentions = [];
        let messengerNotifications = [];
        const messengerNotificationUnreadIds = new Set();
        const MESSENGER_NOTIFICATIONS_STORAGE_PREFIX = 'seych-messenger-notifications:';
        let messengerAppearance = { theme: 'classic', chatWallpaper: '', chatWallpaperBlur: true };
        let profileUsernameCheckTimer = null;
        let profileUsernameCheckSeq = 0;

        function getMessengerNotificationsStorageKey() {
            const userId = String(authProfile?.appUserId || appUserId || '').trim();
            return userId ? `${MESSENGER_NOTIFICATIONS_STORAGE_PREFIX}${userId}` : '';
        }

        function persistMessengerNotifications() {
            const key = getMessengerNotificationsStorageKey();
            if (!key) return;
            try {
                localStorage.setItem(key, JSON.stringify({
                    notifications: Array.isArray(messengerNotifications) ? messengerNotifications.slice(0, 300) : [],
                    unreadIds: Array.from(messengerNotificationUnreadIds)
                }));
            } catch (_) {}
        }

        function loadMessengerNotifications() {
            const key = getMessengerNotificationsStorageKey();
            if (!key) return;
            try {
                const raw = localStorage.getItem(key);
                if (!raw) return;
                const parsed = JSON.parse(raw);
                messengerNotifications = Array.isArray(parsed?.notifications) ? parsed.notifications : [];
                messengerNotificationUnreadIds.clear();
                (Array.isArray(parsed?.unreadIds) ? parsed.unreadIds : []).forEach((id) => {
                    const safeId = String(id || '').trim();
                    if (safeId) messengerNotificationUnreadIds.add(safeId);
                });
            } catch (_) {}
        }

        function loadMessengerTheme() {
            messengerAppearance = messengerAppearance && typeof messengerAppearance === 'object'
                ? messengerAppearance
                : { theme: 'classic', chatWallpaper: '', chatWallpaperBlur: true };
            messengerAppearance.theme = messengerAppearance.theme === 'dark' ? 'dark' : 'classic';
            applyMessengerTheme();
        }

        function applyMessengerTheme() {
            if (messengerAppearance.theme === 'dark') {
                document.body.setAttribute('data-theme', 'dark');
                return;
            }
            document.body.removeAttribute('data-theme');
        }

        function setMessengerTheme(nextTheme) {
            messengerAppearance.theme = nextTheme === 'dark' ? 'dark' : 'classic';
            applyMessengerTheme();
            sendMessengerEvent({
                type: 'messenger-update-appearance',
                theme: messengerAppearance.theme
            });
            if (shouldRenderMessengerUi()) renderMainScreen();
        }

        function setMessengerChatWallpaper(nextWallpaperDataUrl) {
            messengerAppearance.chatWallpaper = String(nextWallpaperDataUrl || '').trim();
            sendMessengerEvent({
                type: 'messenger-update-appearance',
                chatWallpaper: messengerAppearance.chatWallpaper
            });
            if (shouldRenderMessengerUi()) renderMainScreen();
        }

        function setMessengerChatWallpaperBlur(enabled) {
            messengerAppearance.chatWallpaperBlur = enabled !== false;
            sendMessengerEvent({
                type: 'messenger-update-appearance',
                chatWallpaperBlur: !!messengerAppearance.chatWallpaperBlur
            });
            if (shouldRenderMessengerUi()) renderMainScreen();
        }

        function getMessengerNotificationUnreadTotal() {
            return messengerNotificationUnreadIds.size;
        }

        function markMessengerNotificationsRead() {
            messengerNotificationUnreadIds.clear();
            persistMessengerNotifications();
        }

        function getMessengerNotificationChatMeta(chatId) {
            const chat = findMessengerChatById(chatId);
            const title = String(chat?.peer?.displayName || chat?.peer?.name || chatId || 'Чат').trim() || 'Чат';
            return {
                chatId: String(chatId || '').trim(),
                chatTitle: title,
                chatAvatar: String(chat?.peer?.avatar || '').trim(),
                chatInitials: String(chat?.peer?.initials || '').trim() || title.split(/\s+/).filter(Boolean).map((part) => part.charAt(0)).join('').slice(0, 2).toUpperCase()
            };
        }

        function pushMessengerNotification(item, opts = {}) {
            const normalized = item && typeof item === 'object' ? item : {};
            const id = String(normalized.id || '').trim();
            if (!id) return;
            const createdAt = Number(normalized.createdAt || 0) || Date.now();
            const entry = {
                id,
                type: String(normalized.type || 'info').trim() || 'info',
                chatId: String(normalized.chatId || '').trim(),
                chatTitle: String(normalized.chatTitle || '').trim() || 'Чат',
                chatAvatar: String(normalized.chatAvatar || '').trim(),
                chatInitials: String(normalized.chatInitials || '').trim(),
                actorId: String(normalized.actorId || '').trim(),
                actorName: String(normalized.actorName || '').trim() || 'Пользователь',
                actorAvatar: String(normalized.actorAvatar || '').trim(),
                actorInitials: String(normalized.actorInitials || '').trim(),
                messageId: String(normalized.messageId || '').trim(),
                title: String(normalized.title || '').trim() || 'Уведомление',
                text: String(normalized.text || '').trim(),
                duration: String(normalized.duration || '').trim(),
                reason: String(normalized.reason || '').trim(),
                createdAt
            };
            const prevIdx = messengerNotifications.findIndex((it) => String(it?.id || '') === id);
            if (prevIdx >= 0) {
                messengerNotifications[prevIdx] = { ...messengerNotifications[prevIdx], ...entry };
            } else {
                messengerNotifications = [entry, ...(messengerNotifications || [])]
                    .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))
                    .slice(0, 300);
            }
            if (opts.markUnread !== false && messengerView !== 'notifications') {
                messengerNotificationUnreadIds.add(id);
            }
            persistMessengerNotifications();
        }

        function openMessengerNotification(notificationId) {
            const id = String(notificationId || '').trim();
            if (!id) return;
            const item = (messengerNotifications || []).find((entry) => String(entry?.id || '') === id);
            if (!item) return;
            messengerNotificationUnreadIds.delete(id);
            persistMessengerNotifications();
            if (item.type === 'mention' && item.chatId) {
                setMessengerMentionUnreadForChat(item.chatId, 0);
            }
            if (item.chatId && findMessengerChatById(item.chatId)) {
                setMessengerView('chats');
                openMessengerChatById(item.chatId);
                if (item.messageId) {
                    setTimeout(() => {
                        scrollAndHighlightMessengerMessage(item.messageId);
                    }, 250);
                }
                return;
            }
            if (item.actorId) openUserProfile(item.actorId);
        }

        function recordMessengerReactionNotifications(chatId, messageId, prevMessage, nextReactions) {
            const myId = String(authProfile?.appUserId || '').trim();
            if (!myId || String(prevMessage?.fromId || '') !== myId) return;
            const prev = prevMessage?.reactions && typeof prevMessage.reactions === 'object' ? prevMessage.reactions : {};
            const next = nextReactions && typeof nextReactions === 'object' ? nextReactions : {};
            const emojis = new Set([...Object.keys(prev), ...Object.keys(next)]);
            emojis.forEach((emoji) => {
                const prevUsers = new Set((Array.isArray(prev[emoji]) ? prev[emoji] : []).map((userId) => String(userId || '').trim()).filter(Boolean));
                const nextUsers = Array.from(new Set((Array.isArray(next[emoji]) ? next[emoji] : []).map((userId) => String(userId || '').trim()).filter(Boolean)));
                nextUsers.forEach((userId) => {
                    if (!userId || userId === myId || prevUsers.has(userId)) return;
                    const actor = resolvePeerDisplay(userId);
                    const actorName = String(actor?.displayName || actor?.name || userId).trim() || userId;
                    const actorAvatar = String(actor?.avatar || '').trim();
                    const actorInitials = String(actor?.initials || (actorName || '').split(/\s+/).filter(Boolean).map((p) => p.charAt(0)).join('').slice(0, 2).toUpperCase() || '').trim();
                    pushMessengerNotification({
                        id: `reaction:${String(chatId || '').trim()}:${String(messageId || '').trim()}:${emoji}:${userId}`,
                        type: 'reaction',
                        ...getMessengerNotificationChatMeta(chatId),
                        actorId: userId,
                        actorName,
                        actorAvatar,
                        actorInitials,
                        messageId: String(messageId || '').trim(),
                        title: 'Реакция',
                        text: `${actorName} поставил(а) ${emoji} на ваше сообщение`,
                        createdAt: Date.now()
                    });
                });
            });
        }

        function getMessengerMentionUnreadForChat(chatId) {
            return messengerMentionUnreadCounts.get(String(chatId || '')) || 0;
        }

        function setMessengerMentionUnreadForChat(chatId, value) {
            const id = String(chatId || '');
            if (!id) return;
            const v = Math.max(0, Number(value) || 0);
            if (v) messengerMentionUnreadCounts.set(id, v);
            else messengerMentionUnreadCounts.delete(id);
        }

        function getMessengerMentionUnreadTotal() {
            let sum = 0;
            messengerMentionUnreadCounts.forEach((v) => {
                sum += Math.max(0, Number(v) || 0);
            });
            return sum;
        }

        function updateMessengerMentionFabUI() {
            const wrap = document.getElementById('scrollToMentionFabWrap');
            const badge = document.getElementById('scrollToMentionFabBadge');
            if (!wrap || !badge) return;
            const c = Math.max(0, Number(messengerMentionWhileScrolledCount) || 0);
            if (!c) {
                wrap.style.display = 'none';
                return;
            }
            badge.textContent = c > 99 ? '99+' : String(c);
            wrap.style.display = 'flex';
        }

        function scrollMessengerHistoryToNextMention() {
            const chatId = String(messengerActiveChatId || '').trim();
            const ids = messengerPendingMentionIdsByChat.get(chatId) || [];
            const nextId = ids.length ? String(ids[0] || '').trim() : '';
            if (!nextId) {
                messengerMentionWhileScrolledCount = 0;
                updateMessengerMentionFabUI();
                return;
            }
            messengerPendingMentionIdsByChat.set(chatId, ids.slice(1));
            messengerMentionWhileScrolledCount = Math.max(0, ids.length - 1);
            updateMessengerMentionFabUI();
            scrollAndHighlightMessengerMessage(nextId);
        }

        let messengerProfile = { username: '', statusText: '', privacy: { canWrite: 'all', canCall: 'all', canViewProfile: 'all', canSeeStories: 'friends', canJoinGroups: 'friends' }, blacklist: [] };
        let callMinimized = false;
        let currentGroupCallChatId = '';
        let currentGroupCallTitle = '';
        let pendingGroupInviteCode = '';
        let messengerRenderPendingAfterScroll = false;
        // Новые сообщения в текущем чате, пока пользователь прокручен вверх.
        let messengerNewWhileScrolledCount = 0;
        function shouldRenderMessengerUi() {
            const base = !roomId || callMinimized;
            if (!base) return false;
            // Не перерисовываем чат во время ручного скролла — иначе ломается momentum и скролл "останавливается".
            if ((messengerView === 'chats' && messengerIsUserScrolling) || (messengerView === 'notifications' && messengerWorkspaceIsUserScrolling)) {
                messengerRenderPendingAfterScroll = true;
                return false;
            }
            return true;
        }
        let composerReplyMessage = null;
        let composerEditMessageId = '';
        let composerMediaDraft = null;
        let lastActiveChatId = '';
        let lastActivePeerId = '';
        let composerMentionState = { open: false, query: '', candidates: [], activeIndex: 0, atIndex: -1, endIndex: -1 };
        let messageTouchHoldTimer = null;
        let messengerViewedProfile = null;
        let pendingMessengerEvents = [];
        const composerDraftByPeerId = new Map();
        let messengerComposeBlocked = false;
        let messengerComposeHint = '';
        let messengerSyncInProgress = false;
        const _hasRealMsgCache = new Map();
        let voiceMediaRecorder = null;
        let voiceMediaStream = null;
        let voiceRecordChunks = [];
        let voiceRecordingActive = false;
        let voiceRecordStartedAt = 0;
        let voiceRecTimerInterval = null;
        let voiceRecordPreview = null;
        let voicePreviewAudioEl = null;
        // Плеер музыки (аудиосообщения) — мобильный островок.
        const musicPlayer = {
            audioEl: null,
            chatId: '',
            msgId: '',
            playing: false,
            title: '',
            tickInterval: null
        };
        let musicIslandEl = null;
        const messengerPeerHints = new Map();
        // Чтобы аватары/имена профиля обновлялись в "Друзьях" даже после следующего poll.
        const messengerProfileOverrides = new Map();
        let friendsCallsModalPrimed = false;
        let isChatOpen = false;
        const MESSENGER_SESSION_PEER_KEY = 'seych-messenger-active-peer';
        const MESSENGER_SESSION_CHAT_KEY = 'seych-messenger-active-chat';
        let mobileNavDrawerOpen = false;
        let messengerUiTypingTimer = null;
        let friendsSearchDebounceTimer = null;
        let lastComposerTypingEmit = 0;
        let messengerCreateGroupModalOpen = false;
        let messengerComposerFocusLockUntil = 0;
        let messengerSuppressBlurUntil = 0;

        function isComposerFocusLockActive() {
            return messengerView === 'chats'
                && !!messengerActiveChatId
                && Date.now() < Number(messengerComposerFocusLockUntil || 0);
        }

        function armComposerFocusLock(durationMs = 1400) {
            const nextUntil = Date.now() + Math.max(0, Number(durationMs) || 0);
            messengerComposerFocusLockUntil = Math.max(Number(messengerComposerFocusLockUntil || 0), nextUntil);
        }

        function releaseComposerFocusLock() {
            messengerComposerFocusLockUntil = 0;
        }

        function captureMessengerFocusSnapshot() {
            let el = document.activeElement;
            if ((!el || !el.id || el.id !== 'chatComposerInput') && isComposerFocusLockActive()) {
                el = document.getElementById('chatComposerInput');
            }
            if (!el || !el.id) return null;
            if (el.id !== 'chatComposerInput' && el.id !== 'friendsSearchInput') return null;
            let sel = null;
            try {
                if (typeof el.selectionStart === 'number') {
                    sel = { s: el.selectionStart, e: el.selectionEnd };
                }
            } catch (_) {}
            return { id: el.id, value: el.value, sel };
        }

        function restoreMessengerFocusSnapshot(snap) {
            if (!snap) return;
            const n = document.getElementById(snap.id);
            if (!n) return;
            n.value = snap.value;
            n.focus();
            if (snap.sel && typeof n.setSelectionRange === 'function') {
                try {
                    n.setSelectionRange(snap.sel.s, snap.sel.e);
                } catch (_) {}
            }
            if (snap.id === 'chatComposerInput') onComposerInput();
        }

        function shouldDeferTransientMessengerRender() {
            const ae = document.activeElement;
            return isMobileLayout()
                && messengerView === 'chats'
                && ((ae && ae.id === 'chatComposerInput') || isComposerFocusLockActive());
        }

        function handleComposerBlur() {
            setTimeout(() => {
                if (Date.now() < Number(messengerSuppressBlurUntil || 0)) return;
                if (isComposerFocusLockActive()) {
                    const input = document.getElementById('chatComposerInput');
                    if (input && !input.disabled && messengerView === 'chats' && messengerActiveChatId) {
                        try {
                            input.focus({ preventScroll: true });
                        } catch (_) {
                            input.focus();
                        }
                        return;
                    }
                }
                const ae = document.activeElement;
                if (ae && ae.id === 'chatComposerInput') return;
                releaseComposerFocusLock();
                if (messengerRenderPendingAfterScroll && shouldRenderMessengerUi()) {
                    messengerRenderPendingAfterScroll = false;
                    renderMainScreen();
                }
            }, 80);
        }

        const CHAT_SCROLL_STORAGE_KEY = 'seych_chat_hist_scroll_v2';

        function saveChatHistoryScrollState() {
            try {
                const hist = document.querySelector('.chat-history');
                if (!hist || messengerView !== 'chats' || !messengerActiveChatId) return;
                sessionStorage.setItem(CHAT_SCROLL_STORAGE_KEY, JSON.stringify({
                    chatId: messengerActiveChatId,
                    scrollTop: Number(hist.scrollTop || 0),
                    distFromBottom: getChatHistoryDistFromBottom()
                }));
            } catch (_) {}
        }

        function restoreChatHistoryScrollState() {
            try {
                const raw = sessionStorage.getItem(CHAT_SCROLL_STORAGE_KEY);
                if (!raw) return;
                const saved = JSON.parse(raw);
                if (!saved || String(saved.chatId || '') !== String(messengerActiveChatId || '')) return;
                const hist = document.querySelector('.chat-history');
                if (!hist) return;
                const apply = () => {
                    const max = Math.max(0, hist.scrollHeight - hist.clientHeight);
                    const dist = Number(saved.distFromBottom);
                    if (Number.isFinite(dist) && dist < 100) {
                        hist.scrollTop = max;
                    } else {
                        hist.scrollTop = Math.max(0, Math.min(Number(saved.scrollTop || 0), max));
                    }
                };
                requestAnimationFrame(() => requestAnimationFrame(apply));
            } catch (_) {}
        }

        function refreshMessengerChatHistoryOnly() {
            const root = document.querySelector('.messenger-workspace .chat-workspace');
            const hist = root?.querySelector('.chat-history');
            if (!root || !hist) {
                if (shouldRenderMessengerUi()) renderMainScreen();
                return;
            }
            const snap = captureMessengerFocusSnapshot();
            const nearBottom = getChatHistoryDistFromBottom() < 100 || messengerShouldAutoScroll;
            const tmp = document.createElement('div');
            tmp.innerHTML = renderMessengerWorkspace();
            const fresh = tmp.querySelector('.chat-workspace');
            const newHist = fresh?.querySelector('.chat-history');
            const newFab = fresh?.querySelector('.chat-fab-stack');
            if (newHist) hist.innerHTML = newHist.innerHTML;
            const fab = root.querySelector('.chat-fab-stack');
            if (fab && newFab) fab.innerHTML = newFab.innerHTML;
            updateMessengerNewWhileScrolledFabUI();
            if (nearBottom) {
                scrollMessengerHistoryToBottom();
            }
            bindMessengerHistoryScrollGuard();
            hydrateMessengerLinkPreviews();
            requestAnimationFrame(() => {
                restoreMessengerFocusSnapshot(snap);
            });
        }

        function voiceWaveBarsFromSeed(seed, count) {
            let h = 0;
            const s = String(seed || '');
            for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
            const bars = [];
            for (let i = 0; i < count; i++) {
                h = (h * 1664525 + 1013904223) >>> 0;
                bars.push(8 + (h % 20));
            }
            return bars;
        }

        function voiceProgressUpdateHandler(audio, wrap) {
            return () => {
                const fill = wrap && wrap.querySelector ? wrap.querySelector('.voice-progress-fill') : null;
                if (!fill || !audio.duration) return;
                const pct = Math.min(100, (audio.currentTime / audio.duration) * 100);
                fill.style.width = `${pct}%`;
            };
        }

        function ensureMusicPlayerEl() {
            if (musicPlayer.audioEl) return;
            musicPlayer.audioEl = new Audio();
            musicPlayer.audioEl.preload = 'metadata';
            musicPlayer.audioEl.ontimeupdate = () => {
                updateMusicIslandProgress();
            };
            musicPlayer.audioEl.onended = () => {
                musicPlayer.playing = false;
                updateMusicIslandUi();
                syncMusicIslandWidget();
                renderMainScreen();
            };
        }

        function updateMusicIslandProgress() {
            if (!musicIslandEl) return;
            const fill = document.getElementById('musicIslandProgressFill');
            const audio = musicPlayer.audioEl;
            if (!fill || !audio || !audio.duration || Number.isNaN(audio.duration)) return;
            const pct = Math.max(0, Math.min(100, (audio.currentTime / audio.duration) * 100));
            fill.style.width = `${pct}%`;
            // Обновляем прогресс у музыки в сообщении (если оно сейчас играет).
            try {
                const safeMsgId = String(musicPlayer.msgId || '').replace(/[^a-zA-Z0-9_-]/g, '');
                const inlineFill = document.getElementById(`musicInlineProgressFill-${safeMsgId}`);
                if (inlineFill) inlineFill.style.width = `${pct}%`;
            } catch (_) {}
        }

        function updateMusicIslandUi() {
            if (!musicIslandEl) return;
            const title = document.getElementById('musicIslandTitle');
            if (title) title.textContent = musicPlayer.title || 'Музыка';
            const icon = document.getElementById('musicPlayPauseIcon');
            if (icon) {
                const a = musicPlayer.audioEl;
                const playing = !!(a && !a.paused && !a.ended);
                // По ТЗ: когда играет — показываем Stop, когда стоит — Play.
                icon.className = playing ? 'fas fa-stop' : 'fas fa-play';
            }
            updateMusicIslandProgress();
        }

        function ensureMusicIslandWidget() {
            if (musicIslandEl) return;
            musicIslandEl = document.createElement('div');
            musicIslandEl.id = 'musicIsland';
            musicIslandEl.className = 'music-island';
            musicIslandEl.innerHTML = `
                <div class="music-island-row">
                    <button type="button" class="music-island-btn" onclick="seekMusicBy(-10)" aria-label="Назад">
                        <i class="fas fa-backward"></i>
                    </button>
                    <button type="button" class="music-island-btn" id="musicPlayPauseBtn" onclick="toggleMusicIslandPlayPause()" aria-label="Вкл/Пауза">
                        <i id="musicPlayPauseIcon" class="fas fa-play"></i>
                    </button>
                    <button type="button" class="music-island-btn" onclick="seekMusicBy(10)" aria-label="Вперёд">
                        <i class="fas fa-forward"></i>
                    </button>
                    <div class="music-island-title" id="musicIslandTitle">Музыка</div>
                    <button type="button" class="music-island-btn danger" onclick="stopMusicPlayer(true)" aria-label="Закрыть">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
                <div class="music-island-progress" aria-hidden="true">
                    <div class="music-island-progress-fill" id="musicIslandProgressFill"></div>
                </div>
            `;
            document.body.appendChild(musicIslandEl);
        }

        function syncMusicIslandWidget() {
            if (!isMobileLayout()) {
                if (musicIslandEl) musicIslandEl.classList.remove('open');
                // Снимаем смещение контента, если мы ушли с mobile.
                try {
                    const hist = document.querySelector('.chat-history');
                    if (hist && hist.dataset.musicIslandPadApplied) {
                        hist.style.paddingTop = '';
                        delete hist.dataset.musicIslandPadApplied;
                    }
                } catch (_) {}
                return;
            }
            ensureMusicIslandWidget();
            if (!musicPlayer.audioEl || !musicPlayer.chatId || !musicPlayer.msgId) {
                musicIslandEl.classList.remove('open');
                try {
                    const hist = document.querySelector('.chat-history');
                    if (hist && hist.dataset.musicIslandPadApplied) {
                        hist.style.paddingTop = '';
                        delete hist.dataset.musicIslandPadApplied;
                    }
                } catch (_) {}
                return;
            }
            // Если пользователь вернулся в тот же диалог, где включили музыку — скрываем остров.
            const inSameChat = messengerView === 'chats' && messengerActiveChatId === musicPlayer.chatId;
            if (inSameChat) {
                musicIslandEl.classList.remove('open');
                try {
                    const hist = document.querySelector('.chat-history');
                    if (hist && hist.dataset.musicIslandPadApplied) {
                        hist.style.paddingTop = '';
                        delete hist.dataset.musicIslandPadApplied;
                    }
                } catch (_) {}
                return;
            }
            // В чате другого диалога: поднять выше (top). Иначе: снизу над навигацией.
            const inChatMode = messengerView === 'chats' && isChatOpen;
            // Сбрасываем инлайн-позиционирование, чтобы при переключении режимов остров не "залипал".
            musicIslandEl.style.top = '';
            musicIslandEl.style.bottom = '';
            musicIslandEl.classList.toggle('music-island--top', inChatMode);
            musicIslandEl.classList.toggle('music-island--bottom', !inChatMode);
            musicIslandEl.classList.add('open');
            if (inChatMode) {
                // На мобильном при открытом чате сайдбар может быть скрыт,
                // поэтому опираемся в первую очередь на `.chat-topbar` внутри workspace.
                const tb = document.querySelector('.messenger-workspace .chat-topbar') || document.querySelector('.chat-topbar');
                if (tb && tb.getBoundingClientRect) {
                    const r = tb.getBoundingClientRect();
                    // Ниже области статуса/имени/кнопок.
                    musicIslandEl.style.top = `${Math.round(r.bottom + 8)}px`;
                    musicIslandEl.style.bottom = 'auto';
                } else {
                    // Фоллбек: статус-строка в сайдбаре.
                    const statusEl = document.querySelector('.messenger-sidebar .messenger-connection');
                    if (statusEl && statusEl.getBoundingClientRect) {
                        const r = statusEl.getBoundingClientRect();
                        musicIslandEl.style.top = `${Math.round(r.bottom + 10)}px`;
                        musicIslandEl.style.bottom = 'auto';
                    }
                }
            }

            // Важно: остров сверху фиксированный и может перекрыть первый message.
            // Поэтому добавляем padding-top в `.chat-history` на высоту острова.
            try {
                const hist = document.querySelector('.chat-history');
                if (hist) {
                    if (inChatMode) {
                        const h = musicIslandEl.getBoundingClientRect().height || 0;
                        const pad = Math.round(h + 6);
                        hist.style.paddingTop = `${pad}px`;
                        hist.dataset.musicIslandPadApplied = String(pad);
                    } else if (hist.dataset.musicIslandPadApplied) {
                        hist.style.paddingTop = '';
                        delete hist.dataset.musicIslandPadApplied;
                    }
                }
            } catch (_) {}
            updateMusicIslandUi();
        }

        function stopMusicPlayer(hideIsland = true) {
            if (musicPlayer.audioEl) {
                try {
                    musicPlayer.audioEl.pause();
                    musicPlayer.audioEl.currentTime = 0;
                } catch (_) {}
            }
            musicPlayer.playing = false;
            if (hideIsland) {
                musicPlayer.chatId = '';
                musicPlayer.msgId = '';
                musicPlayer.title = '';
            }
            if (musicIslandEl) {
                const fill = document.getElementById('musicIslandProgressFill');
                if (fill) fill.style.width = '0%';
            }
            syncMusicIslandWidget();
            renderMainScreen();
        }

        function seekMusicBy(deltaSeconds) {
            const a = musicPlayer.audioEl;
            if (!a || !a.duration) return;
            try {
                a.currentTime = Math.max(0, Math.min(a.duration, a.currentTime + deltaSeconds));
            } catch (_) {}
        }

        function toggleMusicIslandPlayPause() {
            ensureMusicPlayerEl();
            const a = musicPlayer.audioEl;
            if (!musicPlayer.chatId || !musicPlayer.msgId || !a || !a.src) return;
            const isPlaying = !!a && !a.paused && !a.ended;
            if (isPlaying) {
                // Stop без скрытия острова.
                stopMusicPlayer(false);
                return;
            }
            a.play().catch(() => {});
            musicPlayer.playing = true;
            updateMusicIslandUi();
            syncMusicIslandWidget();
        }

        function toggleMusicFromMessage(btn) {
            const chatId = btn?.dataset?.chatId || '';
            const msgId = btn?.dataset?.msgId || '';
            if (!chatId || !msgId) return;
            const list = messengerMessages.get(chatId) || [];
            const msg = list.find((m) => String(m?.id || '') === String(msgId || ''));
            if (!msg || msg.messageKind !== 'voice') return;
            if (!msg.audioBase64) return;
            ensureMusicPlayerEl();
            const audioMimeRaw = String(msg.audioMime || '').split(';')[0].trim();
            const mime = /^audio\/(webm|ogg|mp4|mpeg|wav|m4a|x-m4a|aac|x-aac)$/i.test(audioMimeRaw) ? audioMimeRaw : 'audio/webm';
            const b64 = String(msg.audioBase64 || '').replace(/[^a-zA-Z0-9+/=]/g, '');
            const src = `data:${mime};base64,${b64}`;
            const isSame = String(musicPlayer.chatId || '') === String(chatId || '') && String(musicPlayer.msgId || '') === String(msgId || '');
            const icon = btn && btn.querySelector ? btn.querySelector('i') : null;
            if (!isSame) {
                musicPlayer.chatId = chatId;
                musicPlayer.msgId = msgId;
                musicPlayer.title = msg.text || 'Музыка';
                musicPlayer.audioEl.src = src;
                musicPlayer.audioEl.currentTime = 0;
                musicPlayer.playing = false;
            }
            if (musicPlayer.audioEl.paused) {
                musicPlayer.audioEl.play().catch(() => {});
                musicPlayer.playing = true;
                if (icon) icon.className = 'fas fa-pause';
            } else {
                musicPlayer.audioEl.pause();
                musicPlayer.playing = false;
                if (icon) icon.className = 'fas fa-play';
            }
            updateMusicIslandUi();
            syncMusicIslandWidget();
        }

        // Оставляем старое имя для совместимости, но теперь логика через music-player.
        function toggleVoicePlay(btn) {
            return toggleMusicFromMessage(btn);
        }

        function messengerMobileWorkspaceOpen() {
            return isMobileLayout() && (isChatOpen || messengerView !== 'chats');
        }

        // Mobile: disable swipe navigation between screens.
        // People were accidentally switching tabs by horizontal swipes.
        function isMessengerMobileTabPagerActive() {
            return false;
        }

        let messengerPagerTouch = null;

        function getMessengerPagerPageIndex() {
            return messengerView === 'friends' ? 1 : 0;
        }

        function applyMessengerPagerTransform(pageIndex, dragPx, animate) {
            const track = document.getElementById('messengerMobilePagerTrack');
            if (!track) return;
            const host = track.parentElement;
            const w = host ? host.clientWidth : window.innerWidth;
            const offset = pageIndex * w - (Number(dragPx) || 0);
            track.style.transition = animate === false ? 'none' : 'transform 0.28s cubic-bezier(0.22, 1, 0.36, 1)';
            track.style.transform = `translate3d(${-offset}px, 0, 0)`;
        }

        function initMessengerMobilePagerPosition(animate) {
            if (!isMessengerMobileTabPagerActive()) return;
            applyMessengerPagerTransform(getMessengerPagerPageIndex(), 0, animate !== false);
        }

        function handleMessengerPagerTouchStart(event) {
            if (mobileNavDrawerOpen) return;
            if (!isMessengerMobileTabPagerActive()) return;
            const t = event.touches && event.touches[0];
            if (!t) return;
            messengerPagerTouch = { x: Number(t.clientX || 0), y: Number(t.clientY || 0), locked: false };
        }

        function handleMessengerPagerTouchMove(event) {
            if (mobileNavDrawerOpen) return;
            if (!messengerPagerTouch || !isMessengerMobileTabPagerActive()) return;
            const t = event.touches && event.touches[0];
            if (!t) return;
            const dx = Number(t.clientX || 0) - messengerPagerTouch.x;
            const dy = Number(t.clientY || 0) - messengerPagerTouch.y;
            if (!messengerPagerTouch.locked) {
                if (Math.abs(dy) > Math.abs(dx) && Math.abs(dy) > 14) {
                    messengerPagerTouch = null;
                    return;
                }
                if (Math.abs(dx) > 10) messengerPagerTouch.locked = true;
            }
            if (!messengerPagerTouch.locked) return;
            event.preventDefault();
            let drag = dx;
            const idx = getMessengerPagerPageIndex();
            if (idx === 0 && drag > 0) drag *= 0.32;
            if (idx === 1 && drag < 0) drag *= 0.32;
            applyMessengerPagerTransform(idx, -drag, false);
        }

        function handleMessengerPagerTouchEnd(event) {
            if (mobileNavDrawerOpen) return;
            if (!messengerPagerTouch || !isMessengerMobileTabPagerActive()) return;
            const t = event.changedTouches && event.changedTouches[0];
            if (!t) {
                messengerPagerTouch = null;
                return;
            }
            const dx = Number(t.clientX || 0) - messengerPagerTouch.x;
            const host = document.getElementById('messengerMobilePager');
            const w = host ? host.clientWidth : window.innerWidth;
            const threshold = Math.max(56, w * 0.2);
            let nextView = messengerView;
            if (dx < -threshold && messengerView === 'chats') nextView = 'friends';
            else if (dx > threshold && messengerView === 'friends') nextView = 'chats';
            messengerPagerTouch = null;
            if (nextView !== messengerView) {
                messengerView = nextView;
                renderMainScreen();
                return;
            }
            initMessengerMobilePagerPosition(true);
        }

        function renderMessengerNavDrawer(notificationTotal) {
            if (!mobileNavDrawerOpen) return '';
            const name = String(authProfile?.name || authProfile?.appUserId || 'Профиль').trim() || 'Профиль';
            const uid = String(authProfile?.appUserId || '').trim();
            const cover = String(authProfile?.coverUrl || '').trim();
            const avatar = authProfile?.avatar || '';
            const initials = authProfile?.initials || '';
            const coverStyle = cover ? `background-image:url('${escapeHtml(cover)}');` : '';
            return `
                <div class="nav-drawer-overlay sm-nav-drawer" onclick="if(event.target===this)closeMobileNavDrawer()">
                    <aside class="nav-drawer-panel sm-nav-drawer-panel" onclick="event.stopPropagation()">
                        <div class="sm-nav-drawer-head">
                            <button type="button" class="messenger-nav-btn nav-drawer-close" onclick="closeMobileNavDrawer()" aria-label="Закрыть"><i class="fas fa-times"></i></button>
                        </div>
                        <div class="sm-nav-drawer-profile">
                            <div class="sm-nav-drawer-cover" style="${coverStyle}"></div>
                            <div class="sm-nav-drawer-profile-body">
                                <div class="sm-nav-drawer-avatar">${avatarMarkup(name, avatar, initials)}</div>
                                <div class="sm-nav-drawer-meta">
                                    <div class="sm-nav-drawer-name">${escapeHtml(name)}</div>
                                    <div class="sm-nav-drawer-id">ID ${escapeHtml(uid)}</div>
                                </div>
                            </div>
                        </div>
                        <nav class="sm-nav-drawer-nav">
                            <button type="button" class="nav-drawer-item sm-nav-drawer-item" onclick="navigateFromNavDrawer('notifications')">
                                <i class="fas fa-bell"></i><span>Уведомления</span>
                                ${notificationTotal ? `<span class="sm-nav-drawer-badge">${notificationTotal > 99 ? '99+' : notificationTotal}</span>` : ''}
                            </button>
                            <button type="button" class="nav-drawer-item sm-nav-drawer-item" onclick="navigateFromNavDrawer('settings')">
                                <i class="fas fa-sliders-h"></i><span>Настройки</span>
                            </button>
                            <button type="button" class="nav-drawer-item sm-nav-drawer-item" onclick="navigateFromNavDrawer('calls')">
                                <i class="fas fa-phone"></i><span>Звонки</span>
                            </button>
                            <button type="button" class="nav-drawer-item sm-nav-drawer-item" onclick="navigateFromNavDrawer('friends')">
                                <i class="fas fa-user-friends"></i><span>Друзья</span>
                            </button>
                            <button type="button" class="nav-drawer-item sm-nav-drawer-item" onclick="navigateFromNavDrawer('profile')">
                                <i class="fas fa-user"></i><span>Профиль</span>
                            </button>
                        </nav>
                    </aside>
                </div>`;
        }

        function navigateFromNavDrawer(view) {
            closeMobileNavDrawer();
            setMessengerView(view);
        }

        function toggleMobileNavDrawer() {
            mobileNavDrawerOpen = !mobileNavDrawerOpen;
            renderMainScreen();
        }

        function closeMobileNavDrawer() {
            mobileNavDrawerOpen = false;
            const overlay = document.querySelector('.sm-nav-drawer');
            if (overlay) {
                overlay.remove();
                return;
            }
            renderMainScreen();
        }

        function createDirectChatIdClient(a, b) {
            const trim = (v) => String(v || '').trim().slice(0, 120);
            const pair = [trim(a), trim(b)].filter(Boolean).sort();
            if (pair.length !== 2 || pair[0] === pair[1]) return '';
            return `dm:${pair[0]}::${pair[1]}`;
        }

        function parsePeerIdFromDirectChatId(chatId, myId) {
            const cid = String(chatId || '').trim();
            const me = String(myId || '').trim();
            if (!cid.startsWith('dm:') || !me) return '';
            const payload = cid.slice(3);
            const parts = payload.split('::').map((x) => String(x || '').trim()).filter(Boolean);
            if (parts.length !== 2) return '';
            const a = parts[0];
            const b = parts[1];
            if (a === me) return b;
            if (b === me) return a;
            return '';
        }

        function findMessengerChatById(chatId) {
            const id = String(chatId || '').trim();
            if (!id) return null;
            return messengerChats.find((item) => String(item?.id || '') === id) || null;
        }

        function isGroupMessengerChat(chat) {
            return !!chat && String(chat.kind || '') === 'group';
        }

        function isDirectMessengerChat(chat) {
            return !!chat && String(chat.kind || 'direct') !== 'group';
        }

        function buildGroupChatClientModel(group) {
            if (!group || String(group.kind || 'group') !== 'group') return null;
            const title = String(group.title || 'Групповой чат').trim() || 'Групповой чат';
            const tempChat = { id: String(group.id || ''), kind: 'group', group };
            return {
                id: String(group.id || ''),
                kind: 'group',
                peer: {
                    id: String(group.id || ''),
                    name: title,
                    displayName: title,
                    avatar: String(group.avatar || ''),
                    initials: title.split(/\s+/).filter(Boolean).map((x) => x.charAt(0)).join('').slice(0, 2).toUpperCase() || 'GC',
                    username: '',
                    statusText: getGroupChatStatusText(tempChat),
                    online: false,
                    lastSeenAt: 0
                },
                group,
                updatedAt: Date.now()
            };
        }

        function upsertGroupChatModel(group) {
            const model = buildGroupChatClientModel(group);
            if (!model) return null;
            const idx = messengerChats.findIndex((item) => String(item?.id || '') === String(model.id || ''));
            if (idx >= 0) {
                messengerChats[idx] = {
                    ...messengerChats[idx],
                    ...model,
                    lastMessage: messengerChats[idx]?.lastMessage || model.lastMessage || null
                };
            } else {
                messengerChats.unshift(model);
            }
            messengerChats = mergeMessengerChatsWithHints(messengerChats);
            return idx >= 0 ? messengerChats[idx] : messengerChats[0];
        }

        function applyMessengerPeerHint(userId, displayName, avatar, initials, username = '', statusText = '', rawName = '') {
            const id = String(userId || '').trim();
            if (!id) return;
            const dn = String(displayName || '').trim();
            const av = String(avatar || '').trim();
            const ini = String(initials || '').trim();
            const un = normalizeMessengerUsernameValue(username || '');
            const st = String(statusText || '').trim();
            const nm = String(rawName || '').trim();
            if (!dn && !av && !ini && !un && !st && !nm) return;
            const prev = messengerPeerHints.get(id) || {};
            messengerPeerHints.set(id, {
                displayName: dn || prev.displayName || '',
                name: nm || prev.name || '',
                avatar: av || prev.avatar || '',
                initials: ini || prev.initials || '',
                username: un || prev.username || '',
                statusText: st || prev.statusText || ''
            });
        }

        function mergeMessengerChatsWithHints(chats) {
            const list = Array.isArray(chats) ? chats : [];
            const prevById = new Map(
                (messengerChats || []).map((x) => [String(x.peer?.id || '').trim(), x.peer]).filter((e) => e[0])
            );
            return list.map((c) => {
                const pid = String(c.peer?.id || '').trim();
                if (!pid) return c;
                const h = messengerPeerHints.get(pid);
                const peer = c.peer || {};
                const prevPeer = prevById.get(pid);
                const curDn = String(peer.displayName || peer.name || '').trim();
                const looksBare = !curDn || curDn === pid;
                const nextDn =
                    (looksBare && h && h.displayName) || curDn || (h && h.displayName) || pid;
                const serverAv = String(peer.avatar || '').trim();
                const prevAv = String(prevPeer?.avatar || '').trim();
                const hintAv = h ? String(h.avatar || '').trim() : '';
                const hintUsername = h ? String(h.username || '').replace(/^@+/, '').trim() : '';
                const prevUsername = String(prevPeer?.username || '').replace(/^@+/, '').trim();
                const serverUsername = String(peer.username || '').replace(/^@+/, '').trim();
                const hintStatusText = h ? String(h.statusText || '').trim() : '';
                const prevStatusText = String(prevPeer?.statusText || '').trim();
                const serverStatusText = String(peer.statusText || '').trim();
                const avatarMerged = serverAv || hintAv || prevAv;
                if (!h && !prevPeer && !avatarMerged && looksBare && nextDn === curDn) return c;
                return {
                    ...c,
                    peer: {
                        ...peer,
                        id: pid,
                        displayName: nextDn,
                        name: (peer.name && peer.name !== pid ? peer.name : '') || (h && h.name) || nextDn,
                        avatar: avatarMerged,
                        initials: peer.initials || (h && h.initials) || prevPeer?.initials || '',
                        username: serverUsername || hintUsername || prevUsername || '',
                        statusText: serverStatusText || hintStatusText || prevStatusText || ''
                    }
                };
            });
        }

        function hydrateMessengerHintsFromChats(chats) {
            const list = Array.isArray(chats) ? chats : [];
            list.forEach((chat) => {
                if (!isGroupMessengerChat(chat)) return;
                getGroupChatParticipants(chat).forEach((participant) => {
                    const uid = String(participant?.userId || participant?.id || '').trim();
                    if (!uid) return;
                    applyMessengerPeerHint(
                        uid,
                        participant?.displayName || participant?.name || uid,
                        participant?.avatar || '',
                        participant?.initials || '',
                        participant?.username || '',
                        '',
                        participant?.name || participant?.displayName || uid
                    );
                });
            });
        }

        function hydrateMessengerHintsFromMessages(messages) {
            const arr = Array.isArray(messages) ? messages : [];
            arr.forEach((m) => {
                if (!m || !m.fromId) return;
                applyMessengerPeerHint(m.fromId, m.senderDisplayName, m.senderAvatar, m.senderInitials);
            });
        }

        function resolvePeerDisplay(peerId) {
            const id = String(peerId || '').trim();
            if (!id) return { id: '', name: '', displayName: '', avatar: '', username: '', statusText: '', initials: '' };
            const ensureUsername = (value) => ensureGeneratedMessengerUsername(value || '', id);
            const fromChat = messengerChats.find((c) => String(c.peer?.id || '') === id)?.peer;
            if (fromChat) {
                const hint = messengerPeerHints.get(id);
                const rawDn = fromChat.displayName || fromChat.name || id;
                const looksBare = !rawDn || rawDn === id;
                const dn = looksBare && hint?.displayName ? hint.displayName : rawDn || id;
                const avatar = fromChat.avatar || hint?.avatar || '';
                const initials = fromChat.initials || hint?.initials || '';
                return {
                    id,
                    name: fromChat.name || hint?.name || dn,
                    displayName: dn,
                    avatar,
                    username: ensureUsername(fromChat.username || hint?.username || ''),
                    statusText: fromChat.statusText || hint?.statusText || '',
                    initials,
                    online: !!fromChat.online,
                    lastSeenAt: Number(fromChat.lastSeenAt) || 0
                };
            }
            const hint = messengerPeerHints.get(id);
            if (hint && (hint.displayName || hint.avatar)) {
                const dn = hint.displayName || id;
                return {
                    id,
                    name: hint.name || dn,
                    displayName: dn,
                    avatar: hint.avatar || '',
                    username: ensureUsername(hint.username || ''),
                    statusText: hint.statusText || '',
                    initials: hint.initials || '',
                    online: false,
                    lastSeenAt: 0
                };
            }
            const fromFriend = (friendsState.friends || []).find((f) => String(f.id) === id);
            if (fromFriend) {
                const dn = fromFriend.name || id;
                return {
                    id,
                    name: dn,
                    displayName: fromFriend.displayName || dn,
                    avatar: fromFriend.avatar || '',
                    username: ensureUsername(fromFriend.username || ''),
                    statusText: fromFriend.statusText || '',
                    initials: fromFriend.initials || '',
                    online: !!fromFriend.online,
                    lastSeenAt: Number(fromFriend.lastSeenAt) || 0
                };
            }
            const fromSearch = (friendsSearchResults || []).find((r) => String(r.id) === id);
            if (fromSearch) {
                const dn = fromSearch.name || id;
                return {
                    id,
                    name: dn,
                    displayName: fromSearch.displayName || dn,
                    avatar: fromSearch.avatar || '',
                    username: ensureUsername(fromSearch.username || ''),
                    statusText: fromSearch.statusText || '',
                    initials: fromSearch.initials || '',
                    online: !!fromSearch.online,
                    lastSeenAt: Number(fromSearch.lastSeenAt) || 0
                };
            }
            const p = messengerViewedProfile?.profile;
            if (p && String(p.id || '') === id) {
                const dn = p.displayName || p.name || id;
                return {
                    id,
                    name: p.name || dn,
                    displayName: dn,
                    avatar: p.avatar || '',
                    username: ensureGeneratedMessengerUsername(p.username || ''),
                    statusText: p.statusText || '',
                    initials: p.initials || '',
                    online: !!p.online,
                    lastSeenAt: Number(p.lastSeenAt) || 0
                };
            }
            return { id, name: id, displayName: id, avatar: '', username: ensureUsername(''), statusText: '', initials: '', online: false, lastSeenAt: 0 };
        }

        function resolveActiveMessengerChat() {
            let activeChat = messengerChats.find((item) => item.id === messengerActiveChatId) || null;
            if (!activeChat && messengerActiveChatId) {
                // Если это групповой чат, ищем его отдельно
                const groupChat = messengerChats.find((item) => 
                    String(item?.id || '') === String(messengerActiveChatId) && String(item?.kind || '') === 'group'
                );
                if (groupChat) {
                    activeChat = groupChat;
                } else if (messengerActivePeerId) {
                    const peer = resolvePeerDisplay(messengerActivePeerId);
                    activeChat = { id: messengerActiveChatId, peer, lastMessage: null };
                }
            }
            return activeChat;
        }

        function persistMessengerSessionChat(chatId) {
            try {
                const id = String(chatId || '').trim();
                if (id) sessionStorage.setItem(MESSENGER_SESSION_CHAT_KEY, id);
                else sessionStorage.removeItem(MESSENGER_SESSION_CHAT_KEY);
            } catch (_) {}
        }

        function persistMessengerSessionPeer(peerId) {
            try {
                const id = String(peerId || '').trim();
                if (id) sessionStorage.setItem(MESSENGER_SESSION_PEER_KEY, id);
                else sessionStorage.removeItem(MESSENGER_SESSION_PEER_KEY);
            } catch (_) {}
        }

        function restoreMessengerSessionPeer() {
            try {
                if (!authProfile?.appUserId) return;
                const chatId = String(sessionStorage.getItem(MESSENGER_SESSION_CHAT_KEY) || '').trim();
                if (chatId) {
                    messengerActiveChatId = chatId;
                    messengerActivePeerId = chatId.startsWith('dm:')
                        ? parsePeerIdFromDirectChatId(chatId, authProfile.appUserId)
                        : '';
                    messengerView = 'chats';
                    if (isMobileLayout()) isChatOpen = true;
                    return;
                }
                const peer = String(sessionStorage.getItem(MESSENGER_SESSION_PEER_KEY) || '').trim();
                if (!peer) return;
                messengerActivePeerId = peer;
                messengerActiveChatId = createDirectChatIdClient(authProfile.appUserId, peer);
                messengerView = 'chats';
                if (isMobileLayout()) isChatOpen = true;
            } catch (_) {}
        }

        function getMessengerPeerActivityState(peerId) {
            const id = String(peerId || '').trim();
            if (!id) return null;
            const v = messengerTypingByUser.get(id);
            if (!v) return null;
            if (typeof v === 'boolean') {
                return v ? { isTyping: true, activity: 'text', ts: Date.now() } : null;
            }
            if (typeof v !== 'object') return null;
            if (!v.isTyping) return null;
            const activity = v.activity === 'voice' ? 'voice' : 'text';
            return {
                isTyping: true,
                activity,
                ts: Number(v.ts || 0) || Date.now(),
                chatId: String(v.chatId || '').trim(),
                withUserId: String(v.withUserId || '').trim()
            };
        }

        function formatPeerStatusLine(peer, typingState) {
            if (!peer) return '';
            if (typingState && typingState.isTyping) {
                if (typingState.activity === 'voice') return 'записывает аудио';
                return 'печатает';
            }
            if (peer.online) return 'в сети';
            const ts = Number(peer.lastSeenAt || 0);
            if (ts > 0) {
                try {
                    return `Был в сети: ${new Date(ts).toLocaleString('ru-RU', {
                        day: 'numeric',
                        month: 'short',
                        hour: '2-digit',
                        minute: '2-digit'
                    })}`;
                } catch (_) {
                    return 'Был в сети';
                }
            }
            return 'не в сети';
        }

        function formatPresenceLabel(online, lastSeenAt, offlineFallback = 'Не в сети') {
            if (online) return 'В сети';
            const ts = Number(lastSeenAt || 0);
            if (ts > 0) {
                try {
                    return `Был(а) в сети ${new Date(ts).toLocaleString('ru-RU', {
                        day: '2-digit',
                        month: '2-digit',
                        hour: '2-digit',
                        minute: '2-digit'
                    })}`;
                } catch (_) {
                    return 'Был(а) в сети недавно';
                }
            }
            return offlineFallback;
        }

        function getParticipantPresenceState(participant) {
            const id = String(participant?.userId || participant?.id || '').trim();
            const resolved = id ? resolvePeerDisplay(id) : null;
            const online = participant?.online === true
                || participant?.presence?.online === true
                || !!resolved?.online;
            const lastSeenAt = Number(participant?.lastSeenAt || participant?.presence?.lastSeenAt || resolved?.lastSeenAt || 0) || 0;
            return { online, lastSeenAt };
        }

        function getGroupChatParticipants(chat, opts = {}) {
            const out = [];
            const seen = new Set();
            const includeLeft = !!opts.includeLeft;
            const pushParticipant = (source) => {
                const memberId = String(source?.userId || source?.id || '').trim();
                if (!memberId || seen.has(memberId)) return;
                const isLeft = !!source?.isLeft || Number(source?.leftAt || 0) > 0 || !!source?.leftState;
                if (isLeft && !includeLeft) return;
                seen.add(memberId);
                const resolved = resolvePeerDisplay(memberId);
                const presence = getParticipantPresenceState(source);
                out.push({
                    userId: memberId,
                    displayName: source?.displayName || resolved.displayName || resolved.name || memberId,
                    name: source?.name || resolved.name || resolved.displayName || memberId,
                    avatar: source?.avatar || resolved.avatar || '',
                    username: source?.username || resolved.username || '',
                    initials: source?.initials || resolved.initials || '',
                    isLeft,
                    leftAt: Math.max(0, Number(source?.leftAt || 0)) || 0,
                    online: presence.online,
                    lastSeenAt: presence.lastSeenAt
                });
            };
            if (Array.isArray(chat?.group?.participants)) {
                chat.group.participants.forEach(pushParticipant);
            }
            if (Array.isArray(chat?.group?.members)) {
                chat.group.members.forEach((memberId) => pushParticipant({ userId: String(memberId || '') }));
            }
            return out;
        }

        function getGroupLeaveStateClient(chat, userId = '') {
            if (!chat || !isGroupMessengerChat(chat)) return null;
            const targetUserId = String(userId || authProfile?.appUserId || '').trim();
            if (!targetUserId) return null;
            const participant = Array.isArray(chat.group?.participants)
                ? chat.group.participants.find((item) => String(item?.userId || '').trim() === targetUserId)
                : null;
            const leftAt = Math.max(
                0,
                Number(
                    participant?.leftAt
                    || participant?.restriction?.leftAt
                    || chat.group?.leftState?.leftAt
                    || chat.group?.leftState?.frozenAt
                    || 0
                )
            ) || 0;
            const isLeft = !!participant?.isLeft || leftAt > 0 || !!chat.group?.leftState;
            if (!isLeft) return null;
            return {
                leftAt,
                frozenAt: leftAt || Math.max(0, Number(chat.group?.leftState?.frozenAt || 0)) || 0
            };
        }

        function isGroupParticipantOnline(participant) {
            if (!participant || typeof participant !== 'object') return false;
            return getParticipantPresenceState(participant).online;
        }

        function getGroupParticipantDisplayName(chat, userId) {
            const id = String(userId || '').trim();
            if (!id) return '';
            const participant = getGroupChatParticipants(chat).find((item) => String(item?.userId || item?.id || '') === id);
            if (participant) {
                return String(participant.displayName || participant.name || participant.userId || '').trim() || id;
            }
            const resolved = resolvePeerDisplay(id);
            return String(resolved.displayName || resolved.name || id).trim() || id;
        }

        function getGroupChatTypingState(chat) {
            if (!chat || !isGroupMessengerChat(chat)) return null;
            const chatId = String(chat.id || '').trim();
            if (!chatId) return null;
            const myId = String(authProfile?.appUserId || '').trim();
            const members = new Set(
                getGroupChatParticipants(chat)
                    .map((item) => String(item?.userId || item?.id || '').trim())
                    .filter(Boolean)
            );
            const textEntries = [];
            const voiceEntries = [];
            messengerTypingByUser.forEach((rawState, rawUserId) => {
                const userId = String(rawUserId || '').trim();
                if (!userId || userId === myId) return;
                const state = rawState && typeof rawState === 'object'
                    ? rawState
                    : (rawState ? { isTyping: true, activity: 'text', ts: Date.now() } : null);
                if (!state || !state.isTyping) return;
                const stateChatId = String(state.chatId || '').trim();
                if (stateChatId && stateChatId !== chatId) return;
                if (!stateChatId && members.size && !members.has(userId)) return;
                const entry = {
                    userId,
                    name: getGroupParticipantDisplayName(chat, userId),
                    ts: Number(state.ts || 0) || Date.now()
                };
                if (state.activity === 'voice') voiceEntries.push(entry);
                else textEntries.push(entry);
            });
            const entries = textEntries.length ? textEntries : voiceEntries;
            if (!entries.length) return null;
            entries.sort((a, b) => a.ts - b.ts);
            return {
                activity: textEntries.length ? 'text' : 'voice',
                entries
            };
        }

        function formatGroupedActivityNames(names) {
            const list = Array.isArray(names) ? names.filter(Boolean) : [];
            if (!list.length) return '';
            if (list.length === 1) return list[0];
            if (list.length === 2) return `${list[0]} и ${list[1]}`;
            if (list.length === 3) return `${list[0]}, ${list[1]} и ${list[2]}`;
            return `${list[0]}, ${list[1]} и ${list.length - 2}`;
        }

        function copyTextToClipboard(text, okMessage = 'Скопировано') {
            const value = String(text || '').trim();
            if (!value) return Promise.resolve(false);
            const fallback = () => {
                const ta = document.createElement('textarea');
                ta.value = value;
                ta.setAttribute('readonly', '');
                ta.style.position = 'fixed';
                ta.style.left = '-9999px';
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                ta.remove();
            };
            return Promise.resolve()
                .then(() => {
                    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
                        return navigator.clipboard.writeText(value);
                    }
                    fallback();
                })
                .then(() => {
                    showNotification('', okMessage, 'info');
                    return true;
                })
                .catch(() => {
                    try {
                        fallback();
                        showNotification('', okMessage, 'info');
                        return true;
                    } catch (_) {
                        showNotification('', 'Не удалось скопировать', 'warning');
                        return false;
                    }
                });
        }

        function isEditableClipboardTarget(target) {
            try {
                const el = target && target.closest ? target.closest('input,textarea,[contenteditable="true"]') : null;
                return !!el;
            } catch (_) {
                return false;
            }
        }

        function initMessengerAntiCopyGuards() {
            if (window.__seychAntiCopyInit) return;
            window.__seychAntiCopyInit = true;
            const blockIfNotEditable = (e) => {
                if (isEditableClipboardTarget(e?.target)) return;
                try { e.preventDefault(); } catch (_) {}
            };
            document.addEventListener('copy', blockIfNotEditable, true);
            document.addEventListener('cut', blockIfNotEditable, true);
            document.addEventListener('paste', blockIfNotEditable, true);
            document.addEventListener('selectstart', blockIfNotEditable, true);
            document.addEventListener('contextmenu', blockIfNotEditable, true);
            document.addEventListener('dragstart', (e) => {
                const tag = String(e?.target?.tagName || '').toUpperCase();
                if (tag === 'IMG' || tag === 'VIDEO') {
                    try { e.preventDefault(); } catch (_) {}
                }
            }, true);
        }

        function getPeerByUsername(username, chat = null) {
            const key = normalizeMentionUsername(username);
            if (!key) return null;
            const candidates = [];
            const pushPeer = (peer) => {
                if (!peer || !peer.id) return;
                const uname = normalizeMentionUsername(peer.username || '');
                if (uname !== key) return;
                if (!candidates.some((item) => String(item.id) === String(peer.id))) candidates.push(peer);
            };

            // If chat context is provided, restrict search to that chat's participants
            if (chat && isGroupMessengerChat(chat)) {
                const members = getGroupChatParticipants(chat, { includeLeft: true }) || [];
                members.forEach((member) => {
                    const memberId = String(member?.userId || member?.id || '').trim();
                    if (!memberId) return;
                    const resolved = resolvePeerDisplay(memberId);
                    pushPeer({
                        ...resolved,
                        id: memberId,
                        username: member?.username || resolved?.username || '',
                        displayName: member?.displayName || resolved?.displayName || resolved?.name || memberId,
                        name: member?.name || resolved?.name || resolved?.displayName || memberId
                    });
                });
                return candidates[0] || null;
            }
            if (chat && isDirectMessengerChat(chat) && chat.peer?.id) {
                // Direct chat: only the peer
                pushPeer(resolvePeerDisplay(chat.peer.id));
                return candidates[0] || null;
            }

            // Fallback: if no chat context or unknown, search globally (active peer, friends, all chats) - this maintains compatibility for other uses
            if (messengerActivePeerId) pushPeer(resolvePeerDisplay(messengerActivePeerId));
            (friendsState.friends || []).forEach((friend) => pushPeer(resolvePeerDisplay(friend.id)));
            (messengerChats || []).forEach((item) => {
                if (isDirectMessengerChat(item) && item.peer?.id) pushPeer(resolvePeerDisplay(item.peer.id));
                if (isGroupMessengerChat(item)) {
                    getGroupChatParticipants(item, { includeLeft: true }).forEach((member) => {
                        const memberId = String(member?.userId || '').trim();
                        if (!memberId) return;
                        pushPeer({
                            ...resolvePeerDisplay(memberId),
                            id: memberId,
                            username: member?.username || '',
                            displayName: member?.displayName || member?.name || memberId,
                            name: member?.name || member?.displayName || memberId
                        });
                    });
                }
            });
            return candidates[0] || null;
        }

        function openMentionProfile(username) {
            const activeChat = resolveActiveMessengerChat();
            const peer = getPeerByUsername(username, activeChat);
            if (peer?.id) {
                openUserProfile(peer.id);
                return;
            }
            const uname = normalizeMentionUsername(username);
            if (!uname) return;
            pendingMentionProfileOpens.add(uname);
            sendMessengerEvent({ type: 'messenger-resolve-username', username: uname });
        }

        function normalizeMentionUsername(value) {
            return String(value || '')
                .replace(/^@+/, '')
                .trim()
                .toLowerCase()
                .replace(/[^a-z0-9_]/g, '')
                .slice(0, 32);
        }

        function getMyMentionUsername() {
            const u = normalizeMentionUsername(messengerProfile?.username || authProfile?.vkUsername || authProfile?.username || '');
            return u;
        }

        function doesMessageMentionMe(text) {
            const raw = String(text || '');
            if (!raw) return false;
            const myId = String(authProfile?.appUserId || '').trim();
            if (myId && raw.includes(`[[user:${myId}|`)) return true;
            const myUsername = getMyMentionUsername();
            if (!myUsername) return false;
            const esc = myUsername.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const re = new RegExp(`(^|[^a-zA-Z0-9_])@${esc}(?=$|[^a-zA-Z0-9_])`, 'i');
            return re.test(raw);
        }

        function parseSystemUserTags(text) {
            const raw = String(text || '');
            const out = [];
            raw.replace(/\[\[user:([^\]|]{1,220})\|([^\]]{1,220})\]\]/g, (_, userId, label) => {
                out.push({
                    userId: String(userId || '').trim(),
                    label: String(label || '').trim()
                });
                return _;
            });
            return out;
        }

        function buildSystemNotificationFromMessage(chatId, msg) {
            const myId = String(authProfile?.appUserId || '').trim();
            const raw = String(msg?.text || '');
            if (!myId || String(msg?.messageKind || '') !== 'system' || !raw.includes(`[[user:${myId}|`)) return null;
            const tags = parseSystemUserTags(raw);
            const actor = tags[0] || null;
            const target = tags.find((tag) => String(tag?.userId || '') === myId) || null;
            if (!actor?.userId || !target) return null;
            
            const actorPeer = resolvePeerDisplay(actor.userId);
            const actorAvatar = String(actorPeer?.avatar || '').trim();
            const actorInitials = String(actorPeer?.initials || (actor.label || '').split(/\s+/).filter(Boolean).map((p) => p.charAt(0)).join('').slice(0, 2).toUpperCase() || '').trim();
            const durationMatch = raw.match(/на\s+([^.\n]+?)(?=\s+\[\[user:|\s+в чате|\.|$)/i);
            const reasonMatch = raw.match(/причина:\s*([^.\n]+)/i);
            const duration = String(durationMatch?.[1] || '').trim();
            const reason = String(reasonMatch?.[1] || '').trim();
            
            const lower = raw.toLowerCase();
            let text = '';
            if (lower.includes('добавил(а)') && lower.includes('в чат')) {
                text = `${actor.label} добавил(а) вас в чат`;
            } else if (lower.includes('исключил(а)')) {
                text = `${actor.label} исключил(а) вас из чата`;
            } else if (lower.includes('выдал(а) мут')) {
                text = `${actor.label} выдал(а) вам мут`;
            } else if (lower.includes('снял(а) мут')) {
                text = `${actor.label} снял(а) с вас мут`;
            } else if (lower.includes('выдал(а) блокировку чата')) {
                text = `${actor.label} заблокировал(а) вас в чате`;
            } else if (lower.includes('снял(а) блокировку чата')) {
                text = `${actor.label} снял(а) с вас блокировку чата`;
            } else {
                return null;
            }
            return {
                id: `system:${String(chatId || '').trim()}:${String(msg?.id || '').trim()}`,
                type: 'system',
                ...getMessengerNotificationChatMeta(chatId),
                actorId: actor.userId,
                actorName: actor.label || actor.userId,
                actorAvatar,
                actorInitials,
                messageId: String(msg?.id || '').trim(),
                title: 'Событие чата',
                text,
                duration,
                reason,
                createdAt: Number(msg?.createdAt || 0) || Date.now()
            };
        }

        function recordMessengerMention(chatId, msg) {
            const cid = String(chatId || '').trim();
            const mid = String(msg?.id || '').trim();
            if (!cid || !mid) return;
            const fromId = String(msg?.fromId || '').trim();
            const createdAt = Number(msg?.createdAt || 0) || Date.now();
            const preview = getMessageCopyableText(msg).slice(0, 180);
            const fromName = String(msg?.senderDisplayName || fromId || 'Пользователь');
            const fromAvatar = String(msg?.senderAvatar || '').trim();
            const fromInitials = String(msg?.senderInitials || (fromName || '').split(/\s+/).filter(Boolean).map((p) => p.charAt(0)).join('').slice(0, 2).toUpperCase() || '').trim();
            messengerMentions = [{ chatId: cid, messageId: mid, fromId, fromName, createdAt, preview }, ...(messengerMentions || [])].slice(0, 200);
            pushMessengerNotification({
                id: `mention:${cid}:${mid}`,
                type: 'mention',
                ...getMessengerNotificationChatMeta(cid),
                actorId: fromId,
                actorName: fromName,
                actorAvatar: fromAvatar,
                actorInitials: fromInitials,
                messageId: mid,
                title: 'Упоминание',
                text: `${fromName} упомянул(а) вас`,
                createdAt
            });
            if (String(messengerActiveChatId || '') !== cid || messengerView !== 'chats') {
                const prev = getMessengerMentionUnreadForChat(cid);
                setMessengerMentionUnreadForChat(cid, prev + 1);
            }
        }

        function getGroupChatStatusText(chat) {
            const typingState = getGroupChatTypingState(chat);
            if (typingState && Array.isArray(typingState.entries) && typingState.entries.length) {
                const names = typingState.entries.map((entry) => entry.name);
                const verb = typingState.activity === 'voice'
                    ? (names.length === 1 ? 'записывает аудио' : 'записывают аудио')
                    : (names.length === 1 ? 'печатает' : 'печатают');
                return `${formatGroupedActivityNames(names)} ${verb}`;
            }
            const participants = getGroupChatParticipants(chat);
            const count = participants.length;
            if (!count) return 'Групповой чат';
            const onlineCount = participants.filter(isGroupParticipantOnline).length;
            return `${count} участников, ${onlineCount} онлайн`;
        }

        function playIncomingMessengerSound() {
            try {
                const base = getBasePath().replace(/\/$/, '');
                const a = new Audio(`${window.location.origin}${base}/upload/message.mp3`);
                a.volume = 0.55;
                a.play().catch(() => {});
            } catch (_) {}
        }

         function applyMessengerPresencePatch(userId, online, lastSeenAt) {
             const pid = String(userId || '').trim();
             if (!pid) return;
             const ts = Number(lastSeenAt) || Date.now();
             messengerChats = (messengerChats || []).map((c) => {
                 const isDirectPeer = String(c.peer?.id || '') === pid;
                 const isGroupChat = isGroupMessengerChat(c);
                 if (!isDirectPeer && !isGroupChat) return c;
                 let nextChat = c;
                  if (isDirectPeer) {
                      nextChat = {
                          ...nextChat,
                          peer: {
                              ...(nextChat.peer || {}),
                              online: !!online,
                              lastSeenAt: ts
                          }
                      };
                  }
                  if (isGroupChat && Array.isArray(nextChat.group?.participants)) {
                      let changed = false;
                      const nextParticipants = nextChat.group.participants.map((participant) => {
                          if (String(participant?.userId || participant?.id || '') !== pid) return participant;
                          changed = true;
                          return {
                              ...participant,
                              online: !!online,
                              lastSeenAt: ts
                          };
                      });
                      if (changed) {
                          nextChat = {
                              ...nextChat,
                              group: {
                                  ...(nextChat.group || {}),
                                  participants: nextParticipants
                              }
                          };
                      }
                  }
                  if (nextChat !== c && isGroupChat) {
                      nextChat = {
                          ...nextChat,
                          peer: {
                              ...(nextChat.peer || {}),
                              statusText: getGroupChatStatusText(nextChat)
                          }
                      };
                  }
                  return nextChat;
             });
             // Update UI respecting defer
             if (shouldRenderMessengerUi()) {
                 if (shouldDeferTransientMessengerRender()) {
                     // Partial: update online indicator in sidebar for direct chats
                     const chatItem = document.querySelector(`.messenger-chat-item[data-peer-id="${pid}"]`);
                     if (chatItem) {
                         const indicator = chatItem.querySelector('.online-indicator');
                         if (indicator) {
                             indicator.style.display = online ? 'block' : 'none';
                         }
                     }
                      // Update active group chat status if this user is a participant
                      const activeChat = resolveActiveMessengerChat();
                      if (isGroupMessengerChat(activeChat)) {
                          const group = activeChat.group || {};
                          const participants = Array.isArray(group.participants) ? group.participants : [];
                          const members = Array.isArray(group.members) ? group.members : [];
                          const isParticipant = participants.some(p => String(p.userId || p.id || '') === pid) || members.includes(pid);
                          if (isParticipant) {
                              const statusEl = document.querySelector('.chat-header-status');
                              if (statusEl) {
                                  statusEl.textContent = getGroupChatStatusText(activeChat);
                              }
                          }
                      }
                 } else {
                     renderMainScreen();
                 }
             }
          }

          const STARTUP_VERSION_STORAGE_KEY = 'seych-runtime-signature';

         function hashIdentityPart(value, seed = 5381) {
            let hash = seed >>> 0;
            const input = String(value || '');
            for (let i = 0; i < input.length; i++) {
                hash = (((hash << 5) + hash) ^ input.charCodeAt(i)) >>> 0;
            }
            return hash.toString(16).padStart(8, '0');
        }

        function computeRuntimeSignature() {
            try {
                const scriptTag = document.currentScript || document.querySelector('script:last-of-type');
                const source = String(scriptTag?.textContent || '');
                const signature = hashIdentityPart(source, 2166136261) + hashIdentityPart(source, 5381);
                return signature;
            } catch (_) {
                return '';
            }
        }

        function hasRuntimeUpdated() {
            const signature = computeRuntimeSignature();
            if (!signature) return false;
            try {
                const previous = String(localStorage.getItem(STARTUP_VERSION_STORAGE_KEY) || '').trim();
                return previous !== signature;
            } catch (_) {
                return false;
            }
        }

        function persistRuntimeSignature() {
            const signature = computeRuntimeSignature();
            if (!signature) return;
            try {
                localStorage.setItem(STARTUP_VERSION_STORAGE_KEY, signature);
            } catch (_) {}
        }

        function showStartupLoader() {
            const root = document.getElementById('startupLoader');
            const titleEl = document.getElementById('startupLoaderTitle');
            const marqueeEl = document.getElementById('startupLoaderMarquee');
            if (!root || !titleEl || !marqueeEl) return;
            root.setAttribute('aria-hidden', 'false');
            root.classList.add('visible');
            const titles = ['Загружаем', 'Обновляем', 'Запускаем'];
            let frame = 0;
            if (startupLoaderTicker) {
                clearInterval(startupLoaderTicker);
                startupLoaderTicker = null;
            }
            startupLoaderTicker = setInterval(() => {
                const dots = '.'.repeat((frame % 3) + 1);
                titleEl.textContent = titles[Math.floor(frame / 2) % titles.length];
                marqueeEl.textContent = `Загрузка${dots}`;
                frame += 1;
            }, 360);
        }

        function hideStartupLoader() {
            const root = document.getElementById('startupLoader');
            if (startupLoaderTicker) {
                clearInterval(startupLoaderTicker);
                startupLoaderTicker = null;
            }
            if (!root) return;
            root.classList.remove('visible');
            root.setAttribute('aria-hidden', 'true');
        }

        async function startAppWithConditionalLoader() {
            const shouldShowLoader = hasRuntimeUpdated();
            if (shouldShowLoader) {
                showStartupLoader();
            }
            try {
                await bootApp();
                persistRuntimeSignature();
                if (shouldShowLoader) {
                    await new Promise((resolve) => setTimeout(resolve, 900));
                }
            } finally {
                hideStartupLoader();
            }
        }

        function buildExternalAccountKey(profile) {
            const provider = String(profile?.provider || '').trim().toLowerCase();
            if (!provider) return '';
            if (provider === 'telegram') {
                const telegramId = String(profile?.telegramId || '').trim();
                if (telegramId) return `telegram:${telegramId}`;
                const telegramUsername = String(profile?.username || '').trim().toLowerCase().replace(/^@+/, '');
                if (telegramUsername) return `telegram_username:${telegramUsername}`;
            }
            if (provider === 'vk') {
                const vkUserId = String(profile?.vkUserId || '').trim();
                if (vkUserId) return `vk:${vkUserId}`;
                const vkUsername = String(profile?.vkUsername || '').trim().toLowerCase();
                if (vkUsername) return `vk_username:${vkUsername}`;
            }
            return '';
        }

        function buildIdentityKeys(profile) {
            const keys = [];
            const push = (value) => {
                const key = String(value || '').trim().toLowerCase();
                if (!key || keys.includes(key)) return;
                keys.push(key);
            };
            const provider = String(profile?.provider || '').trim().toLowerCase();
            if (provider === 'telegram') {
                const telegramId = String(profile?.telegramId || '').trim();
                const telegramUsername = String(profile?.username || '').trim().toLowerCase().replace(/^@+/, '');
                if (telegramId) push(`telegram:${telegramId}`);
                if (telegramUsername) push(`telegram_username:${telegramUsername}`);
            }
            if (provider === 'vk') {
                const vkUserId = String(profile?.vkUserId || '').trim();
                const vkUsername = String(profile?.vkUsername || '').trim().toLowerCase();
                if (vkUserId) push(`vk:${vkUserId}`);
                if (vkUsername) push(`vk_username:${vkUsername}`);
            }
            const externalKey = buildExternalAccountKey(profile);
            if (externalKey) push(externalKey);
            return keys;
        }

        function buildStableAppUserId(profile, fallbackAppUserId = '') {
            const externalKey = buildExternalAccountKey(profile);
            if (!externalKey) {
                const fallback = String(profile?.appUserId || fallbackAppUserId || '').trim();
                return fallback || generateAppUserId();
            }
            const h1 = hashIdentityPart(externalKey, 5381);
            const h2 = hashIdentityPart(`seych:${externalKey}`, 2166136261);
            return `u${h1}${h2}`;
        }

        function getOutgoingCallStatusStorageKey() {
            const userId = String(authProfile?.appUserId || appUserId || '').trim();
            if (!userId) return '';
            return `seych-outgoing-statuses:${userId}`;
        }

        function loadKnownOutgoingCallStatuses() {
            const storageKey = getOutgoingCallStatusStorageKey();
            if (!storageKey) return;
            try {
                const raw = localStorage.getItem(storageKey);
                if (!raw) return;
                const parsed = JSON.parse(raw);
                if (!Array.isArray(parsed)) return;
                const restored = new Map();
                parsed.forEach((row) => {
                    if (!Array.isArray(row) || row.length < 2) return;
                    const inviteId = String(row[0] || '').trim();
                    const status = String(row[1] || '').trim();
                    if (!inviteId || !status) return;
                    restored.set(inviteId, status);
                });
                knownOutgoingCallStatuses = restored;
            } catch (_) {}
        }

        function persistKnownOutgoingCallStatuses() {
            const storageKey = getOutgoingCallStatusStorageKey();
            if (!storageKey) return;
            try {
                const rows = Array.from(knownOutgoingCallStatuses.entries()).slice(-200);
                localStorage.setItem(storageKey, JSON.stringify(rows));
            } catch (_) {}
        }

        function touchKnownOutgoingCallStatus(inviteId, status) {
            const normalizedInviteId = String(inviteId || '').trim();
            const normalizedStatus = String(status || '').trim();
            if (!normalizedInviteId || !normalizedStatus) return;
            knownOutgoingCallStatuses.delete(normalizedInviteId);
            knownOutgoingCallStatuses.set(normalizedInviteId, normalizedStatus);
        }

        function clearKnownOutgoingCallStatusesStorage() {
            const storageKey = getOutgoingCallStatusStorageKey();
            if (!storageKey) return;
            try {
                localStorage.removeItem(storageKey);
            } catch (_) {}
        }

        function generateAppUserId() {
            return `u${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
        }

        function getStoredFriendsNotifyValue() {
            try {
                const raw = localStorage.getItem('seych-friends-notify');
                if (raw === '0') return false;
                if (raw === '1') return true;
            } catch (_) {}
            return true;
        }

        function persistFriendsNotifyValue(enabled) {
            friendsNotificationsEnabled = !!enabled;
            try {
                localStorage.setItem('seych-friends-notify', friendsNotificationsEnabled ? '1' : '0');
            } catch (_) {}
            if (friendsNotificationsEnabled) {
                ensureSystemNotificationPermission(true).catch(() => {});
                ensurePushNotificationsReady().catch(() => {});
            } else {
                disablePushNotificationsSubscription().catch(() => {});
            }
            renderMainScreen();
            showNotification('Друзья', friendsNotificationsEnabled ? 'Уведомления включены' : 'Уведомления выключены', 'info');
        }

        async function ensureSystemNotificationPermission(requestAccess = false) {
            if (!('Notification' in window)) return false;
            if (Notification.permission === 'granted') return true;
            if (!requestAccess) return false;
            if (systemNotifyPermissionAsked && Notification.permission !== 'default') {
                return Notification.permission === 'granted';
            }
            systemNotifyPermissionAsked = true;
            try {
                const result = await Notification.requestPermission();
                return result === 'granted';
            } catch (_) {
                return false;
            }
        }

        function showSystemNotification(title, body, tag = '') {
            if (!friendsNotificationsEnabled) return;
            if (!('Notification' in window)) return;
            if (Notification.permission !== 'granted') return;
            try {
                new Notification(title, {
                    body: String(body || ''),
                    tag: tag || undefined,
                    silent: false
                });
            } catch (_) {}
        }

        function urlBase64ToUint8Array(base64String) {
            const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
            const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
            const rawData = atob(base64);
            return Uint8Array.from([...rawData].map((char) => char.charCodeAt(0)));
        }

        async function initPushNotifications() {
            if (HOSTING_SAFE_MODE) return;
            if (!authProfile?.appUserId || !friendsNotificationsEnabled) return;
            if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
            if (Notification.permission !== 'granted') return;
            try {
                const basePath = getBasePath().replace(/\/$/, '');
                const swUrl = `${basePath || ''}/sw.js`;
                if (!pushRegistration) {
                    pushRegistration = await navigator.serviceWorker.register(swUrl);
                }
                await navigator.serviceWorker.ready;
                const pushConfig = await friendsApiRequest('push_config');
                const publicKey = String(pushConfig?.publicKey || '').trim();
                if (!publicKey) return;
                let subscription = await pushRegistration.pushManager.getSubscription();
                if (!subscription) {
                    subscription = await pushRegistration.pushManager.subscribe({
                        userVisibleOnly: true,
                        applicationServerKey: urlBase64ToUint8Array(publicKey)
                    });
                }
                await friendsApiRequest('save_push_subscription', {
                    subscription: subscription.toJSON ? subscription.toJSON() : subscription
                });
                await syncPushContextToServiceWorker();
            } catch (_) {}
        }

        async function ensurePushNotificationsReady() {
            if (pushInitPromise) {
                await pushInitPromise;
                return;
            }
            pushInitPromise = initPushNotifications()
                .catch(() => {})
                .finally(() => {
                    pushInitPromise = null;
                });
            await pushInitPromise;
        }

        async function disablePushNotificationsSubscription() {
            if (HOSTING_SAFE_MODE) return;
            if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
            try {
                const basePath = getBasePath().replace(/\/$/, '');
                const swUrl = `${basePath || ''}/sw.js`;
                let registration = pushRegistration;
                if (!registration) {
                    registration = await navigator.serviceWorker.getRegistration(swUrl);
                }
                if (!registration) {
                    registration = await navigator.serviceWorker.ready.catch(() => null);
                }
                if (!registration) return;
                pushRegistration = registration;
                const subscription = await registration.pushManager.getSubscription();
                if (!subscription) return;
                await subscription.unsubscribe();
            } catch (_) {}
        }

        async function syncPushContextToServiceWorker() {
            if (HOSTING_SAFE_MODE) return;
            if (!('serviceWorker' in navigator)) return;
            const currentAppUserId = String(authProfile?.appUserId || appUserId || '').trim();
            if (!currentAppUserId) return;
            try {
                const basePath = getBasePath().replace(/\/$/, '');
                const swUrl = `${basePath || ''}/sw.js`;
                let registration = pushRegistration;
                if (!registration) {
                    registration = await navigator.serviceWorker.getRegistration(swUrl);
                }
                if (!registration) {
                    registration = await navigator.serviceWorker.ready.catch(() => null);
                }
                if (!registration?.active) return;
                pushRegistration = registration;
                registration.active.postMessage({
                    type: 'push-context',
                    appUserId: currentAppUserId
                });
            } catch (_) {}
        }

        function getReconnectKey() {
            if (reconnectKey) return reconnectKey;
            try {
                const saved = sessionStorage.getItem(RECONNECT_KEY_STORAGE);
                if (saved && saved.length >= 10) {
                    reconnectKey = saved;
                    return reconnectKey;
                }
            } catch (_) {}
            reconnectKey = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 14)}`;
            try {
                sessionStorage.setItem(RECONNECT_KEY_STORAGE, reconnectKey);
            } catch (_) {}
            return reconnectKey;
        }

        function normalizeIceServers(list) {
            if (!Array.isArray(list)) return [];
            const out = [];
            list.forEach((item) => {
                if (!item || typeof item !== 'object') return;
                const urls = Array.isArray(item.urls)
                    ? item.urls.filter((u) => typeof u === 'string' && u.trim())
                    : typeof item.urls === 'string' && item.urls.trim()
                        ? item.urls.trim()
                        : null;
                if (!urls) return;
                const server = { urls };
                if (typeof item.username === 'string' && item.username) server.username = item.username;
                if (typeof item.credential === 'string' && item.credential) server.credential = item.credential;
                const key = JSON.stringify(server);
                if (!out.some((entry) => JSON.stringify(entry) === key)) {
                    out.push(server);
                }
            });
            return out;
        }

        function applyIceServersFromPayload(payload) {
            const serverList = normalizeIceServers(payload?.iceServers);
            if (!serverList.length) return;
            rtcIceServers = serverList;
        }

        function getAssetUrl(path) {
            const safePath = String(path || '').replace(/^\/+/, '');
            const basePath = `${window.location.origin}${getBasePath().replace(/\/$/, '')}`;
            return `${basePath}/${safePath}`;
        }

        function initSoundEffects() {
            joinSoundEffect = new Audio(getAssetUrl('upload/login.mp3'));
            joinSoundEffect.preload = 'auto';
            leaveSoundEffect = new Audio(getAssetUrl('upload/logut.mp3'));
            leaveSoundEffect.preload = 'auto';
            kickSoundEffect = new Audio(getAssetUrl('upload/kick.mp3'));
            kickSoundEffect.preload = 'auto';
            if (!incomingCallSound) {
                incomingCallSound = new Audio(getAssetUrl('upload/rington.mp3'));
                incomingCallSound.loop = true;
                incomingCallSound.preload = 'auto';
            }
        }

        function playSoundEffect(sound) {
            if (!sound) return;
            try {
                sound.currentTime = 0;
                const playResult = sound.play();
                if (playResult && typeof playResult.catch === 'function') {
                    playResult.catch(() => {});
                }
            } catch (_) {}
        }

        function resolveWsUrls() {
            // Только Render сервер
            return [WS_URL];
        }

        function proxifyAvatarUrl(url) {
            const raw = String(url || '').trim();
            if (!raw) return '';
            if (/^data:image\//i.test(raw)) return raw;
            if (raw.startsWith(AVATAR_PROXY_API)) return raw;
            let parsed;
            try {
                parsed = new URL(raw, window.location.origin);
            } catch (_) {
                return '';
            }
            if (!/^https?:$/i.test(parsed.protocol)) return '';
            const host = parsed.hostname.toLowerCase();
            const allowedHosts = [
                't.me',
                'telegram.org',
                'googleusercontent.com',
                'ggpht.com',
                'ytimg.com',
                'vk.com',
                'vkuser.net',
                'userapi.com'
            ];
            const hostAllowed = allowedHosts.some((entry) => host === entry || host.endsWith(`.${entry}`));
            if (!hostAllowed) return parsed.toString();
            return `${AVATAR_PROXY_API}${encodeURIComponent(parsed.toString())}`;
        }

        function showCustomPrompt(title, defaultValue, callback) {
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.innerHTML = `
                <div class="modal-content">
                    <h2><i class="fas fa-user"></i> ${title}</h2>
                    <input type="text" id="promptInput" class="modal-input" placeholder="Введите имя" value="${defaultValue}">
                    <div class="modal-buttons">
                        <button class="modal-btn cancel" id="promptCancel">Отмена</button>
                        <button class="modal-btn confirm" id="promptConfirm">Продолжить</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            
            document.getElementById('promptConfirm').onclick = () => {
                const val = document.getElementById('promptInput').value.trim();
                modal.remove();
                if (val) callback(val);
                else callback(defaultValue);
            };
            document.getElementById('promptCancel').onclick = () => {
                modal.remove();
                callback(defaultValue);
            };
        }

        function showCustomConfirm(title, message, onConfirm, onCancel) {
            const modal = document.createElement('div');
            modal.className = 'request-modal';
            modal.innerHTML = `
                <div class="request-content">
                    <div style="font-size: 48px;">${title.includes('камеру') ? '📹' : title.includes('микрофон') ? '🎤' : '❓'}</div>
                    <h3>${title}</h3>
                    <p>${message}</p>
                    <div class="request-buttons">
                        <button class="request-btn cancel" id="confirmCancel">Отмена</button>
                        <button class="request-btn confirm" id="confirmOk">OK</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            
            document.getElementById('confirmOk').onclick = () => {
                modal.remove();
                if (onConfirm) onConfirm();
            };
            document.getElementById('confirmCancel').onclick = () => {
                modal.remove();
                if (onCancel) onCancel();
            };
        }

        function formatTime(seconds) {
            const safe = Math.max(0, seconds);
            const h = Math.floor(safe / 3600);
            const m = Math.floor((safe % 3600) / 60);
            const s = safe % 60;
            if (h > 0) return `${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
            return `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
        }

        function clearOutgoingFriendCallTimeout() {
            if (!outgoingFriendCallTimeout) return;
            clearTimeout(outgoingFriendCallTimeout);
            outgoingFriendCallTimeout = null;
        }

        function isOutgoingFriendCallConnecting() {
            return !!(outgoingFriendCallSession && !outgoingFriendCallSession.answered);
        }

        function getOutgoingCallDots() {
            const sequence = [' ···', '.··', '..·', '...'];
            const step = Math.floor(Date.now() / 320) % sequence.length;
            return sequence[step];
        }

        function applyCallConnectionBadges() {
            const connecting = isOutgoingFriendCallConnecting();
            const privacyIslandBadge = document.getElementById('privacyIslandBadge');
            const privacyIslandLabel = document.getElementById('privacyIslandLabel');
            const roomPrivacyBadge = document.getElementById('roomPrivacyBadge');

            if (privacyIslandBadge) {
                if (connecting) {
                    privacyIslandBadge.className = 'room-status connecting';
                    privacyIslandBadge.title = 'Соединение';
                    privacyIslandBadge.innerHTML = '<i class="fas fa-circle-notch fa-spin"></i>';
                } else {
                    privacyIslandBadge.className = `room-status ${roomIsPrivate ? 'private' : 'public'}`;
                    privacyIslandBadge.title = roomIsPrivate ? 'Закрытая' : 'Публичная';
                    privacyIslandBadge.innerHTML = `<i class="fas ${roomIsPrivate ? 'fa-lock' : 'fa-globe'}"></i>`;
                }
            }
            if (privacyIslandLabel) {
                privacyIslandLabel.textContent = connecting ? 'Соединение' : (roomIsPrivate ? 'Приватный' : 'Публичный');
            }
            if (roomPrivacyBadge) {
                if (connecting) {
                    roomPrivacyBadge.className = 'room-status connecting';
                    roomPrivacyBadge.title = 'Соединение';
                    roomPrivacyBadge.innerHTML = '<i class="fas fa-circle-notch fa-spin"></i>';
                } else {
                    roomPrivacyBadge.className = `room-status ${roomIsPrivate ? 'private' : 'public'}`;
                    roomPrivacyBadge.title = roomIsPrivate ? 'Закрытая' : 'Публичная';
                    roomPrivacyBadge.innerHTML = `<i class="fas ${roomIsPrivate ? 'fa-lock' : 'fa-globe'}"></i>`;
                }
            }
        }

        function updateCallTimerDisplay() {
            const timerElement = document.getElementById('callTimer');
            const emptyTimerElement = document.getElementById('emptyCallTimer');
            let text = '00:00';
            if (isOutgoingFriendCallConnecting()) {
                const dots = getOutgoingCallDots();
                text = `Звоним ${outgoingFriendCallSession.targetName || 'другу'}${dots}`;
            } else if (!isConnected || wsReconnectTimer || (ws && ws.readyState !== WebSocket.OPEN)) {
                text = 'Соединение...';
            } else if (callStartTime) {
                const elapsed = Math.floor((Date.now() - callStartTime) / 1000);
                text = formatTime(Math.max(0, elapsed));
            }
            if (timerElement) timerElement.textContent = text;
            if (emptyTimerElement) emptyTimerElement.textContent = text;
            const islandTimer = document.getElementById('callIslandTimer');
            if (islandTimer) islandTimer.textContent = text;
            applyCallConnectionBadges();
        }

        function clearOutgoingFriendCallSession() {
            clearOutgoingFriendCallTimeout();
            outgoingFriendCallSession = null;
            updateCallTimerDisplay();
        }

        function cancelPendingOutgoingFriendCall(reason = '') {
            const activeSession = outgoingFriendCallSession;
            if (!activeSession?.inviteId || activeSession.answered) return;
            friendsApiRequest('cancel_call_invite', { invite_id: activeSession.inviteId, reason })
                .catch(() => {});
        }

        function startOutgoingFriendCallSession(session) {
            const targetName = String(session?.targetName || 'другу').trim();
            outgoingFriendCallSession = {
                inviteId: String(session?.inviteId || '').trim(),
                roomId: String(session?.roomId || '').trim(),
                targetId: String(session?.targetId || '').trim(),
                targetName,
                startedAt: Date.now(),
                answered: false
            };
            clearOutgoingFriendCallTimeout();
            outgoingFriendCallTimeout = setTimeout(() => {
                const current = outgoingFriendCallSession;
                if (!current || current.answered) return;
                if (roomId && current.roomId && roomId === current.roomId) {
                    showNotification('Звонок другу', `${current.targetName} не ответил за 1 минуту`, 'warning');
                    cancelPendingOutgoingFriendCall('timeout');
                    endCall(false);
                }
            }, 60000);
            updateCallTimerDisplay();
        }

        function acceptOutgoingFriendCallSession() {
            if (!outgoingFriendCallSession) return;
            outgoingFriendCallSession.answered = true;
            clearOutgoingFriendCallTimeout();
            callStartTime = Date.now();
            updateCallTimerDisplay();
        }

        function startCallTimer() {
            if (callTimerInterval) clearInterval(callTimerInterval);
            callStartTime = Date.now();
            updateCallTimerDisplay();
            callTimerInterval = setInterval(() => {
                updateCallTimerDisplay();
            }, 1000);
        }

        function stopCallTimer() {
            if (callTimerInterval) {
                clearInterval(callTimerInterval);
                callTimerInterval = null;
            }
            callStartTime = null;
        }

        function resetCallState() {
            stopCallAudioHealTimer();
            stopConnectionQualityMonitor();
            clearAvRecoveryTimers();
            cancelAnimationFrame(animationId);
            currentGroupCallChatId = '';
            currentGroupCallTitle = '';
            if (audioContextRef && audioContextRef.state !== 'closed') {
                try { audioContextRef.close(); } catch (_) {}
            }
            audioContextRef = null;
            isConnected = false;
            myId = null;
            ownerId = null;
            currentContextTargetId = null;
            remoteName = '';
            remoteAvatar = '';
            remoteVideo = false;
            remoteAudio = true;
            remoteScreen = false;
            isScreenSharing = false;
            isSpeaking = false;
            remoteSpeaking = false;
            peers.forEach(p => { try { p.destroy(); } catch (_) {} });
            peers.clear();
            participants.clear();
            participantAvatars.clear();
            participantStates.clear();
            screenConnMap.clear();
            localScreenShareId = null;
            videoTiles.forEach(tile => { try { tile.remove(); } catch (_) {} });
            videoTiles.clear();
            screenTiles.forEach(tile => { try { tile.remove(); } catch (_) {} });
            screenTiles.clear();
            removeWatchPartyTile();
            watchPartyState = null;
            remoteMediaStreams.clear();
            stopRemoteAudio();
            roomIsPrivate = false;
            pendingJoinRequests = [];
            participantConnectionQuality.clear();
            clearOutgoingFriendCallSession();
            if (joinPendingModal) {
                try { joinPendingModal.remove(); } catch (_) {}
                joinPendingModal = null;
            }
            if (roomSettingsMenu) {
                try { roomSettingsMenu.remove(); } catch (_) {}
                roomSettingsMenu = null;
            }
            if (videoTrack) {
                videoTrack.stop();
                videoTrack = null;
            }
            if (cameraSourceTrack && cameraSourceTrack !== videoTrack) {
                try { cameraSourceTrack.stop(); } catch (_) {}
            }
            cameraSourceTrack = null;
            selfPreviewTrack = null;
            if (outgoingTrackCleanup) {
                try { outgoingTrackCleanup(); } catch (_) {}
            }
            outgoingTrackCleanup = null;
            try {
                if (localStream) {
                    localStream.getTracks().forEach((t) => {
                        try { t.stop(); } catch (_) {}
                    });
                }
            } catch (_) {}
            localStream = null;
            rawMicTrack = null;
            cameraFacingMode = 'user';
            cameraSwitchInProgress = false;
            try {
                const csr = document.getElementById('callScreenRoot');
                if (csr) csr.remove();
            } catch (_) {}
        }

        function updateCreatorFlag() {
            const oid = String(ownerId ?? '');
            const mid = String(myId ?? '');
            isCreator = !!mid && oid === mid;
        }

        function getParticipantState(participantId) {
            if (!participantId) return null;
            if (!participantStates.has(participantId)) {
                participantStates.set(participantId, {
                    id: participantId,
                    userName: participants.get(participantId) || '',
                    userAvatar: participantAvatars.get(participantId) || '',
                    video: false,
                    audio: true,
                    screen: false,
                    speaking: false,
                    isAdmin: false,
                    cameraFacingMode: '',
                    appUserId: ''
                });
            }
            return participantStates.get(participantId);
        }

        function upsertParticipantState(raw) {
            if (!raw || !raw.id) return;
            participants.set(raw.id, raw.userName || participants.get(raw.id) || '');
            participantAvatars.set(raw.id, raw.userAvatar || participantAvatars.get(raw.id) || '');
            const state = getParticipantState(raw.id);
            state.id = raw.id;
            state.userName = raw.userName || state.userName || '';
            state.userAvatar = raw.userAvatar || state.userAvatar || '';
            if (typeof raw.video === 'boolean') state.video = raw.video;
            if (typeof raw.audio === 'boolean') state.audio = raw.audio;
            if (typeof raw.screen === 'boolean') state.screen = raw.screen;
            if (typeof raw.speaking === 'boolean') state.speaking = raw.speaking;
            if (typeof raw.isAdmin === 'boolean') state.isAdmin = raw.isAdmin;
            if (typeof raw.cameraFacingMode === 'string') state.cameraFacingMode = normalizeFacingMode(raw.cameraFacingMode, '');
            if (typeof raw.appUserId === 'string') state.appUserId = raw.appUserId;
        }

        function removeParticipantState(participantId) {
            participants.delete(participantId);
            participantAvatars.delete(participantId);
            participantStates.delete(participantId);
            participantConnectionQuality.delete(participantId);
            audioRecoverCooldown.delete(participantId);
            connectionNoticeCooldown.delete(participantId);
            remoteMediaStreams.delete(participantId);
            stopRemoteAudio(participantId);
            const timerId = avPeerRecoverTimers.get(participantId);
            if (timerId) {
                clearTimeout(timerId);
                avPeerRecoverTimers.delete(participantId);
            }
        }

        function getRemoteParticipantIds() {
            return Array.from(participantStates.keys()).filter(id => !!id && id !== myId);
        }

        function shouldInitiatePeer(localId, remoteId) {
            if (!localId || !remoteId) return false;
            return String(localId) < String(remoteId);
        }

        function isAvPeerHealthy(peer) {
            if (!peer || peer.destroyed) return false;
            if (peer.connected) return true;
            const pc = peer._pc;
            if (!pc) return true;
            const iceState = String(pc.iceConnectionState || '').toLowerCase();
            const connState = String(pc.connectionState || '').toLowerCase();
            // disconnected — состояние, которое можно вылечить ICE-restart; не считаем его "мертвым" сразу.
            if (['failed', 'closed'].includes(iceState) || ['failed', 'closed'].includes(connState)) {
                return false;
            }
            return true;
        }

        function ensureAvPeerForParticipant(participantId, initiator = null) {
            if (!participantId || participantId === myId || !localStream) return null;
            const avKey = `av-${participantId}`;
            const existing = peers.get(avKey);
            if (isAvPeerHealthy(existing)) {
                return existing;
            }
            if (existing && !existing.destroyed) {
                try { existing.destroy(); } catch (_) {}
            }
            peers.delete(avKey);
            const state = getParticipantState(participantId);
            const shouldInitiate = typeof initiator === 'boolean' ? initiator : shouldInitiatePeer(myId, participantId);
            const avPeer = createPeer(localStream, 'video', shouldInitiate, participantId, state.userName || '');
            peers.set(avKey, avPeer);
            return avPeer;
        }

        function getConnectionQuality(participantId) {
            return participantConnectionQuality.get(participantId) || 'normal';
        }

        async function refreshConnectionQuality() {
            if (!roomId || !isConnected) return;
            if (connectionQualityBusy) return;
            connectionQualityBusy = true;
            let changed = false;
            try {
                const keys = Array.from(peers.keys()).filter((key) => key.startsWith('av-'));
                for (const key of keys) {
                    const participantId = key.slice(3);
                    if (!participantId) continue;
                    const prevLevel = participantConnectionQuality.get(participantId) || 'normal';
                    const peer = peers.get(key);
                    if (!peer || peer.destroyed) {
                        if (participantConnectionQuality.get(participantId) !== 'weak') {
                            participantConnectionQuality.set(participantId, 'weak');
                            changed = true;
                        }
                        continue;
                    }
                    const pc = peer._pc;
                    const iceState = String(pc?.iceConnectionState || '').toLowerCase();
                    if (iceState === 'disconnected') {
                        // Это не "плохой RTT", а реальное переподключение.
                        if (participantConnectionQuality.get(participantId) !== 'reconnecting') {
                            participantConnectionQuality.set(participantId, 'reconnecting');
                            changed = true;
                        }
                        continue;
                    }

                    let level = peer.connected ? 'good' : 'normal';
                    if (pc && typeof pc.getStats === 'function') {
                        try {
                            const stats = await pc.getStats();
                            let rtt = null;
                            stats.forEach((report) => {
                                if (report.type === 'candidate-pair' && (report.state === 'succeeded' || report.nominated) && typeof report.currentRoundTripTime === 'number') {
                                    rtt = report.currentRoundTripTime;
                                }
                            });
                            if (typeof rtt === 'number') {
                                if (rtt <= 0.12) level = 'good';
                                else if (rtt <= 0.28) level = 'normal';
                                else level = 'weak';
                            } else if (peer.connected) {
                                level = 'normal';
                            }
                        } catch (_) {
                            if (peer.connected) level = 'normal';
                        }
                    }
                    if (participantConnectionQuality.get(participantId) !== level) {
                        participantConnectionQuality.set(participantId, level);
                        changed = true;
                    }
                    const nextLevel = participantConnectionQuality.get(participantId) || level;
                    if (prevLevel !== nextLevel) {
                        const state = getParticipantState(participantId);
                        const participantName = state?.userName || 'собеседником';
                        const now = Date.now();
                        const lastNotice = connectionNoticeCooldown.get(participantId) || 0;
                        if (nextLevel === 'reconnecting' && now - lastNotice > 12000) {
                            showNotification('Связь', `Переподключение с ${participantName}…`, 'info', '<i class="fas fa-sync-alt"></i>');
                            connectionNoticeCooldown.set(participantId, now);
                        } else if (nextLevel === 'weak' && now - lastNotice > 15000) {
                            showNotification('Связь', `Плохая связь с ${participantName}`, 'warning', '<i class="fas fa-signal"></i>');
                            connectionNoticeCooldown.set(participantId, now);
                        }
                    }
                }
                getRemoteParticipantIds().forEach((id) => {
                    if (!keys.includes(`av-${id}`) && participantConnectionQuality.get(id) !== 'weak') {
                        participantConnectionQuality.set(id, 'weak');
                        changed = true;
                    }
                });
                healRemoteAudioLinks();
            } finally {
                connectionQualityBusy = false;
            }
            if (changed) updateUI();
        }

        function improveVideoSdpQuality(sdp, bitrateKbps = 1200) {
            if (!sdp || typeof sdp !== 'string') return sdp;
            const lines = sdp.split('\r\n');
            const out = [];
            let inVideo = false;
            const safeBitrate = Number.isFinite(bitrateKbps) ? Math.max(512, Math.min(5000, Math.floor(bitrateKbps))) : 1200;
            for (const line of lines) {
                if (line.startsWith('m=')) {
                    inVideo = line.startsWith('m=video');
                    out.push(line);
                    continue;
                }
                if (inVideo && line.startsWith('b=AS:')) {
                    continue;
                }
                out.push(line);
                if (inVideo && line.startsWith('c=')) {
                    out.push(`b=AS:${safeBitrate}`);
                }
            }
            return out.join('\r\n');
        }

        function updatePrimaryRemoteState() {
            const firstRemoteId = getRemoteParticipantIds()[0] || null;
            if (!firstRemoteId) {
                remoteName = '';
                remoteAvatar = '';
                remoteVideo = false;
                remoteAudio = true;
                remoteScreen = false;
                remoteSpeaking = false;
                window.remoteIsAdmin = false;
                return;
            }
            const state = getParticipantState(firstRemoteId);
            remoteName = state.userName || participants.get(firstRemoteId) || '';
            remoteAvatar = state.userAvatar || participantAvatars.get(firstRemoteId) || '';
            remoteVideo = !!state.video;
            remoteAudio = !!state.audio;
            remoteScreen = !!state.screen;
            remoteSpeaking = !!state.speaking;
            window.remoteIsAdmin = !!state.isAdmin;
        }

        function showNotification(title, message, type = 'info', iconMarkup = '') {
            const container = document.getElementById('notifications');
            if (!container) return;
            const defaultIcon = type === 'success' ? '✅' : type === 'error' ? '❌' : type === 'warning' ? '⚠️' : 'ℹ️';
            const notif = document.createElement('div');
            notif.className = `notification ${type}`;
            notif.innerHTML = `
                <div class="notification-icon">${iconMarkup || defaultIcon}</div>
                <div class="notification-content">
                    <div class="notification-title">${escapeHtml(title)}</div>
                    <div class="notification-message">${escapeHtml(message)}</div>
                </div>
                <div class="notification-close" onclick="this.parentElement.remove()">✕</div>
            `;
            container.appendChild(notif);
            setTimeout(() => {
                if (notif.parentElement) notif.remove();
            }, 5000);
        }

        function escapeHtml(v) {
            return String(v || '')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        function renderMaybeMarqueeText(text, threshold = 100, baseClass = '') {
            const raw = String(text || '');
            const value = raw.trim();
            const cls = String(baseClass || '').trim();
            if (value.length > threshold) {
                const full = cls ? `${cls} seych-marquee` : 'seych-marquee';
                return `<span class="${escapeHtml(full)}"><span class="seych-marquee__inner">${escapeHtml(raw)}</span></span>`;
            }
            if (cls) return `<span class="${escapeHtml(cls)}">${escapeHtml(raw)}</span>`;
            return escapeHtml(raw);
        }

        function syncComposerMentionMenuDom(chatOverride = null) {
            const host = document.getElementById('composerMentionMenuHost');
            if (!host) return;
            const chat = chatOverride || resolveActiveMessengerChat();
            const html = renderComposerMentionMenu(chat);
            if (host.innerHTML !== html) host.innerHTML = html;
        }

        const LINKIFY_SKIP_TLDS = new Set([
            'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'mp3', 'mp4', 'webm', 'zip', 'rar', '7z',
            'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'xml', 'csv', 'log', 'js', 'css', 'map', 'json', 'wasm'
        ]);

        function linkifyOverlaps(intervals, s, e) {
            return intervals.some((it) => !(e <= it.start || s >= it.end));
        }

        /**
         * Делает URL в тексте кликабельными; опционально добавляет блок для превью первой ссылки.
         * @param {string} raw
         * @param {{ includePreview?: boolean }} opts
         */
        function linkifyMessengerText(raw, opts) {
            const includePreview = !!(opts && opts.includePreview);
            const chat = opts && opts.chat ? opts.chat : resolveActiveMessengerChat();
            const text = String(raw || '');
            if (!text) return '';
            const intervals = [];

            const reSysUserTag = /\[\[user:([^\]|]{1,220})\|([^\]]{1,220})\]\]/g;
            let m;
            while ((m = reSysUserTag.exec(text)) !== null) {
                const userId = String(m[1] || '').trim();
                const label = String(m[2] || '').trim();
                if (!userId || !label) continue;
                intervals.push({
                    start: m.index,
                    end: reSysUserTag.lastIndex,
                    raw: m[0],
                    type: 'user_tag',
                    userId,
                    label
                });
            }

            const reProto = /https?:\/\/[^\s<>"']+/gi;
            while ((m = reProto.exec(text)) !== null) {
                intervals.push({
                    start: m.index,
                    end: reProto.lastIndex,
                    raw: m[0],
                    href: m[0]
                });
            }

            const reWww = /www\.[^\s<>"']+/gi;
            while ((m = reWww.exec(text)) !== null) {
                if (linkifyOverlaps(intervals, m.index, reWww.lastIndex)) continue;
                intervals.push({
                    start: m.index,
                    end: reWww.lastIndex,
                    raw: m[0],
                    href: 'https://' + m[0]
                });
            }

            const reBare = /(^|[^\w@/])((?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,}|xn--[a-z0-9-]+)(?:\/[^\s<>"']*)?)/gi;
            while ((m = reBare.exec(text)) !== null) {
                const start = m.index + m[1].length;
                const rawUrl = m[2];
                const end = start + rawUrl.length;
                if (linkifyOverlaps(intervals, start, end)) continue;
                const hostPart = rawUrl.split('/')[0];
                const tld = hostPart.split('.').pop().toLowerCase();
                if (LINKIFY_SKIP_TLDS.has(tld)) continue;
                intervals.push({
                    start,
                    end,
                    raw: rawUrl,
                    href: 'https://' + rawUrl
                });
            }

            const reMention = /(^|[^a-zA-Z0-9])@([a-zA-Z0-9]{3,32})/g;
            while ((m = reMention.exec(text)) !== null) {
                const start = m.index + m[1].length;
                const rawMention = `@${m[2]}`;
                const end = start + rawMention.length;
                if (linkifyOverlaps(intervals, start, end)) continue;
                const peer = getPeerByUsername(m[2], chat);
                intervals.push({
                    start,
                    end,
                    raw: rawMention,
                    type: 'mention',
                    username: m[2],
                    userId: peer?.id || ''
                });
            }

            intervals.sort((a, b) => a.start - b.start || b.end - a.end - (b.start - a.start));

            let out = '';
            let last = 0;
            let firstHref = null;
            for (const it of intervals) {
                if (it.start < last) continue;
                out += escapeHtml(text.slice(last, it.start));
                const labelEsc = escapeHtml(text.slice(it.start, it.end));
                if (it.type === 'user_tag') {
                    out += `<a href="#" class="mention-link" onclick="openUserProfile('${escapeHtml(it.userId || '')}'); return false;">${escapeHtml(it.label || '')}</a>`;
                } else if (it.type === 'mention') {
                    out += `<a href="#" class="mention-link" onclick="openMentionProfile('${escapeHtml(it.username || '')}'); return false;">${labelEsc}</a>`;
                } else {
                    if (!firstHref) firstHref = it.href;
                    const hrefEsc = escapeHtml(it.href);
                    out += `<a href="${hrefEsc}" target="_blank" rel="noopener noreferrer" class="chat-msg-link">${labelEsc}</a>`;
                }
                last = it.end;
            }
            out += escapeHtml(text.slice(last));
            if (includePreview && firstHref) {
                const enc = encodeURIComponent(firstHref);
                out += `<div class="msg-link-preview" data-preview-url="${enc}"></div>`;
            }
            return out;
        }

        function normalizeMessengerUsernameValue(value) {
            return String(value || '')
                .replace(/^@+/, '')
                .toLowerCase()
                .replace(/[^a-z0-9_]/g, '')
                .slice(0, 50);
        }

        function ensureGeneratedMessengerUsername(value, fallbackId) {
            const normalized = normalizeMessengerUsernameValue(value);
            if (normalized) return normalized;
            const cleanId = String(fallbackId || '').toLowerCase().replace(/[^a-z0-9]/g, '');
            return `user${(cleanId.slice(-8) || '00000000').padStart(8, '0')}`.slice(0, 32);
        }

        function messengerPlainTextPreview(text) {
            const raw = String(text || '');
            if (!raw) return '';
            const groupEvent = raw.match(/^\[\[group-event:(.+)\]\]$/s);
            if (groupEvent) {
                try {
                    const payload = JSON.parse(groupEvent[1]);
                    return String(payload?.title || 'Системное сообщение');
                } catch (_) {
                    return 'Системное сообщение';
                }
            }
            if (raw.startsWith('[[group-event:')) return 'Системное сообщение';
            return raw
                .replace(/\[\[user:([^\]|]{1,220})\|([^\]]{1,220})\]\]/g, '$2')
                .replace(/@(\w+)/g, '@$1')
                .replace(/\[\[user:[^\]]*\]\]/g, '@user')
                .replace(/\s+/g, ' ')
                .trim();
        }

        function renderMessengerLinkPreviewCard(el, data) {
            const href = String(data.url || '').trim();
            const title = escapeHtml(String(data.title || href || 'Ссылка').slice(0, 300));
            const desc = escapeHtml(String(data.description || '').slice(0, 400));
            let host = '';
            try {
                host = escapeHtml(new URL(href).hostname || '');
            } catch (_) {}
            const img = String(data.image || '').trim();
            const imgEsc = img ? escapeHtml(img) : '';
            el.className = 'msg-link-preview msg-link-preview--ready';
            el.innerHTML = `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer" class="msg-link-preview-card">
                ${imgEsc ? `<div class="msg-link-preview-img-wrap"><img src="${imgEsc}" alt="" loading="lazy" referrerpolicy="no-referrer"></div>` : ''}
                <div class="msg-link-preview-body">
                    <div class="msg-link-preview-title">${title}</div>
                    ${desc ? `<div class="msg-link-preview-desc">${desc}</div>` : ''}
                    ${host ? `<div class="msg-link-preview-host">${host}</div>` : ''}
                </div>
            </a>`;
        }

        function hydrateMessengerLinkPreviews() {
            try {
                document.querySelectorAll('.msg-link-preview[data-preview-url]:not([data-preview-hydrated])').forEach((el) => {
                    el.setAttribute('data-preview-hydrated', '1');
                    let url = '';
                    try {
                        url = decodeURIComponent(String(el.getAttribute('data-preview-url') || '').trim());
                    } catch (_) {
                        el.remove();
                        return;
                    }
                    if (!url || !/^https?:\/\//i.test(url)) {
                        el.remove();
                        return;
                    }

                    const finish = (data) => {
                        if (!data || !data.ok) {
                            el.remove();
                            return;
                        }
                        renderMessengerLinkPreviewCard(el, data);
                    };

                    const cached = messengerLinkPreviewCache.get(url);
                    if (cached) {
                        finish(cached);
                        return;
                    }

                    el.classList.add('msg-link-preview--loading');
                    let p = messengerLinkPreviewPromises.get(url);
                    if (!p) {
                        p = fetch(`${LINK_PREVIEW_API}?url=${encodeURIComponent(url)}`)
                            .then((r) => r.json())
                            .then((data) => {
                                if (data && data.ok) messengerLinkPreviewCache.set(url, data);
                                return data;
                            })
                            .catch(() => null)
                            .finally(() => {
                                messengerLinkPreviewPromises.delete(url);
                            });
                        messengerLinkPreviewPromises.set(url, p);
                    }
                    p.then(finish);
                });
            } catch (_) {}
        }

        function avatarMarkup(name, avatarUrl, initialsHint) {
            const safeName = String(name || '').trim();
            const fromParts = safeName.split(/\s+/).filter(Boolean).map((p) => p.charAt(0)).join('').slice(0, 2);
            const fallback = String(initialsHint || fromParts || (safeName ? safeName.slice(0, 2) : '') || '·').slice(0, 2).toUpperCase();
            const safeUrl = typeof avatarUrl === 'string' ? avatarUrl.trim() : '';
            if (safeUrl) {
                const fbAttr = escapeHtml(fallback).replace(/"/g, '&quot;');
                return `<img class="messenger-avatar-img" src="${escapeHtml(safeUrl)}" alt="" referrerpolicy="no-referrer" loading="lazy" decoding="async" data-fallback="${fbAttr}" onerror="avatarImgOnError(this)">`;
            }
            return `<span class="messenger-avatar-fallback">${escapeHtml(fallback)}</span>`;
        }

        function avatarImgOnError(img) {
            if (!img || !img.parentNode) return;
            const fb = String(img.getAttribute('data-fallback') || '·').slice(0, 3) || '·';
            const span = document.createElement('span');
            span.className = 'messenger-avatar-fallback';
            span.textContent = fb;
            img.replaceWith(span);
        }

        function formatVoiceDurationMs(ms) {
            const sec = Math.max(0, Math.round(Number(ms) / 1000));
            const m = Math.floor(sec / 60);
            const s = sec % 60;
            return `${m}:${String(s).padStart(2, '0')}`;
        }

         function getBasePath() {
             const parts = window.location.pathname.split('/').filter(Boolean);
             if (parts.length && parts[parts.length - 1].toLowerCase() === 'index.html') {
                 parts.pop();
             }
             // Remove all grp_call_* segments from the end
             while (parts.length && /^grp_call_[a-z0-9]+$/i.test(parts[parts.length - 1])) {
                 parts.pop();
             }
             // Remove room id (id...)
             if (parts.length && /^id[a-z0-9_-]+$/i.test(parts[parts.length - 1])) {
                 parts.pop();
             }
             return '/' + parts.join('/');
         }

        function resolveAssetUrl(relativePath) {
            const basePath = getBasePath().replace(/\/$/, '');
            const rel = String(relativePath || '').replace(/^\/+/, '');
            return `${basePath}/${rel}`;
        }

        function buildRoomLink(targetRoomId) {
            const basePath = getBasePath().replace(/\/$/, '');
            return `${window.location.origin}${basePath}/${targetRoomId}`;
        }

        function buildTelegramRoomLink(targetRoomId) {
            const basePath = getBasePath().replace(/\/$/, '');
            const payload = encodeURIComponent(targetRoomId);
            return `${window.location.origin}${basePath}/${targetRoomId}`;
        }

        function shouldCopyTelegramInvite() {
            return authProfile?.provider === 'telegram';
        }

        function getRoomInviteToCopy() {
            if (!roomId) return '';
            return shouldCopyTelegramInvite() ? buildTelegramRoomLink(roomId) : String(roomId);
        }

        function parseRoomFromPath() {
            const params = new URLSearchParams(window.location.search);
            const startPayload = params.get('tgWebAppStartParam') || params.get('startapp') || params.get('start') || '';
            if (/^id[a-z0-9_-]+$/i.test(startPayload)) {
                return startPayload;
            }
            const parts = window.location.pathname.split('/').filter(Boolean);
            if (parts.length && parts[parts.length - 1].toLowerCase() === 'index.html') {
                parts.pop();
            }
            const last = parts[parts.length - 1] || '';
            if (/^id[a-z0-9_-]+$/i.test(last)) {
                return last;
            }
            return null;
        }

        function parseRoomInput(raw) {
            const value = String(raw || '').trim();
            if (!value) return '';
            if (/^id[a-z0-9_-]+$/i.test(value)) return value;
            try {
                const url = new URL(value);
                const segs = url.pathname.split('/').filter(Boolean);
                const maybeRoom = segs[segs.length - 1] || '';
                if (/^id[a-z0-9_-]+$/i.test(maybeRoom)) return maybeRoom;
            } catch (_) {}
            return '';
        }

        function parseGroupInviteFromLocation() {
            try {
                const params = new URLSearchParams(window.location.search);
                return String(params.get('groupInvite') || '').trim().replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 120);
            } catch (_) {
                return '';
            }
        }

        function removeQueryParamFromLocation(paramName) {
            try {
                const url = new URL(window.location.href);
                url.searchParams.delete(paramName);
                const next = `${url.pathname}${url.search ? url.search : ''}${url.hash || ''}`;
                history.replaceState(null, '', next);
            } catch (_) {}
        }

        function extractGroupInviteCodeFromHref(rawHref) {
            const href = String(rawHref || '').trim();
            if (!href) return '';
            try {
                const url = new URL(href, window.location.href);
                if (url.origin !== window.location.origin) return '';
                return String(url.searchParams.get('groupInvite') || '').trim().replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 120);
            } catch (_) {
                return '';
            }
        }

        function consumePendingGroupInviteIfAny(forceCode = '') {
            const inviteCode = String(forceCode || pendingGroupInviteCode || '').trim().replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 120);
            if (!inviteCode) return false;
            pendingGroupInviteCode = inviteCode;
            if (!authProfile?.appUserId) {
                renderAuthScreen();
                return false;
            }
            sendMessengerEvent({ type: 'messenger-preview-group-invite', inviteCode });
            removeQueryParamFromLocation('groupInvite');
            pendingGroupInviteCode = '';
            return true;
        }

        function generateRoomId() {
            return `id${Math.random().toString(36).substring(2, 10)}`;
        }

        function normalizeFacingMode(value, fallback = 'user') {
            const normalized = String(value || '').toLowerCase();
            if (normalized === 'environment') return 'environment';
            if (normalized === 'user') return 'user';
            return fallback;
        }

        function getTrackFacingMode(track, fallback = cameraFacingMode) {
            if (!track || typeof track.getSettings !== 'function') return normalizeFacingMode(fallback, 'user');
            const settings = track.getSettings();
            return normalizeFacingMode(settings?.facingMode, normalizeFacingMode(fallback, 'user'));
        }

        function applyVideoTileMirroring(userId) {
            const tile = videoTiles.get(userId);
            if (!tile) return;
            const video = tile.querySelector('video');
            if (!video) return;
            if (userId === 'self') {
                video.style.transform = cameraFacingMode === 'user' ? 'scaleX(-1)' : 'none';
                video.style.transformOrigin = 'center center';
                return;
            }
            video.style.transform = 'none';
            video.style.transformOrigin = 'center center';
        }

        function syncCameraFacingMode() {
            if (!videoEnabled || !ws || ws.readyState !== WebSocket.OPEN) return;
            ws.send(JSON.stringify({ type: 'camera-facing', mode: cameraFacingMode }));
        }

        function applySelfPreviewTrack() {
            const tile = videoTiles.get('self');
            if (!tile) return;
            const video = tile.querySelector('video');
            if (!video || !selfPreviewTrack) return;
            const previewStream = new MediaStream([selfPreviewTrack]);
            if (video.srcObject !== previewStream) {
                video.srcObject = previewStream;
            }
            video.play().catch(() => {});
        }

        async function createOutgoingAntiMirrorTrack(sourceTrack) {
            if (!sourceTrack) return null;
            const settings = sourceTrack.getSettings ? sourceTrack.getSettings() : {};
            const width = Math.max(320, Math.floor(settings.width || 960));
            const height = Math.max(240, Math.floor(settings.height || 540));
            const frameRate = Math.max(60, Math.min(120, Math.floor(settings.frameRate || 60)));
            const sourceVideo = document.createElement('video');
            sourceVideo.muted = true;
            sourceVideo.autoplay = true;
            sourceVideo.playsInline = true;
            sourceVideo.srcObject = new MediaStream([sourceTrack]);
            try {
                await sourceVideo.play();
            } catch (_) {
                return null;
            }
            const canvas = document.createElement('canvas');
            canvas.width = width;
            canvas.height = height;
            const ctx = canvas.getContext('2d');
            if (!ctx) return null;
            let rafId = null;
            let frameCbId = null;
            let useVideoCallback = typeof sourceVideo.requestVideoFrameCallback === 'function';
            const draw = () => {
                try {
                    if (sourceVideo.readyState >= 2) {
                        ctx.setTransform(-1, 0, 0, 1, canvas.width, 0);
                        ctx.drawImage(sourceVideo, 0, 0, canvas.width, canvas.height);
                        ctx.setTransform(1, 0, 0, 1, 0, 0);
                    }
                } catch (_) {}
                if (useVideoCallback) {
                    frameCbId = sourceVideo.requestVideoFrameCallback(draw);
                } else {
                    rafId = requestAnimationFrame(draw);
                }
            };
            draw();
            const outStream = canvas.captureStream(frameRate);
            const outTrack = outStream.getVideoTracks()[0] || null;
            if (!outTrack) {
                if (rafId) cancelAnimationFrame(rafId);
                if (frameCbId && typeof sourceVideo.cancelVideoFrameCallback === 'function') {
                    try { sourceVideo.cancelVideoFrameCallback(frameCbId); } catch (_) {}
                }
                try { sourceVideo.pause(); } catch (_) {}
                sourceVideo.srcObject = null;
                return null;
            }
            outTrack.contentHint = 'motion';
            const cleanup = () => {
                if (rafId) {
                    cancelAnimationFrame(rafId);
                    rafId = null;
                }
                if (frameCbId && typeof sourceVideo.cancelVideoFrameCallback === 'function') {
                    try { sourceVideo.cancelVideoFrameCallback(frameCbId); } catch (_) {}
                    frameCbId = null;
                }
                try { outStream.getTracks().forEach((track) => track.stop()); } catch (_) {}
                try { sourceVideo.pause(); } catch (_) {}
                sourceVideo.srcObject = null;
            };
            return { track: outTrack, cleanup };
        }

        async function createCameraTracks(preferredFacingMode = cameraFacingMode) {
            const normalizedFacing = normalizeFacingMode(preferredFacingMode, 'user');
            const baseVideo = {
                width: { ideal: 1280, max: 1920 },
                height: { ideal: 720, max: 1080 },
                frameRate: { ideal: 60, max: 120 }
            };
            const attempts = [
                { ...baseVideo, facingMode: { exact: normalizedFacing } },
                { ...baseVideo, facingMode: { ideal: normalizedFacing } },
                { ...baseVideo }
            ];
            let lastError = null;
            let sourceTrack = null;
            for (const video of attempts) {
                try {
                    const videoStream = await navigator.mediaDevices.getUserMedia({ video });
                    const track = videoStream.getVideoTracks()[0] || null;
                    if (track) {
                        sourceTrack = track;
                        break;
                    }
                } catch (error) {
                    lastError = error;
                }
            }
            if (!sourceTrack) throw lastError || new Error('No video track');
            const resolvedFacingMode = getTrackFacingMode(sourceTrack, normalizedFacing);
            let outgoingTrack = sourceTrack;
            let cleanup = null;
            if (resolvedFacingMode === 'user') {
                const transformed = await createOutgoingAntiMirrorTrack(sourceTrack);
                if (transformed && transformed.track) {
                    outgoingTrack = transformed.track;
                    cleanup = transformed.cleanup;
                }
            }
            return {
                sourceTrack,
                outgoingTrack,
                facingMode: resolvedFacingMode,
                previewTrack: sourceTrack,
                cleanup
            };
        }

        function detachCurrentVideoTrack(stopTrack = true) {
            if (!videoTrack) return;
            peers.forEach((peer, key) => {
                if (!key.startsWith('av-') || !peer || peer.destroyed || typeof peer.removeTrack !== 'function') return;
                try {
                    peer.removeTrack(videoTrack, localStream);
                } catch (_) {}
            });
            try { localStream.removeTrack(videoTrack); } catch (_) {}
            if (stopTrack) {
                try { videoTrack.stop(); } catch (_) {}
            }
            if (outgoingTrackCleanup) {
                try { outgoingTrackCleanup(); } catch (_) {}
                outgoingTrackCleanup = null;
            }
            if (stopTrack && cameraSourceTrack && cameraSourceTrack !== videoTrack) {
                try { cameraSourceTrack.stop(); } catch (_) {}
            }
            videoTrack = null;
            cameraSourceTrack = null;
            selfPreviewTrack = null;
        }

        function replaceVideoTrackForAllPeers(oldTrack, newTrack) {
            if (!newTrack || !localStream) return;
            getRemoteParticipantIds().forEach((participantId) => {
                const peer = ensureAvPeerForParticipant(participantId, shouldInitiatePeer(myId, participantId));
                if (!peer || peer.destroyed) return;
                let updated = false;
                if (oldTrack && typeof peer.replaceTrack === 'function') {
                    try {
                        peer.replaceTrack(oldTrack, newTrack, localStream);
                        updated = true;
                    } catch (_) {}
                }
                if (!updated) {
                    if (oldTrack && typeof peer.removeTrack === 'function') {
                        try { peer.removeTrack(oldTrack, localStream); } catch (_) {}
                    }
                    if (typeof peer.addTrack === 'function') {
                        try {
                            peer.addTrack(newTrack, localStream);
                            updated = true;
                        } catch (error) {
                            const text = String(error?.message || '');
                            if (/already|exist|added/i.test(text)) {
                                updated = true;
                            }
                        }
                    }
                }
                if (!updated) {
                    recreateAvPeerForParticipant(participantId);
                }
            });
        }

        function replaceAudioTrackForAllPeers(oldTrack, newTrack) {
            if (!newTrack || !localStream) return;
            getRemoteParticipantIds().forEach((participantId) => {
                const peer = ensureAvPeerForParticipant(participantId, shouldInitiatePeer(myId, participantId));
                if (!peer || peer.destroyed) return;
                let updated = false;
                if (oldTrack && typeof peer.replaceTrack === 'function') {
                    try {
                        peer.replaceTrack(oldTrack, newTrack, localStream);
                        updated = true;
                    } catch (_) {}
                }
                if (!updated) {
                    if (oldTrack && typeof peer.removeTrack === 'function') {
                        try { peer.removeTrack(oldTrack, localStream); } catch (_) {}
                    }
                    if (typeof peer.addTrack === 'function') {
                        try {
                            peer.addTrack(newTrack, localStream);
                            updated = true;
                        } catch (error) {
                            const text = String(error?.message || '');
                            if (/already|exist|added/i.test(text)) updated = true;
                        }
                    }
                }
                if (!updated) recreateAvPeerForParticipant(participantId);
            });
        }

        function applyMicTrackEnabledState() {
            try {
                localStream?.getAudioTracks?.().forEach((t) => {
                    try { t.enabled = !!audioEnabled; } catch (_) {}
                });
            } catch (_) {}
        }

        /** В эфир только сырой трек с микрофона (без Web Audio / gain). */
        function applyMicOutgoingChain() {
            if (!localStream) return;
            const cur = localStream.getAudioTracks()[0] || null;
            if (!rawMicTrack) rawMicTrack = cur;
            if (!cur || !rawMicTrack) {
                applyMicTrackEnabledState();
                return;
            }
            if (cur === rawMicTrack) {
                applyMicTrackEnabledState();
                return;
            }
            if (rawMicTrack.readyState === 'live') {
                try { localStream.removeTrack(cur); } catch (_) {}
                try {
                    if (!localStream.getAudioTracks().includes(rawMicTrack)) {
                        localStream.addTrack(rawMicTrack);
                    }
                } catch (_) {}
                try {
                    replaceAudioTrackForAllPeers(cur, rawMicTrack);
                } catch (_) {}
            }
            applyMicTrackEnabledState();
        }

        function attachVideoTrack(track, previousTrack = null) {
            if (!track || !localStream) return;
            track.enabled = true;
            track.contentHint = 'motion';
            if (previousTrack && previousTrack !== track) {
                try { localStream.removeTrack(previousTrack); } catch (_) {}
            }
            if (!localStream.getVideoTracks().includes(track)) {
                localStream.addTrack(track);
            }
            replaceVideoTrackForAllPeers(previousTrack, track);
            if (!previousTrack) {
                ensureVideoTrackForAllPeers(track);
            }
            const facingTrack = cameraSourceTrack && cameraSourceTrack.readyState === 'live' ? cameraSourceTrack : track;
            cameraFacingMode = getTrackFacingMode(facingTrack, cameraFacingMode);
            addVideoTile('self', `${userName} (Вы)`, localStream);
            applyVideoTileMirroring('self');
            applySelfPreviewTrack();
        }

        async function prewarmCameraTrack() {
            if (videoTrack && videoTrack.readyState === 'live') {
                videoTrack.enabled = false;
                return;
            }
            if (videoPrewarmPromise) {
                await videoPrewarmPromise;
                return;
            }
            videoPrewarmPromise = (async () => {
                try {
                    const newVideoTracks = await createCameraTracks(cameraFacingMode);
                    const newVideoTrack = newVideoTracks?.outgoingTrack || null;
                    if (!newVideoTrack) return;
                    newVideoTrack.enabled = false;
                    videoTrack = newVideoTrack;
                    cameraSourceTrack = newVideoTracks?.sourceTrack || null;
                    selfPreviewTrack = newVideoTracks?.previewTrack || null;
                    outgoingTrackCleanup = newVideoTracks?.cleanup || null;
                    if (localStream && !localStream.getVideoTracks().includes(newVideoTrack)) {
                        localStream.addTrack(newVideoTrack);
                    }
                } catch (_) {}
            })();
            try {
                await videoPrewarmPromise;
            } finally {
                videoPrewarmPromise = null;
            }
        }

        function loadStoredProfile() {
            try {
                const raw = localStorage.getItem('seych-auth-profile');
                if (!raw) return null;
                const parsed = JSON.parse(raw);
                if (!parsed || !parsed.name) return null;
                parsed.avatar = proxifyAvatarUrl(parsed.avatar || '');
                parsed.coverUrl = proxifyAvatarUrl(parsed.coverUrl || '');
                const previousAppUserId = String(parsed.appUserId || '').trim();
                parsed.externalKey = buildExternalAccountKey(parsed);
                parsed.appUserId = buildStableAppUserId(parsed, previousAppUserId);
                if (previousAppUserId && previousAppUserId !== parsed.appUserId) {
                    pendingLegacyAppUserId = previousAppUserId;
                }
                if (!parsed.appUserId || previousAppUserId !== parsed.appUserId) {
                    localStorage.setItem('seych-auth-profile', JSON.stringify(parsed));
                }
                return parsed;
            } catch (_) {
                return null;
            }
        }

        function loadTelegramContacts() {
            return [];
        }

        function loadVkCustomContacts() {
            try {
                const raw = localStorage.getItem('seych-vk-custom-contacts');
                const parsed = raw ? JSON.parse(raw) : [];
                return Array.isArray(parsed) ? parsed : [];
            } catch (_) {
                return [];
            }
        }

        function loadVkHiddenContacts() {
            try {
                const raw = localStorage.getItem('seych-vk-hidden-contacts');
                const parsed = raw ? JSON.parse(raw) : [];
                return Array.isArray(parsed) ? parsed : [];
            } catch (_) {
                return [];
            }
        }

        function saveVkCustomContacts(list) {
            vkCustomContacts = Array.isArray(list) ? list : [];
            localStorage.setItem('seych-vk-custom-contacts', JSON.stringify(vkCustomContacts));
        }

        function saveVkHiddenContacts(list) {
            vkHiddenContactIds = Array.isArray(list) ? list : [];
            localStorage.setItem('seych-vk-hidden-contacts', JSON.stringify(vkHiddenContactIds));
        }

        function normalizeTelegramUsername(value) {
            let v = String(value || '').trim();
            if (!v) return '';
            v = v.replace(/^https?:\/\/t\.me\//i, '');
            v = v.replace(/^@+/, '');
            v = v.split('/')[0];
            if (!/^[a-zA-Z0-9_]{4,32}$/.test(v)) return '';
            return `@${v}`;
        }

        function getTelegramAvatarUrl(username) {
            const clean = String(username || '').replace(/^@/, '').trim();
            if (!clean) return '';
            return proxifyAvatarUrl(`https://t.me/i/userpic/320/${encodeURIComponent(clean)}.jpg`);
        }

        function normalizeVkUserInput(value) {
            let v = String(value || '').trim();
            if (!v) return '';
            v = v.replace(/^https?:\/\/(m\.)?vk\.com\//i, '');
            v = v.replace(/^@+/, '');
            v = v.split(/[/?#]/)[0];
            return v;
        }

        function mergeContacts() {
            const merged = [];
            const seen = new Set();
            const pushContact = (contact) => {
                if (!contact) return;
                const username = normalizeTelegramUsername(contact.username || contact.target || '');
                const target = String(contact.target || contact.id || username).trim();
                const key = String(contact.id || username || target).toLowerCase();
                if (!key || seen.has(key)) return;
                seen.add(key);
                merged.push({
                    id: String(contact.id || key),
                    name: String(contact.name || 'Контакт'),
                    username: username || '',
                    target: target || username,
                    avatar: proxifyAvatarUrl(contact.avatar || getTelegramAvatarUrl(username))
                });
            };
            telegramContacts.forEach(pushContact);
            return merged;
        }

        function mergeVkContacts() {
            const merged = [];
            const seen = new Set();
            const hidden = new Set((vkHiddenContactIds || []).map(v => String(v).toLowerCase()));
            const pushContact = (contact) => {
                if (!contact) return;
                const id = String(contact.id || '').trim();
                if (!id) return;
                const key = id.toLowerCase();
                if (hidden.has(key)) return;
                if (seen.has(key)) return;
                seen.add(key);
                merged.push({
                    id,
                    name: String(contact.name || 'Контакт'),
                    username: String(contact.username || ''),
                    avatar: proxifyAvatarUrl(contact.avatar || ''),
                    target: id
                });
            };
            vkContacts.forEach(pushContact);
            vkCustomContacts.forEach(pushContact);
            return merged;
        }

        function saveProfile(profile) {
            const previousAppUserId = String(authProfile?.appUserId || appUserId || '').trim();
            const externalKey = buildExternalAccountKey(profile);
            const stableAppUserId = buildStableAppUserId(profile, previousAppUserId);
            if (previousAppUserId && previousAppUserId !== stableAppUserId) {
                pendingLegacyAppUserId = previousAppUserId;
            }
            const normalizedProfile = {
                ...profile,
                avatar: proxifyAvatarUrl(profile?.avatar || ''),
                coverUrl: proxifyAvatarUrl(profile?.coverUrl || ''),
                externalKey,
                appUserId: stableAppUserId
            };
            authProfile = normalizedProfile;
            appUserId = normalizedProfile.appUserId;
            userName = normalizedProfile.name || 'Пользователь';
            userAvatar = normalizedProfile.avatar || '';
            localStorage.setItem('seych-auth-profile', JSON.stringify(normalizedProfile));
        }

        function clearProfile() {
            stopDeviceSessionWatchdog();
            clearKnownOutgoingCallStatusesStorage();
            authProfile = null;
            appUserId = '';
            userName = '';
            userAvatar = '';
            friendsState = { friends: [], incomingRequests: [], outgoingRequests: [], incomingCalls: [], outgoingCalls: [] };
            friendsSearchResults = [];
            friendsSearchValue = '';
            friendsCallsModalPrimed = false;
            knownIncomingCallIds = new Set();
            knownOutgoingCallStatuses = new Map();
            if (friendsPollTimer) {
                clearInterval(friendsPollTimer);
                friendsPollTimer = null;
            }
            closeIncomingCallModal();
            closeIncomingFriendModal();
            localStorage.removeItem('seych-auth-profile');
            try { localStorage.removeItem(DEVICE_SESSION_KEY); } catch (_) {}
        }

        function isMobileDeviceType() {
            if (typeof navigator === 'undefined') return false;
            const ua = String(navigator.userAgent || '');
            if (/Android|webOS|iPhone|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua)) return true;
            if (/Mobile/i.test(ua) && !/iPad/i.test(ua)) return true;
            if (/iPad/i.test(ua)) return true;
            if (/Macintosh|Mac OS X/i.test(ua) && Number(navigator.maxTouchPoints || 0) > 1) return true;
            return false;
        }

        function isDesktopDeviceType() {
            return !isMobileDeviceType();
        }

        function getDeviceSessionId() {
            try {
                return String(localStorage.getItem(DEVICE_SESSION_KEY) || '').trim();
            } catch (_) {
                return '';
            }
        }

        function setDeviceSessionId(sessionId) {
            const sid = String(sessionId || '').trim();
            if (!sid) return;
            try { localStorage.setItem(DEVICE_SESSION_KEY, sid); } catch (_) {}
        }

        function parseDeviceNameFromUa(ua) {
            const s = String(ua || navigator.userAgent || '');
            if (/iPhone/i.test(s)) return 'iPhone';
            if (/iPad/i.test(s)) return 'iPad';
            if (/Android/i.test(s)) return /Mobile/i.test(s) ? 'Android' : 'Android-планшет';
            if (/Windows/i.test(s)) return 'Windows';
            if (/Macintosh|Mac OS X/i.test(s)) return 'Mac';
            if (/Linux/i.test(s)) return 'Linux';
            return 'Браузер';
        }

        async function qrAuthApiRequest(action, payload = {}) {
            const body = {
                action,
                user_agent: navigator.userAgent || '',
                is_mobile: isMobileDeviceType(),
                device_name: parseDeviceNameFromUa(navigator.userAgent || ''),
                ...payload
            };
            if (authProfile?.appUserId) {
                body.app_user_id = authProfile.appUserId;
                body.provider = authProfile.provider || '';
                body.name = authProfile.name || '';
                body.avatar = authProfile.avatar || '';
                body.external_key = String(authProfile.externalKey || buildExternalAccountKey(authProfile) || '');
                body.telegram_id = authProfile.telegramId || '';
                body.vk_user_id = authProfile.vkUserId || '';
                body.vk_username = authProfile.vkUsername || '';
                body.username = authProfile.username || '';
            }
            const sid = getDeviceSessionId();
            if (sid && body.device_session_id === undefined) {
                body.device_session_id = sid;
            }
            const response = await fetch(QR_AUTH_API, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            const data = await response.json();
            if (!data || !data.success) {
                throw new Error((data && data.error) ? data.error : 'Ошибка QR-авторизации');
            }
            return data.data || {};
        }

        function buildQrLoginUrl(token) {
            const base = `${window.location.origin}${getBasePath().replace(/\/$/, '')}/`;
            const url = new URL(base, window.location.origin);
            url.searchParams.set('qr_login', token);
            return url.toString();
        }

        function stopQrAuthPolling() {
            if (qrAuthPollTimer) {
                clearInterval(qrAuthPollTimer);
                qrAuthPollTimer = null;
            }
            qrAuthCurrentToken = '';
        }

        function showQrAuthSuccessAnimation() {
            const overlay = document.getElementById('authQrSuccess');
            if (!overlay) return;
            overlay.classList.add('is-visible');
        }

        let qrCodeLibPromise = null;

        function ensureQrCodeLibLoaded() {
            if (typeof QRCode !== 'undefined') {
                return Promise.resolve(true);
            }
            if (qrCodeLibPromise) return qrCodeLibPromise;
            qrCodeLibPromise = new Promise((resolve) => {
                const urls = [
                    'https://cdnjs.cloudflare.com/ajax/libs/qrcode/1.5.3/qrcode.min.js',
                    'https://cdn.jsdelivr.net/npm/qrcode@1.5.4/build/qrcode.min.js'
                ];
                let idx = 0;
                const tryNext = () => {
                    if (typeof QRCode !== 'undefined') {
                        resolve(true);
                        return;
                    }
                    if (idx >= urls.length) {
                        resolve(false);
                        return;
                    }
                    const src = urls[idx++];
                    const existing = document.querySelector(`script[data-qr-lib="${src}"]`);
                    if (existing) {
                        existing.addEventListener('load', () => resolve(typeof QRCode !== 'undefined'), { once: true });
                        existing.addEventListener('error', tryNext, { once: true });
                        return;
                    }
                    const s = document.createElement('script');
                    s.src = src;
                    s.async = true;
                    s.crossOrigin = 'anonymous';
                    s.dataset.qrLib = src;
                    s.onload = () => resolve(typeof QRCode !== 'undefined');
                    s.onerror = tryNext;
                    document.head.appendChild(s);
                };
                tryNext();
            });
            return qrCodeLibPromise;
        }

        async function renderAuthQrImage(url) {
            const host = document.getElementById('authQrHost');
            if (!host) return false;
            host.innerHTML = '<div class="auth-qr-loading"><i class="fas fa-circle-notch fa-spin"></i></div>';
            const libOk = await ensureQrCodeLibLoaded();
            if (libOk && typeof QRCode !== 'undefined' && typeof QRCode.toDataURL === 'function') {
                try {
                    const dataUrl = await QRCode.toDataURL(url, {
                        width: 220,
                        margin: 2,
                        errorCorrectionLevel: 'M',
                        color: { dark: '#111827', light: '#ffffff' }
                    });
                    host.innerHTML = `<img class="auth-qr-image" src="${dataUrl}" width="220" height="220" alt="QR-код">`;
                    return true;
                } catch (_) {}
            }
            const enc = encodeURIComponent(url);
            host.innerHTML = `<img class="auth-qr-image" src="https://api.qrserver.com/v1/create-qr-code/?size=220x220&margin=10&data=${enc}" width="220" height="220" alt="QR-код" referrerpolicy="no-referrer">`;
            return true;
        }

        async function startDesktopQrAuthFlow() {
            stopQrAuthPolling();
            const host = document.getElementById('authQrHost');
            const overlay = document.getElementById('authQrSuccess');
            if (overlay) overlay.classList.remove('is-visible');
            if (!host) return;
            try {
                const data = await qrAuthApiRequest('create_qr', {});
                const token = String(data.token || '').trim();
                if (!token) {
                    host.innerHTML = '<div class="auth-qr-loading" style="font-size:12px;padding:12px;text-align:center;color:#64748b;">Не удалось создать код</div>';
                    return;
                }
                qrAuthCurrentToken = token;
                const url = buildQrLoginUrl(token);
                await renderAuthQrImage(url);
                qrAuthPollTimer = setInterval(async () => {
                    if (!qrAuthCurrentToken) return;
                    try {
                        const poll = await qrAuthApiRequest('poll_qr', { token: qrAuthCurrentToken });
                        if (poll.status === 'expired') {
                            stopQrAuthPolling();
                            await startDesktopQrAuthFlow();
                            return;
                        }
                        if (poll.status !== 'approved' || !poll.profile) return;
                        stopQrAuthPolling();
                        showQrAuthSuccessAnimation();
                        const profile = { ...poll.profile };
                        if (poll.deviceSessionId) setDeviceSessionId(poll.deviceSessionId);
                        profile.avatar = proxifyAvatarUrl(profile.avatar || '');
                        profile.externalKey = profile.externalKey || buildExternalAccountKey(profile);
                        profile.appUserId = buildStableAppUserId(profile, profile.appUserId || '');
                        setTimeout(() => {
                            setAuthenticatedProfile(profile);
                        }, 1100);
                    } catch (_) {}
                }, 2000);
            } catch (err) {
                if (host) {
                    host.innerHTML = '<div class="auth-qr-loading" style="font-size:12px;padding:12px;text-align:center;color:#64748b;">Ошибка загрузки</div>';
                }
                showNotification('QR-вход', err.message || 'Не удалось создать QR-код', 'error');
            }
        }

        function toggleAuthClassicProviders() {
            authShowClassicProviders = !authShowClassicProviders;
            renderAuthScreen();
        }

        function parseQrLoginFromLocation() {
            try {
                const params = new URLSearchParams(window.location.search);
                return String(params.get('qr_login') || '').trim();
            } catch (_) {
                return '';
            }
        }

        function clearQrLoginFromLocation() {
            try {
                const url = new URL(window.location.href);
                if (!url.searchParams.has('qr_login')) return;
                url.searchParams.delete('qr_login');
                history.replaceState(null, '', url.pathname + (url.search || '') + url.hash);
            } catch (_) {}
        }

        async function openQrApproveModal(token) {
            if (!token) return;
            if (!authProfile?.appUserId) {
                showNotification('QR-вход', 'Сначала войдите в аккаунт на этом устройстве', 'warning');
                return;
            }
            let info = null;
            try {
                info = await qrAuthApiRequest('qr_info', { token });
            } catch (err) {
                showNotification('QR-вход', err.message || 'QR-код недействителен', 'error');
                clearQrLoginFromLocation();
                return;
            }
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'qrApproveModal';
            modal.innerHTML = `
                <div class="modal-content auth-card" style="max-width:380px;">
                    <div class="qr-confirm-card">
                        <img class="qr-confirm-avatar" src="${escapeHtml(proxifyAvatarUrl(authProfile.avatar || ''))}" alt="">
                        <div class="qr-confirm-name">${escapeHtml(authProfile.name || 'Пользователь')}</div>
                        <div class="qr-confirm-meta">Подтвердите вход</div>
                        <div class="qr-confirm-device">
                            <div><i class="fas fa-desktop"></i> ${escapeHtml(info.deviceName || 'Компьютер')}</div>
                            <div style="margin-top:6px;opacity:0.8;"><i class="fas fa-location-dot"></i> ${escapeHtml(info.location || 'Не определено')}</div>
                        </div>
                        <div class="modal-buttons">
                            <button type="button" class="modal-btn cancel" onclick="closeTransientModal('qrApproveModal')">Отмена</button>
                            <button type="button" class="modal-btn confirm" id="qrApproveConfirmBtn">Вход</button>
                        </div>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            const confirmBtn = modal.querySelector('#qrApproveConfirmBtn');
            if (confirmBtn) {
                confirmBtn.onclick = async () => {
                    confirmBtn.disabled = true;
                    try {
                        await qrAuthApiRequest('approve_qr', { token });
                        closeTransientModal('qrApproveModal');
                        clearQrLoginFromLocation();
                        showNotification('QR-вход', 'Вход на компьютере подтверждён', 'success');
                    } catch (err) {
                        showNotification('QR-вход', err.message || 'Не удалось подтвердить', 'error');
                        confirmBtn.disabled = false;
                    }
                };
            }
        }

        async function stopQrScanner() {
            if (!qrScannerInstance) return;
            try { await qrScannerInstance.stop(); } catch (_) {}
            try { await qrScannerInstance.clear(); } catch (_) {}
            qrScannerInstance = null;
        }

        async function openQrScannerModal() {
            if (!authProfile?.appUserId) {
                showNotification('QR-вход', 'Сначала войдите в аккаунт', 'warning');
                return;
            }
            if (typeof Html5Qrcode === 'undefined') {
                showNotification('QR-вход', 'Сканер недоступен в этом браузере', 'error');
                return;
            }
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'qrScannerModal';
            modal.innerHTML = `
                <div class="modal-content qr-scanner-modal">
                    <h2 style="margin-bottom:6px;font-size:18px;"><i class="fas fa-qrcode"></i> Сканировать QR</h2>
                    <p class="auth-subtitle" style="margin-top:0;">Наведите камеру на код на компьютере</p>
                    <div class="qr-scanner-shell">
                        <div id="qrScannerRegion"></div>
                    </div>
                    <button type="button" class="modal-btn cancel" style="margin-top:14px;width:100%;" onclick="closeQrScannerModal()">Закрыть</button>
                </div>
            `;
            document.body.appendChild(modal);
            await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
            qrScannerInstance = new Html5Qrcode('qrScannerRegion');
            const onScan = (decoded) => {
                let token = '';
                try {
                    const u = new URL(decoded);
                    token = String(u.searchParams.get('qr_login') || '').trim();
                } catch (_) {
                    const m = String(decoded || '').match(/qr_login=([a-f0-9]+)/i);
                    token = m ? m[1] : '';
                }
                if (!token) return;
                stopQrScanner().then(() => {
                    closeTransientModal('qrScannerModal');
                    openQrApproveModal(token);
                });
            };
            const scanConfig = {
                fps: 12,
                aspectRatio: 1.0,
                qrbox: (viewWidth, viewHeight) => {
                    const edge = Math.min(viewWidth, viewHeight);
                    const size = Math.max(180, Math.floor(edge * 0.72));
                    return { width: size, height: size };
                }
            };
            const tryStartCamera = async (cameraIdOrConfig) => {
                await qrScannerInstance.start(cameraIdOrConfig, scanConfig, onScan, () => {});
            };
            try {
                const cameras = await Html5Qrcode.getCameras();
                const backCam = (cameras || []).find((c) => /back|rear|environment/i.test(String(c.label || '')));
                const pick = backCam || (cameras && cameras.length ? cameras[cameras.length - 1] : null);
                if (pick && pick.id) {
                    await tryStartCamera(pick.id);
                } else {
                    await tryStartCamera({ facingMode: 'environment' });
                }
            } catch (_) {
                try {
                    await tryStartCamera({ facingMode: 'environment' });
                } catch (err2) {
                    try {
                        await tryStartCamera({ facingMode: 'user' });
                    } catch (err3) {
                        showNotification('QR-вход', 'Не удалось открыть камеру', 'error');
                        closeQrScannerModal();
                    }
                }
            }
        }

        async function closeQrScannerModal() {
            await stopQrScanner();
            closeTransientModal('qrScannerModal');
        }

        function stopDeviceSessionWatchdog() {
            if (deviceSessionWatchTimer) {
                clearInterval(deviceSessionWatchTimer);
                deviceSessionWatchTimer = null;
            }
        }

        function startDeviceSessionWatchdog() {
            stopDeviceSessionWatchdog();
            if (!authProfile?.appUserId) return;
            deviceSessionWatchTimer = setInterval(() => {
                validateCurrentDeviceSession();
            }, 8000);
        }

        function sendDeviceSessionRevokeWs(targetSessionId) {
            const sid = String(targetSessionId || '').trim();
            if (!sid || !ws || ws.readyState !== WebSocket.OPEN) return false;
            try {
                ws.send(JSON.stringify({ type: 'device-session-revoke', deviceSessionId: sid }));
                return true;
            } catch (_) {
                return false;
            }
        }

        async function registerCurrentDeviceSession() {
            if (!authProfile?.appUserId) return null;
            try {
                const data = await qrAuthApiRequest('register_device', {});
                if (data.deviceSessionId) setDeviceSessionId(data.deviceSessionId);
                return data.deviceSessionId || getDeviceSessionId();
            } catch (_) {
                return getDeviceSessionId();
            }
        }

        async function validateCurrentDeviceSession() {
            const sid = getDeviceSessionId();
            if (!sid || !authProfile?.appUserId) return true;
            try {
                const data = await qrAuthApiRequest('validate_session', {});
                if (data.valid === false || data.revoked === true) {
                    handleDeviceSessionKicked('revoked');
                    return false;
                }
                await qrAuthApiRequest('touch_device', {});
            } catch (_) {}
            return true;
        }

        async function revokeDeviceSessionRemote(deviceSessionId) {
            const sid = String(deviceSessionId || '').trim();
            const currentSid = getDeviceSessionId();
            if (!sid || !authProfile?.appUserId) return;
            if (sid === currentSid) {
                showNotification('Устройства', 'Нельзя завершить сессию на этом устройстве', 'warning');
                return;
            }
            try {
                await qrAuthApiRequest('revoke_device', { revoke_session_id: sid });
            } catch (err) {
                showNotification('Устройства', err.message || 'Не удалось завершить сессию', 'error');
                return;
            }
            sendDeviceSessionRevokeWs(sid);
            showNotification('Устройства', 'Сессия завершена', 'success');
            openDevicesSettingsModal(true);
        }

        async function openDevicesSettingsModal(refreshOnly = false) {
            if (!authProfile?.appUserId) return;
            let devices = [];
            try {
                const data = await qrAuthApiRequest('list_devices', {});
                devices = Array.isArray(data.devices) ? data.devices : [];
            } catch (err) {
                showNotification('Устройства', err.message || 'Не удалось загрузить список', 'error');
                return;
            }
            devices.sort((a, b) => {
                if (!!a.isCurrent !== !!b.isCurrent) return a.isCurrent ? -1 : 1;
                return (Number(b.lastSeenAt || 0) - Number(a.lastSeenAt || 0));
            });
            const listHtml = devices.length
                ? devices.map((d) => {
                    const created = d.createdAt ? new Date(d.createdAt * 1000).toLocaleString('ru-RU') : '—';
                    const sessionId = String(d.deviceSessionId || '').trim();
                    const currentBadge = d.isCurrent ? '<span style="color:#34d399;font-size:11px;"> · текущее</span>' : '';
                    const endBtn = d.isCurrent
                        ? ''
                        : `<button type="button" class="device-row-end" data-device-session-id="${escapeHtml(sessionId)}">Завершить</button>`;
                    return `
                        <div class="device-row${d.isCurrent ? ' is-current' : ''}" data-device-session-id="${escapeHtml(sessionId)}">
                            <div class="device-row-icon"><i class="fas ${/phone|iphone|android/i.test(d.deviceName || '') ? 'fa-mobile-alt' : 'fa-laptop'}"></i></div>
                            <div class="device-row-body">
                                <div class="device-row-title">${escapeHtml(d.deviceName || 'Устройство')}${currentBadge}</div>
                                <div class="device-row-sub"><i class="fas fa-location-dot"></i> ${escapeHtml(d.location || 'Не определено')}</div>
                                <div class="device-row-sub">Вход: ${escapeHtml(created)}</div>
                            </div>
                            ${endBtn}
                        </div>
                    `;
                }).join('')
                : '<div style="opacity:0.7;font-size:13px;padding:12px 0;">Нет активных сессий</div>';
            const existing = document.getElementById('devicesSettingsModal');
            if (existing && refreshOnly) {
                const listEl = existing.querySelector('.devices-list');
                if (listEl) listEl.innerHTML = listHtml;
                return;
            }
            if (existing) existing.remove();
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'devicesSettingsModal';
            modal.innerHTML = `
                <div class="modal-content" style="max-width:520px;text-align:left;">
                    <h2><i class="fas fa-laptop-mobile"></i> Устройства</h2>
                    <p class="auth-subtitle" style="text-align:left;margin-bottom:12px;">Управление входами и QR-авторизацией</p>
                    <button type="button" class="blacklist-open-btn" onclick="closeTransientModal('devicesSettingsModal'); openQrScannerModal();"><i class="fas fa-qrcode"></i> Вход по QR-коду</button>
                    <div style="font-size:14px;font-weight:600;margin:16px 0 4px;">Активные сессии</div>
                    <div class="devices-list">${listHtml}</div>
                    <div class="modal-buttons" style="margin-top:16px;">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('devicesSettingsModal')">Закрыть</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            modal.querySelectorAll('.device-row-end[data-device-session-id]').forEach((btn) => {
                btn.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    const targetId = String(btn.getAttribute('data-device-session-id') || '').trim();
                    if (targetId) revokeDeviceSessionRemote(targetId);
                });
            });
        }

        function handleDeviceSessionKicked(reason = 'revoked') {
            stopDeviceSessionWatchdog();
            stopQrAuthPolling();
            if (wsReconnectTimer) {
                clearTimeout(wsReconnectTimer);
                wsReconnectTimer = null;
            }
            wsLastInitialMsg = null;
            if (ws) {
                try {
                    ws.onopen = null;
                    ws.onmessage = null;
                    ws.onerror = null;
                    ws.onclose = null;
                    ws.__closingByUser = true;
                    ws.close();
                } catch (_) {}
            }
            ws = null;
            currentWsType = '';
            clearProfile();
            renderAuthScreen();
            showNotification('Сессия', reason === 'revoked' ? 'Вход на этом устройстве завершён' : 'Сессия завершена', 'warning');
        }

        async function friendsApiRequest(action, payload = {}) {
            const identityKeys = authProfile ? buildIdentityKeys(authProfile) : [];
            const requestBody = {
                action,
                app_user_id: appUserId,
                active_tab: !document.hidden,
                name: authProfile?.name || userName || 'Пользователь',
                avatar: authProfile?.avatar || userAvatar || '',
                username: ensureGeneratedMessengerUsername(messengerProfile.username || authProfile?.vkUsername || '', appUserId),
                external_key: authProfile ? String(authProfile.externalKey || buildExternalAccountKey(authProfile) || '') : '',
                identity_keys: identityKeys,
                ...payload
            };
            let lastErr = null;
            for (const apiUrl of FRIENDS_API_FALLBACKS) {
                try {
                    const response = await fetch(apiUrl, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(requestBody)
                    });
                    const rawText = await response.text();
                    let data = null;
                    try {
                        data = rawText ? JSON.parse(rawText) : null;
                    } catch (_) {
                        const raw = String(rawText || '');
                        if (/^\s*</.test(raw)) {
                            throw new Error(`API returned HTML: ${raw.slice(0, 160)}`);
                        }
                        throw new Error(raw ? `Invalid JSON: ${raw.slice(0, 160)}` : 'Invalid JSON');
                    }
                    if (!data || !data.success) {
                        throw new Error((data && data.error) ? data.error : 'Ошибка друзей');
                    }
                    if (apiUrl && apiUrl !== FRIENDS_API) {
                        FRIENDS_API = apiUrl;
                        try { localStorage.setItem('seych-friends-api-url', apiUrl); } catch (_) {}
                    }
                    return data.data || {};
                } catch (err) {
                    lastErr = err;
                }
            }
            throw lastErr || new Error('Ошибка друзей');
        }

        function stopIncomingCallSound() {
            if (incomingCallSoundRetryTimer) {
                clearInterval(incomingCallSoundRetryTimer);
                incomingCallSoundRetryTimer = null;
            }
            if (!incomingCallSound) return;
            try {
                incomingCallSound.pause();
                incomingCallSound.currentTime = 0;
            } catch (_) {}
        }

        function tryPlayIncomingCallSound() {
            if (!incomingCallSound || !incomingCallModal) return;
            try {
                incomingCallSound.currentTime = 0;
                const playResult = incomingCallSound.play();
                if (playResult && typeof playResult.then === 'function') {
                    playResult
                        .then(() => {
                            if (incomingCallSoundRetryTimer) {
                                clearInterval(incomingCallSoundRetryTimer);
                                incomingCallSoundRetryTimer = null;
                            }
                        })
                        .catch(() => {});
                }
            } catch (_) {}
        }

        function startIncomingCallSound() {
            if (!friendsNotificationsEnabled) return;
            if (!incomingCallSound) {
                incomingCallSound = new Audio(getAssetUrl('upload/rington.mp3'));
                incomingCallSound.loop = true;
                incomingCallSound.preload = 'auto';
            }
            tryPlayIncomingCallSound();
            if (!incomingCallSoundRetryTimer) {
                incomingCallSoundRetryTimer = setInterval(() => {
                    if (!incomingCallModal) {
                        clearInterval(incomingCallSoundRetryTimer);
                        incomingCallSoundRetryTimer = null;
                        return;
                    }
                    tryPlayIncomingCallSound();
                }, 1200);
            }
        }

        function clearIncomingCallAutoDeclineTimeout() {
            if (!incomingCallAutoDeclineTimeout) return;
            clearTimeout(incomingCallAutoDeclineTimeout);
            incomingCallAutoDeclineTimeout = null;
        }

        function scheduleIncomingCallAutoDecline(inviteId) {
            const normalizedInviteId = String(inviteId || '').trim();
            if (!normalizedInviteId) return;
            clearIncomingCallAutoDeclineTimeout();
            incomingCallAutoDeclineTimeout = setTimeout(() => {
                const activeModalInviteId = String(incomingCallModal?.dataset?.inviteId || '').trim();
                if (!activeModalInviteId || activeModalInviteId !== normalizedInviteId) return;
                replyIncomingCall(normalizedInviteId, 'decline').catch(() => {});
                showNotification('Звонок другу', 'Вызов автоматически сброшен через 30 секунд', 'warning');
            }, 30000);
        }

        function closeIncomingCallModal() {
            stopIncomingCallSound();
            clearIncomingCallAutoDeclineTimeout();
            if (!incomingCallModal) return;
            try { incomingCallModal.remove(); } catch (_) {}
            incomingCallModal = null;
        }

        function handleServiceWorkerMessage(event) {
            const payload = event?.data || null;
            if (!payload || payload.type !== 'friend-call-declined-from-push') return;
            const declinedInviteId = String(payload.inviteId || '').trim();
            const modalInviteId = String(incomingCallModal?.dataset?.inviteId || '').trim();
            if (declinedInviteId && modalInviteId && declinedInviteId === modalInviteId) {
                closeIncomingCallModal();
            }
            refreshFriendsState(true).catch(() => {});
        }

        function closeIncomingFriendModal() {
            if (!incomingFriendModal) return;
            try { incomingFriendModal.remove(); } catch (_) {}
            incomingFriendModal = null;
        }

        function setAuthenticatedProfile(profile) {
            saveProfile(profile);
            window.location.reload();
            return;
            const roomToJoin = pendingRoomJoin;
            pendingRoomJoin = null;
            const contactsPromise = profile?.provider === 'telegram'
                ? fetchTelegramContactsFromApi()
                : profile?.provider === 'vk'
                    ? fetchVkFriendsFromApi()
                    : Promise.resolve();
            contactsPromise.finally(() => {
                ensureFriendsRuntime();
                refreshFriendsState(true).finally(() => {
                    renderMainScreen();
                });
                if (roomToJoin) {
                    joinRoom(roomToJoin);
                }
            });
        }

        function ensureFriendsRuntime() {
            if (!authProfile?.appUserId) return;
            friendsNotificationsEnabled = getStoredFriendsNotifyValue();
            syncPushContextToServiceWorker().catch(() => {});
            loadKnownOutgoingCallStatuses();
            if (friendsNotificationsEnabled) {
                ensureSystemNotificationPermission(true).catch(() => {});
                ensurePushNotificationsReady().catch(() => {});
            } else {
                disablePushNotificationsSubscription().catch(() => {});
            }
            friendsSearchValue = '';
            friendsSearchResults = [];
            if (friendsPollTimer) {
                clearInterval(friendsPollTimer);
                friendsPollTimer = null;
            }
            registerFriendsAccount().catch(() => {});
            refreshFriendsState(true).catch(() => {});
            friendsPollTimer = setInterval(() => {
                refreshFriendsState(true).catch(() => {});
            }, 3500);
        }

        async function registerFriendsAccount() {
            if (!authProfile?.appUserId) return;
            const payload = await friendsApiRequest('register', {
                app_user_id: authProfile.appUserId,
                name: authProfile.name || userName || 'Пользователь',
                avatar: authProfile.avatar || '',
                external_key: String(authProfile.externalKey || buildExternalAccountKey(authProfile) || ''),
                previous_app_user_id: pendingLegacyAppUserId || ''
            });
            pendingLegacyAppUserId = '';
            const canonicalAppUserId = String(payload?.appUserId || payload?.user?.id || '').trim();
            if (canonicalAppUserId && canonicalAppUserId !== String(authProfile?.appUserId || '').trim()) {
                saveProfile({
                    ...authProfile,
                    appUserId: canonicalAppUserId
                });
                syncPushContextToServiceWorker().catch(() => {});
            }
        }

        function handleFriendsStateSideEffects(previousState, nextState, primeIncomingCallModals = false) {
            const previousIncomingIds = new Set((previousState?.incomingCalls || []).map((item) => item.inviteId));
            const incomingCalls = Array.isArray(nextState?.incomingCalls) ? nextState.incomingCalls : [];
            const incomingIds = new Set(incomingCalls.map((item) => String(item?.inviteId || '')).filter(Boolean));
            const activeModalInviteId = String(incomingCallModal?.dataset?.inviteId || '').trim();
            if (activeModalInviteId && !incomingIds.has(activeModalInviteId)) {
                closeIncomingCallModal();
                showNotification('Звонок другу', 'Вызов отменен', 'info');
            }
            if (primeIncomingCallModals) {
                incomingCalls.forEach((invite) => {
                    if (invite?.inviteId) knownIncomingCallIds.add(invite.inviteId);
                });
            } else {
                incomingCalls.forEach((invite) => {
                    if (!invite?.inviteId) return;
                    knownIncomingCallIds.add(invite.inviteId);
                    if (previousIncomingIds.has(invite.inviteId)) return;
                    showSystemNotification('Входящий звонок', `${invite.fromName || 'Друг'} звонит вам`, `friend-call-${invite.inviteId}`);
                    showIncomingCallInviteModal(invite);
                });
            }

            const outgoingCalls = Array.isArray(nextState?.outgoingCalls) ? nextState.outgoingCalls : [];
            outgoingCalls.forEach((item) => {
                if (!item?.inviteId) return;
                const previousStatus = knownOutgoingCallStatuses.get(item.inviteId) || '';
                touchKnownOutgoingCallStatus(item.inviteId, item.status);
                if (previousStatus && previousStatus === item.status) return;
                const isActiveRoom = !!roomId && !!item.roomId && roomId === item.roomId;
                const isActiveInvite = !!outgoingFriendCallSession?.inviteId && outgoingFriendCallSession.inviteId === item.inviteId;
                const isActiveFriendCall = isActiveRoom || isActiveInvite;
                if (!previousStatus && !isActiveFriendCall) return;
                if (item.status === 'accepted') {
                    if (isActiveFriendCall) {
                        acceptOutgoingFriendCallSession();
                    }
                    showNotification('Звонок другу', `${item.toName || 'Друг'} ответил на звонок`, 'success');
                }
                if (item.status === 'declined' || item.status === 'cancelled') {
                    if (isActiveFriendCall) {
                        const targetName = outgoingFriendCallSession?.targetName || item.toName || 'Друг';
                        clearOutgoingFriendCallSession();
                        showNotification('Звонок другу', `${targetName} отклонил вызов`, 'warning');
                        if (roomId) {
                            endCall(false);
                        }
                        return;
                    }
                    if (previousStatus) {
                        showNotification('Звонок другу', `Друг ${item.toName || ''} сбросил`, 'warning');
                    }
                }
            });
            persistKnownOutgoingCallStatuses();

            const previousIncomingRequests = new Set((previousState?.incomingRequests || []).map((item) => item.requestId));
            const incomingRequests = Array.isArray(nextState?.incomingRequests) ? nextState.incomingRequests : [];
            incomingRequests.forEach((request) => {
                if (!request?.requestId) return;
                if (previousIncomingRequests.has(request.requestId)) return;
                showSystemNotification('Новый запрос в друзья', `${request.name || 'Пользователь'} отправил вам заявку`, `friend-request-${request.requestId}`);
                showIncomingFriendRequestModal(request.fromId, request.name || 'Пользователь');
            });
        }

        function syncSearchResultsWithFriendsState() {
            if (!Array.isArray(friendsSearchResults) || !friendsSearchResults.length) return;
            const friendIds = new Set((friendsState.friends || []).map((item) => String(item.id || '')));
            const incomingIds = new Set((friendsState.incomingRequests || []).map((item) => String(item.fromId || '')));
            const outgoingIds = new Set((friendsState.outgoingRequests || []).map((item) => String(item.toId || '')));
            friendsSearchResults = friendsSearchResults.map((result) => {
                const userId = String(result?.id || '');
                const isFriend = friendIds.has(userId);
                return {
                    ...result,
                    isFriend,
                    incomingPending: !isFriend && incomingIds.has(userId),
                    outgoingPending: !isFriend && outgoingIds.has(userId)
                };
            });
        }

        async function refreshFriendsState(silent = false) {
            if (!authProfile?.appUserId) return;
            try {
                const payload = await friendsApiRequest('state', {
                    app_user_id: authProfile.appUserId,
                    name: authProfile.name || userName || 'Пользователь',
                    avatar: authProfile.avatar || ''
                });
                if (messengerProfileOverrides.size && Array.isArray(payload.friends)) {
                    payload.friends = payload.friends.map((f) => {
                        const id = String(f?.id || '');
                        const ov = messengerProfileOverrides.get(id);
                        if (!ov) return f;
                        return {
                            ...f,
                            name: ov.name || f.name,
                            displayName: ov.displayName || f.displayName || ov.name || f.name,
                            avatar: ov.avatar || f.avatar,
                            username: ov.username || f.username || '',
                            statusText: ov.statusText || f.statusText || '',
                            initials: ov.initials || f.initials || ''
                        };
                    });
                }
                const previous = friendsState;
                friendsState = {
                    friends: Array.isArray(payload.friends) ? payload.friends : [],
                    incomingRequests: Array.isArray(payload.incomingRequests) ? payload.incomingRequests : [],
                    outgoingRequests: Array.isArray(payload.outgoingRequests) ? payload.outgoingRequests : [],
                    incomingCalls: Array.isArray(payload.incomingCalls) ? payload.incomingCalls : [],
                    outgoingCalls: Array.isArray(payload.outgoingCalls) ? payload.outgoingCalls : []
                };
                sendMessengerEvent({
                    type: 'messenger-friends-sync',
                    friendIds: (friendsState.friends || []).map((f) => String(f.id || '').trim()).filter(Boolean)
                });
                syncSearchResultsWithFriendsState();
                const primeCalls = !friendsCallsModalPrimed;
                friendsCallsModalPrimed = true;
                handleFriendsStateSideEffects(previous, friendsState, primeCalls);
                if (!roomId) {
                    const ae = document.activeElement;
                    const composing = messengerView === 'chats' && ae && ae.id === 'chatComposerInput';
                    if (!composing) {
                        renderMainScreen();
                    }
                }
            } catch (error) {
                if (!silent) {
                    showNotification('Друзья', error.message || 'Ошибка обновления друзей', 'error');
                }
            }
        }

        function findIncomingRequestByUser(userId) {
            const list = Array.isArray(friendsState.incomingRequests) ? friendsState.incomingRequests : [];
            return list.find((item) => item.fromId === userId) || null;
        }

        async function searchFriendsUsers() {
            const input = document.getElementById('friendsSearchInput');
            friendsSearchValue = String(input?.value ?? friendsSearchValue ?? '').trim();
            if (!friendsSearchValue) {
                friendsSearchResults = [];
                renderMainScreen();
                return;
            }
            try {
                const payload = await friendsApiRequest('search', { query: friendsSearchValue });
                friendsSearchResults = Array.isArray(payload.results)
                    ? payload.results.map((item) => ({
                        ...item,
                        username: ensureGeneratedMessengerUsername(item?.username || '', item?.id || '')
                    }))
                    : [];
                renderMainScreen();
            } catch (error) {
                const msg = String(error?.message || '');
                if (/Invalid JSON/i.test(msg) || /<html/i.test(msg)) {
                    friendsSearchResults = [];
                    renderMainScreen();
                    showNotification('Друзья', 'Поиск временно недоступен: API вернул некорректный ответ', 'warning');
                    return;
                }
                showNotification('Друзья', error.message || 'Ошибка поиска', 'error');
            }
        }

        async function sendFriendRequest(targetId) {
            if (!targetId) return;
            try {
                await friendsApiRequest('send_request', { target_id: targetId });
                showNotification('Друзья', 'Запрос отправлен', 'success');
                await refreshFriendsState(true);
                await searchFriendsUsers();
            } catch (error) {
                showNotification('Друзья', error.message || 'Ошибка отправки запроса', 'error');
            }
        }

        async function handleFriendRequest(requestId, decision) {
            if (!requestId) return;
            try {
                await friendsApiRequest('respond_request', {
                    request_id: requestId,
                    decision
                });
                showNotification('Друзья', decision === 'accept' ? 'Заявка принята' : 'Заявка отклонена', 'info');
                await refreshFriendsState(true);
            } catch (error) {
                showNotification('Друзья', error.message || 'Ошибка обработки заявки', 'error');
            }
        }

        async function deleteFriend(friendId) {
            if (!friendId) return;
            try {
                await friendsApiRequest('remove_friend', { friend_id: friendId });
                showNotification('Друзья', 'Друг удален', 'info');
                await refreshFriendsState(true);
            } catch (error) {
                showNotification('Друзья', error.message || 'Ошибка удаления', 'error');
            }
        }

        async function callFriend(friendId) {
            if (!friendId) return;
            try {
                if (!roomId) {
                    const createdRoomId = await createRoom({ privateRoom: true, silent: true, friendCallTargetId: friendId });
                    if (!createdRoomId) return;
                }
                if (isCreator && ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'set-room-private', enabled: true }));
                }
                const friend = (friendsState.friends || []).find((item) => String(item.id || '') === String(friendId)) || null;
                const inviteResponse = await friendsApiRequest('send_call_invite', {
                    target_id: friendId,
                    room_id: roomId
                });
                startOutgoingFriendCallSession({
                    inviteId: inviteResponse?.inviteId || '',
                    roomId,
                    targetId: friendId,
                    targetName: friend?.name || 'другу'
                });
                showNotification('Звонок другу', 'Вызов отправлен', 'success');
                await refreshFriendsState(true);
            } catch (error) {
                showNotification('Звонок другу', error.message || 'Не удалось позвонить другу', 'error');
            }
        }

        async function replyIncomingCall(inviteId, decision) {
            if (!inviteId) return;
            if (decision === 'answer') {
                primeCallAudioSession();
            }
            clearIncomingCallAutoDeclineTimeout();
            try {
                const payload = await friendsApiRequest('respond_call_invite', {
                    invite_id: inviteId,
                    decision
                });
                closeIncomingCallModal();
                await refreshFriendsState(true);
                if (decision === 'answer' && payload.roomId) {
                    joinRoom(payload.roomId);
                }
            } catch (error) {
                showNotification('Звонок другу', error.message || 'Ошибка ответа на звонок', 'error');
            }
        }

        function showIncomingCallInviteModal(invite) {
            if (!invite?.inviteId || incomingCallModal) return;
            closeIncomingCallModal();
            startIncomingCallSound();
            scheduleIncomingCallAutoDecline(invite.inviteId);
            const modal = document.createElement('div');
            modal.className = 'request-modal';
            modal.dataset.inviteId = String(invite.inviteId || '');
            modal.innerHTML = `
                <div class="request-content">
                    <div style="font-size: 42px;"><i class="fas fa-phone-volume"></i></div>
                    <h3>Входящий звонок</h3>
                    <p>${escapeHtml(invite.fromName || 'Друг')} приглашает в комнату</p>
                    <div class="request-buttons">
                        <button class="request-btn cancel" onclick="replyIncomingCall('${invite.inviteId}','decline')">Сбросить</button>
                        <button class="request-btn confirm" onclick="replyIncomingCall('${invite.inviteId}','answer')">Ответить</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            incomingCallModal = modal;
        }

        function showIncomingFriendRequestModal(fromAccountId, fromName) {
            if (!fromAccountId || incomingFriendModal || !friendsNotificationsEnabled) return;
            const modal = document.createElement('div');
            modal.className = 'request-modal';
            modal.innerHTML = `
                <div class="request-content">
                    <div style="font-size: 42px;"><i class="fas fa-user-plus"></i></div>
                    <h3>Запрос в друзья</h3>
                    <p>${escapeHtml(fromName || 'Пользователь')} хочет добавить вас в друзья</p>
                    <div class="request-buttons">
                        <button class="request-btn cancel" onclick="closeIncomingFriendModal()">Отмена</button>
                        <button class="request-btn confirm" onclick="acceptIncomingFriendFromModal('${fromAccountId}')">Принять</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            incomingFriendModal = modal;
        }

        async function acceptIncomingFriendFromModal(fromAccountId) {
            closeIncomingFriendModal();
            const request = findIncomingRequestByUser(fromAccountId);
            if (request?.requestId) {
                await handleFriendRequest(request.requestId, 'accept');
                return;
            }
            await sendFriendRequest(fromAccountId);
        }

        function decodeJwtPayload(token) {
            const payload = token.split('.')[1] || '';
            const normalized = payload.replace(/-/g, '+').replace(/_/g, '/');
            const decoded = atob(normalized);
            return JSON.parse(decoded);
        }

        function getVkRedirectUri() {
            return VK_REDIRECT_URL || `${window.location.origin}${window.location.pathname}`;
        }

        function vkApiCallJsonp(method, params, accessToken) {
            return new Promise((resolve, reject) => {
                const callbackName = `vkcb_${Date.now()}_${Math.random().toString(36).slice(2)}`;
                const query = new URLSearchParams({
                    ...params,
                    access_token: accessToken,
                    v: VK_API_VERSION,
                    callback: callbackName
                });
                const url = `https://api.vk.com/method/${method}?${query.toString()}`;
                const script = document.createElement('script');
                const cleanup = () => {
                    try { delete window[callbackName]; } catch (_) { window[callbackName] = undefined; }
                    script.remove();
                };
                const timeoutId = setTimeout(() => {
                    cleanup();
                    reject(new Error('VK API timeout'));
                }, 8000);
                window[callbackName] = (data) => {
                    clearTimeout(timeoutId);
                    cleanup();
                    if (data?.error) {
                        reject(new Error(data.error.error_msg || 'VK API error'));
                        return;
                    }
                    resolve(data?.response || null);
                };
                script.src = url;
                script.onerror = () => {
                    clearTimeout(timeoutId);
                    cleanup();
                    reject(new Error('VK API request failed'));
                };
                document.body.appendChild(script);
            });
        }

        async function vkApiCall(method, params, accessToken) {
            return vkApiCallJsonp(method, params, accessToken);
        }

        async function fetchVkProfile(accessToken, userId) {
            const params = { fields: 'photo_200,domain' };
            if (userId) {
                params.user_ids = userId;
            }
            const response = await vkApiCall('users.get', params, accessToken);
            const user = Array.isArray(response) ? response[0] : null;
            if (!user) {
                throw new Error('VK profile not found');
            }
            const name = `${user.first_name || ''} ${user.last_name || ''}`.trim() || 'VK User';
            return {
                id: String(user.id || userId),
                name,
                avatar: proxifyAvatarUrl(user.photo_200 || ''),
                username: user.domain || ''
            };
        }

        async function fetchVkFriendsFromApi() {
            if (!authProfile || authProfile.provider !== 'vk' || !authProfile.vkAccessToken) {
                vkContacts = [];
                return;
            }
            try {
                const response = await vkApiCall('friends.get', { fields: 'photo_200,domain' }, authProfile.vkAccessToken);
                const items = Array.isArray(response?.items) ? response.items : [];
                vkContacts = items.map((friend) => ({
                    id: String(friend.id || ''),
                    name: `${friend.first_name || ''} ${friend.last_name || ''}`.trim() || 'Друг',
                    avatar: proxifyAvatarUrl(friend.photo_200 || ''),
                    username: friend.domain || ''
                })).filter(contact => contact.id);
            } catch (_) {
                vkContacts = [];
            }
        }

        async function resolveVkUserByInput(value) {
            const normalized = normalizeVkUserInput(value);
            if (!normalized) {
                throw new Error('Укажите корректный VK ID или ссылку');
            }
            if (!authProfile || authProfile.provider !== 'vk' || !authProfile.vkAccessToken) {
                throw new Error('Добавление доступно только после входа через VK');
            }
            const response = await vkApiCall('users.get', { user_ids: normalized, fields: 'photo_200,domain' }, authProfile.vkAccessToken);
            const user = Array.isArray(response) ? response[0] : null;
            if (!user) {
                throw new Error('Пользователь VK не найден');
            }
            return {
                id: String(user.id || ''),
                name: `${user.first_name || ''} ${user.last_name || ''}`.trim() || 'VK User',
                avatar: proxifyAvatarUrl(user.photo_200 || ''),
                username: user.domain || ''
            };
        }

        function vkidOnSuccess(data) {
            const accessToken = data?.access_token || data?.accessToken || '';
            const userId = data?.user_id || data?.userId || '';
            const expiresIn = Number(data?.expires_in || 0);
            if (!accessToken || !userId) {
                if (!accessToken) {
                    showNotification('VK', 'Не удалось получить токен VK', 'error');
                    return;
                }
            }
            fetchVkProfile(accessToken, userId)
                .then((profile) => {
                    setAuthenticatedProfile({
                        provider: 'vk',
                        name: profile.name,
                        avatar: profile.avatar,
                        vkUserId: String(profile.id),
                        vkAccessToken: accessToken,
                        vkTokenExpiresAt: expiresIn ? Date.now() + expiresIn * 1000 : null,
                        vkUsername: profile.username || ''
                    });
                })
                .catch((error) => {
                    showNotification('VK', error?.message || 'Не удалось получить профиль VK', 'error');
                });
        }

        function vkidOnError(error) {
            const message = error?.message || error?.error_description || error?.error || 'Ошибка авторизации VK';
            showNotification('VK', message, 'error');
        }

        function handleTelegramAuth(user) {
            if (!user) {
                showNotification('Авторизация', 'Не удалось получить профиль Telegram', 'error');
                return;
            }
            const fullName = [user.first_name, user.last_name].filter(Boolean).join(' ').trim() || user.username || 'Telegram User';
            setAuthenticatedProfile({
                provider: 'telegram',
                name: fullName,
                avatar: proxifyAvatarUrl(user.photo_url || ''),
                telegramId: user.id ? String(user.id) : '',
                username: user.username ? String(user.username) : ''
            });
        }

        function tryTelegramWebAppAuth() {
            const tgUser = window.Telegram?.WebApp?.initDataUnsafe?.user;
            if (tgUser) {
                handleTelegramAuth(tgUser);
                return true;
            }
            return false;
        }

        function renderTelegramWidget() {
            const container = document.getElementById('telegramAuthWidget');
            if (!container) return;
            if (!TELEGRAM_BOT_USERNAME || TELEGRAM_BOT_USERNAME.includes('YOUR_')) {
                container.innerHTML = '<span style="opacity:0.8;font-size:13px">Укажите TELEGRAM_BOT_USERNAME в index.html</span>';
                return;
            }
            container.innerHTML = '';
            const fallback = document.createElement('button');
            fallback.className = 'btn';
            fallback.innerHTML = '<i class="fab fa-telegram"></i> Войти через Telegram';
            fallback.onclick = () => {
                window.open(`https://t.me/${TELEGRAM_BOT_USERNAME}`, '_blank');
                showNotification('Telegram', 'Откройте бота и запустите приложение', 'info');
            };
            container.appendChild(fallback);
            const script = document.createElement('script');
            script.async = true;
            script.src = 'https://telegram.org/js/telegram-widget.js?22';
            script.setAttribute('data-telegram-login', TELEGRAM_BOT_USERNAME);
            script.setAttribute('data-size', 'large');
            script.setAttribute('data-userpic', 'true');
            script.setAttribute('data-radius', '10');
            script.setAttribute('data-request-access', 'write');
            script.setAttribute('data-onauth', 'handleTelegramAuth(user)');
            container.appendChild(script);
            setTimeout(() => {
                if (container.querySelector('iframe')) {
                    fallback.remove();
                }
            }, 1200);
        }

        function renderVkIdWidget() {
            const container = document.getElementById('vkAuthWidget');
            if (!container) return;
            if (!VK_CLIENT_ID || VK_CLIENT_ID.includes('YOUR_')) {
                container.innerHTML = '<span style="opacity:0.8;font-size:13px">Укажите VK_CLIENT_ID в index.html</span>';
                return;
            }
            if (!window.VKIDSDK) {
                container.innerHTML = '<span style="opacity:0.8;font-size:13px">Загружаем VKID SDK...</span>';
                setTimeout(() => {
                    if (!window.VKIDSDK) {
                        container.innerHTML = '<span style="opacity:0.8;font-size:13px">VKID SDK не загрузился</span>';
                        return;
                    }
                    renderVkIdWidget();
                }, 1500);
                return;
            }
            const VKID = window.VKIDSDK;
            VKID.Config.init({
                app: Number(VK_CLIENT_ID),
                redirectUrl: getVkRedirectUri(),
                responseMode: VKID.ConfigResponseMode.Callback,
                source: VKID.ConfigSource.LOWCODE,
                scope: 'friends'
            });
            const oneTap = new VKID.OneTap();
            oneTap.render({
                container,
                scheme: 'dark',
                showAlternativeLogin: true,
                styles: {
                    borderRadius: 41,
                    height: 38
                }
            })
                .on(VKID.WidgetEvents.ERROR, vkidOnError)
                .on(VKID.OneTapInternalEvents.LOGIN_SUCCESS, function (payload) {
                    const code = payload.code;
                    const deviceId = payload.device_id;
                    VKID.Auth.exchangeCode(code, deviceId)
                        .then(vkidOnSuccess)
                        .catch(vkidOnError);
                });
        }

        function signOutProfile() {
            stopQrAuthPolling();
            clearProfile();
            window.location.reload();
        }

        async function sendTelegramInvite(contactTarget, contactName, roomLink) {
            const payload = {
                target: contactTarget,
                contactName: contactName || '',
                roomLink,
                roomId,
                callerName: userName || 'Пользователь',
                callerUsername: authProfile?.username || ''
            };
            const response = await fetch(TELEGRAM_INVITE_API, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Ошибка отправки');
            }
        }

        async function fetchTelegramContactsFromApi() {
            if (!authProfile || authProfile.provider !== 'telegram' || !authProfile.telegramId) {
                telegramContacts = [];
                return;
            }
            try {
                const url = `${TELEGRAM_CONTACTS_API}?telegram_id=${encodeURIComponent(authProfile.telegramId)}`;
                const response = await fetch(url);
                const data = await response.json();
                if (data.success && Array.isArray(data.data?.contacts)) {
                    telegramContacts = data.data.contacts;
                } else {
                    telegramContacts = [];
                }
            } catch (_) {
                telegramContacts = [];
            }
        }

        function openTelegramDMFallback(contact, roomLink) {
            const username = String(contact?.username || '').replace(/^@/, '');
            if (!username) {
                showNotification('Не удалось отправить', 'У контакта нет @username. Напишите ему через бота /start', 'warning');
                return;
            }
            const currentRoom = roomId || parseRoomInput(roomLink) || '';
            const botLink = currentRoom ? buildTelegramRoomLink(currentRoom) : (authProfile?.provider === 'telegram' ? buildTelegramRoomLink(roomId) : roomLink);
            const webLink = currentRoom ? buildRoomLink(currentRoom) : roomLink;
            const caller = userName || 'Пользователь';
            const text = encodeURIComponent(
`🔔 Входящий звонок в Seych

Вас приглашает: ${caller}
Контакт: @${username}

Ответить в Telegram Mini App:
${botLink}

Ответить в браузере:
${webLink}

Если вы уже в приложении Telegram, откройте первую ссылку.`
            );
            window.open(`https://t.me/${username}?text=${text}`, '_blank');
            showNotification('Telegram', `Открыт чат с @${username} и готовым текстом`, 'info');
        }

        async function callTelegramContact(index) {
            if (!authProfile || authProfile.provider !== 'telegram') {
                showNotification('Контакты', 'Контакты Telegram доступны только для Telegram профиля', 'warning');
                return;
            }
            const contacts = mergeContacts();
            const contact = contacts[index];
            if (!contact) return;
            if (!roomId) {
                await createRoom();
            }
            const roomLink = buildRoomLink(roomId);
            openTelegramDMFallback(contact, roomLink);
        }

        async function addTelegramContactToApi(username) {
            if (!authProfile || authProfile.provider !== 'telegram' || !authProfile.telegramId) {
                throw new Error('Добавление контактов доступно только для Telegram профиля');
            }
            const response = await fetch(TELEGRAM_CONTACTS_API, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    action: 'add',
                    telegram_id: authProfile.telegramId,
                    username
                })
            });
            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Не удалось сохранить контакт');
            }
            if (Array.isArray(data.data?.contacts)) {
                telegramContacts = data.data.contacts;
            }
        }

        async function deleteTelegramContactFromApi(contactId) {
            if (!authProfile || authProfile.provider !== 'telegram' || !authProfile.telegramId) {
                throw new Error('Удаление контактов доступно только для Telegram профиля');
            }
            const response = await fetch(TELEGRAM_CONTACTS_API, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    action: 'delete',
                    telegram_id: authProfile.telegramId,
                    contact_id: contactId
                })
            });
            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Не удалось удалить контакт');
            }
            if (Array.isArray(data.data?.contacts)) {
                telegramContacts = data.data.contacts;
            }
        }

        function removeTelegramContact(contactId) {
            if (!contactId) return;
            deleteTelegramContactFromApi(contactId)
                .then(() => {
                    renderContactsModal();
                    showNotification('Контакты', 'Контакт удалён', 'info');
                })
                .catch((error) => {
                    showNotification('Контакты', error.message || 'Ошибка удаления контакта', 'error');
                });
        }

        function addTelegramContactFromModal() {
            const usernameEl = document.getElementById('tgContactUsernameInput');
            const username = normalizeTelegramUsername(usernameEl?.value || '');
            if (!username) {
                showNotification('Контакты', 'Укажите корректный @username', 'warning');
                return;
            }
            addTelegramContactToApi(username)
                .then(() => {
                    renderContactsModal();
                    showNotification('Контакты', 'Контакт найден и сохранён', 'success');
                })
                .catch((error) => {
                    showNotification('Контакты', error.message || 'Ошибка сохранения контакта', 'error');
                });
        }

        function renderContactsModal() {
            if (!authProfile || authProfile.provider !== 'telegram') {
                showNotification('Контакты', 'Контакты Telegram доступны только для Telegram профиля', 'warning');
                return;
            }
            const oldModal = document.getElementById('tgContactsModal');
            if (oldModal) oldModal.remove();
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'tgContactsModal';
            const contacts = mergeContacts();
            const contactsHtml = contacts.length
                ? contacts.map((contact, idx) => `
                    <div class="contact-item">
                        <div class="participant-avatar" style="width:38px;height:38px;min-width:38px">${contact.avatar ? `<img src="${escapeHtml(contact.avatar)}" alt="${escapeHtml(contact.name)}" style="width:100%;height:100%;border-radius:50%;object-fit:cover" referrerpolicy="no-referrer">` : escapeHtml((contact.name || '?').charAt(0).toUpperCase())}</div>
                        <div>
                            <div class="contact-name">${escapeHtml(contact.name)}</div>
                            <div class="contact-chat">${escapeHtml(contact.username || contact.target)}</div>
                        </div>
                        <div class="contact-actions">
                            <button class="contact-btn" onclick="callTelegramContact(${idx})"><i class="fas fa-phone"></i></button>
                            <button class="contact-btn delete" onclick="removeTelegramContact('${escapeHtml(contact.id)}')"><i class="fas fa-trash"></i></button>
                        </div>
                    </div>
                `).join('')
                : `<div class="contact-item"><div class="contact-chat">Нет доступных контактов. Попросите друга написать вашему боту /start</div></div>`;
            modal.innerHTML = `
                <div class="modal-content">
                    <h2><i class="fab fa-telegram"></i> Telegram контакты</h2>
                    <div class="contacts-header">
                        <div class="contacts-title">Добавить по username</div>
                        <div class="contacts-title">Список: ${contacts.length}</div>
                    </div>
                    <div class="contacts-form">
                        <input type="text" id="tgContactUsernameInput" class="modal-input" placeholder="@username Telegram" />
                        <div class="contacts-form-actions">
                            <button class="modal-btn confirm" onclick="addTelegramContactFromModal()">Добавить</button>
                            <button class="modal-btn cancel" onclick="refreshTelegramContacts()">Обновить</button>
                        </div>
                    </div>
                    <div class="modal-buttons">
                        <button class="modal-btn cancel" onclick="document.getElementById('tgContactsModal').remove()">Закрыть</button>
                    </div>
                    <div class="contacts-list">${contactsHtml}</div>
                </div>
            `;
            document.body.appendChild(modal);
        }

        function openVkDM(contact, roomLink) {
            const id = String(contact?.id || '');
            if (!id) return;
            const text = `Я тебе звоню в Seych\nСсылки для ответа\n${roomLink}`;
            const writeUrl = new URL(`https://vk.com/write${id}`);
            writeUrl.searchParams.set('text', text);
            const fallbackUrl = new URL('https://vk.com/im');
            fallbackUrl.searchParams.set('sel', id);
            fallbackUrl.searchParams.set('text', text);
            window.open(writeUrl.toString(), '_blank');
            setTimeout(() => {
                window.open(fallbackUrl.toString(), '_blank');
            }, 350);
            if (navigator.clipboard?.writeText) {
                navigator.clipboard.writeText(text).catch(() => {});
            }
        }

        async function callVkContact(index) {
            if (!authProfile || authProfile.provider !== 'vk') {
                showNotification('Контакты', 'Контакты VK доступны только для VK профиля', 'warning');
                return;
            }
            const contacts = mergeVkContacts();
            const contact = contacts[index];
            if (!contact) return;
            if (!roomId) {
                const createdRoomId = await createRoom();
                if (!createdRoomId) {
                    ensureInviteRoomId();
                }
            }
            const roomLink = buildRoomLink(roomId);
            openVkDM(contact, roomLink);
            showNotification('VK', `Открыт диалог с ${contact.name}`, 'success');
        }

        function renderVkContactsModal() {
            if (!authProfile || authProfile.provider !== 'vk') {
                showNotification('Контакты', 'Контакты VK доступны только для VK профиля', 'warning');
                return;
            }
            const oldModal = document.getElementById('vkContactsModal');
            if (oldModal) oldModal.remove();
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'vkContactsModal';
            const contacts = mergeVkContacts();
            const contactsHtml = contacts.length
                ? contacts.map((contact, idx) => `
                    <div class="contact-item">
                        <div class="participant-avatar" style="width:38px;height:38px;min-width:38px">${contact.avatar ? `<img src="${escapeHtml(contact.avatar)}" alt="${escapeHtml(contact.name)}" style="width:100%;height:100%;border-radius:50%;object-fit:cover" referrerpolicy="no-referrer">` : escapeHtml((contact.name || '?').charAt(0).toUpperCase())}</div>
                        <div>
                            <div class="contact-name">${escapeHtml(contact.name)}</div>
                            <div class="contact-chat">${escapeHtml(contact.username ? `vk.com/${contact.username}` : `id${contact.id}`)}</div>
                        </div>
                        <div class="contact-actions">
                            <button class="contact-btn" onclick="callVkContact(${idx})"><i class="fas fa-phone"></i></button>
                            <button class="contact-btn delete" onclick="removeVkContact('${escapeHtml(contact.id)}')"><i class="fas fa-trash"></i></button>
                        </div>
                    </div>
                `).join('')
                : `<div class="contact-item"><div class="contact-chat">Нет доступных друзей VK</div></div>`;
            modal.innerHTML = `
                <div class="modal-content">
                    <h2><i class="fab fa-vk"></i> Друзья VK</h2>
                    <div class="contacts-header">
                        <div class="contacts-title">Добавить по ссылке</div>
                        <div class="contacts-title">Список: ${contacts.length}</div>
                    </div>
                    <div class="contacts-form">
                        <input type="text" id="vkContactInput" class="modal-input" placeholder="vk.com/username или id123" />
                        <div class="contacts-form-actions">
                            <button class="modal-btn confirm" onclick="addVkContactFromModal()">Добавить</button>
                            <button class="modal-btn cancel" onclick="refreshVkContacts()">Обновить</button>
                        </div>
                    </div>
                    <div class="modal-buttons">
                        <button class="modal-btn cancel" onclick="document.getElementById('vkContactsModal').remove()">Закрыть</button>
                    </div>
                    <div class="contacts-list">${contactsHtml}</div>
                </div>
            `;
            document.body.appendChild(modal);
        }

        function addVkContactFromModal() {
            const inputEl = document.getElementById('vkContactInput');
            const value = inputEl?.value || '';
            resolveVkUserByInput(value)
                .then((user) => {
                    const existing = vkCustomContacts.find(contact => String(contact.id) === String(user.id));
                    if (existing) {
                        existing.name = user.name;
                        existing.avatar = user.avatar;
                        existing.username = user.username;
                        saveVkCustomContacts(vkCustomContacts);
                    } else {
                        saveVkCustomContacts([...vkCustomContacts, user]);
                    }
                    renderVkContactsModal();
                    showNotification('VK', 'Контакт добавлен', 'success');
                })
                .catch((error) => {
                    showNotification('VK', error.message || 'Ошибка добавления', 'error');
                });
        }

        function removeVkContact(contactId) {
            const id = String(contactId || '').trim();
            if (!id) return;
            if (!vkHiddenContactIds.includes(id)) {
                saveVkHiddenContacts([...vkHiddenContactIds, id]);
            }
            renderVkContactsModal();
            showNotification('VK', 'Контакт скрыт из списка', 'info');
        }

        async function refreshVkContacts() {
            await fetchVkFriendsFromApi();
            renderVkContactsModal();
        }

        async function refreshTelegramContacts() {
            await fetchTelegramContactsFromApi();
            renderContactsModal();
        }

        let silentAudioUnlockEl = null;

        /** Разблокировка autoplay в том же жесте, что «Войти/Создать/Ответить» — без отдельной кнопки. */
        function primeCallAudioSession() {
            audioPlaybackUnlocked = true;
            try {
                const Ctx = window.AudioContext || window.webkitAudioContext;
                if (Ctx && (!audioContextRef || audioContextRef.state === 'closed')) {
                    audioContextRef = new Ctx();
                }
                if (audioContextRef && audioContextRef.state === 'suspended') {
                    audioContextRef.resume().catch(() => {});
                }
            } catch (_) {}
            try {
                if (!silentAudioUnlockEl) {
                    silentAudioUnlockEl = document.createElement('audio');
                    silentAudioUnlockEl.playsInline = true;
                    silentAudioUnlockEl.setAttribute('playsinline', '');
                    silentAudioUnlockEl.muted = true;
                    silentAudioUnlockEl.volume = 0.001;
                    silentAudioUnlockEl.style.cssText = 'position:fixed;width:0;height:0;opacity:0;pointer-events:none';
                    document.body.appendChild(silentAudioUnlockEl);
                }
                silentAudioUnlockEl.src = 'data:audio/wav;base64,UklGRigAAABXQVZFZm10IBIAAAABAAEARKwAAIhYAQACABAAAABkYXRhAgAAAAEA';
                const sp = silentAudioUnlockEl.play();
                if (sp && typeof sp.catch === 'function') sp.catch(() => {});
            } catch (_) {}
            tryPlayAllRemoteAudio();
        }

        function tryPlayAllRemoteAudio() {
            remoteAudioEls.forEach((audioEl) => {
                if (!audioEl || !audioEl.srcObject) return;
                try {
                    audioEl.muted = false;
                    audioEl.volume = 1;
                    const p = audioEl.play();
                    if (p && typeof p.catch === 'function') {
                        p.catch(() => {});
                    }
                } catch (_) {}
            });
        }

        function safeWsSend(payload) {
            if (!ws || ws.readyState !== WebSocket.OPEN) return false;
            let data = '';
            try {
                data = typeof payload === 'string' ? payload : JSON.stringify(payload);
            } catch (_) {
                return false;
            }
            try {
                if (ws.bufferedAmount > WS_SEND_BUFFER_HIGH_WATER) {
                    return false;
                }
                ws.send(data);
                return true;
            } catch (_) {
                return false;
            }
        }

        function clearAvRecoveryTimers() {
            avPeerRecoverTimers.forEach((timerId) => clearTimeout(timerId));
            avPeerRecoverTimers.clear();
            iceRestartTimers.forEach((timerId) => clearTimeout(timerId));
            iceRestartTimers.clear();
        }

        function stopConnectionQualityMonitor() {
            if (connectionQualityTimer) {
                clearInterval(connectionQualityTimer);
                connectionQualityTimer = null;
            }
        }

        function startConnectionQualityMonitor() {
            stopConnectionQualityMonitor();
            connectionQualityTimer = setInterval(() => {
                if (!roomId || !isConnected) return;
                refreshConnectionQuality();
            }, 8000);
        }

        function cleanupCallMediaResources() {
            stopCallAudioHealTimer();
            stopConnectionQualityMonitor();
            clearAvRecoveryTimers();
            try {
                cancelAnimationFrame(animationId);
            } catch (_) {}
            if (detectLoopTimer) {
                clearTimeout(detectLoopTimer);
                detectLoopTimer = null;
            }
            if (audioContextRef && audioContextRef.state !== 'closed') {
                try { audioContextRef.close(); } catch (_) {}
            }
            audioContextRef = null;
            if (isScreenSharing) {
                try { stopScreenShare(); } catch (_) {}
            }
            try {
                if (screenStreamLocal) {
                    screenStreamLocal.getTracks().forEach((t) => {
                        try { t.stop(); } catch (_) {}
                    });
                }
            } catch (_) {}
            screenStreamLocal = null;
            localScreenShareId = null;
            peers.forEach((peer) => {
                try { peer.destroy(); } catch (_) {}
            });
            peers.clear();
            screenConnMap.clear();
            remoteMediaStreams.clear();
            stopRemoteAudio();
            try {
                if (localStream) {
                    localStream.getTracks().forEach((t) => {
                        try { t.stop(); } catch (_) {}
                    });
                }
            } catch (_) {}
            localStream = null;
            rawMicTrack = null;
            videoTiles.forEach((tile, key) => {
                if (key === 'self') return;
                try { tile.remove(); } catch (_) {}
            });
            Array.from(videoTiles.keys()).forEach((key) => {
                if (key !== 'self') videoTiles.delete(key);
            });
            screenTiles.forEach((tile, key) => {
                if (key === 'self-screen') return;
                try { tile.remove(); } catch (_) {}
            });
            Array.from(screenTiles.keys()).forEach((key) => {
                if (key !== 'self-screen') screenTiles.delete(key);
            });
            connectingAudioParticipants.clear();
            audioPlaybackUnlocked = false;
        }

        function renderAuthScreen() {
            stopQrAuthPolling();
            const isDesktop = isDesktopDeviceType();
            const showClassic = authShowClassicProviders || !isDesktop;
            const qrPanelHtml = isDesktop ? `
                <div id="authQrPanel" class="${showClassic ? 'auth-panel-hidden' : ''}">
                    <div class="auth-qr-wrap">
                        <div id="authQrHost" class="auth-qr-host" aria-label="QR-код для входа">
                            <div class="auth-qr-loading"><i class="fas fa-circle-notch fa-spin"></i></div>
                        </div>
                        <div id="authQrSuccess" class="auth-qr-success">
                            <div class="auth-qr-check" aria-hidden="true">
                                <svg viewBox="0 0 52 52"><path d="M14 27 L22 35 L38 17"/></svg>
                            </div>
                        </div>
                    </div>
                    <p class="auth-qr-hint">Откройте Seych на телефоне → Настройки → Устройства → сканируйте код</p>
                    <button type="button" class="auth-alt-link" onclick="toggleAuthClassicProviders()">Другой способ входа</button>
                </div>
            ` : '';
            const providersPanelClass = isDesktop && !showClassic ? 'auth-panel-hidden' : '';
            const mobileAltLink = !isDesktop ? `
                <button type="button" class="auth-alt-link" onclick="toggleAuthClassicProviders()">${authShowClassicProviders ? 'Скрыть другие способы' : 'Другие способы входа'}</button>
                ${authShowClassicProviders ? `<button type="button" class="auth-alt-link" style="margin-top:6px;display:block;margin-left:auto;margin-right:auto;" onclick="openQrScannerModal()">Сканировать QR для входа на компьютере</button>` : ''}
            ` : '';
            const desktopBackLink = isDesktop && showClassic
                ? `<button type="button" class="auth-alt-link" onclick="toggleAuthClassicProviders()">← Вход по QR-коду</button>`
                : '';
            document.getElementById('app').innerHTML = `
                <div class="main-screen main-screen--auth">
                    <div class="gradient-bg"></div>
                    <div class="auth-card ${isDesktop ? 'auth-card--qr' : ''}">
                        <h2><i class="fas fa-shield-alt"></i> Вход в Seych</h2>
                        ${isDesktop && !showClassic
                            ? '<p class="auth-subtitle">Отсканируйте QR-код в приложении на телефоне</p>'
                            : '<p class="auth-subtitle">Авторизуйтесь через Telegram или VK</p>'}
                        ${qrPanelHtml}
                        <div id="authProvidersPanel" class="auth-providers ${providersPanelClass}">
                            ${desktopBackLink}
                            <div class="auth-provider" id="telegramAuthWidget"></div>
                            <div class="auth-provider" id="vkAuthWidget"></div>
                        </div>
                        ${mobileAltLink}
                    </div>
                </div>
            `;
            if (!tryTelegramWebAppAuth()) {
                renderTelegramWidget();
            }
            renderVkIdWidget();
            if (isDesktop && !showClassic) {
                requestAnimationFrame(() => {
                    startDesktopQrAuthFlow();
                });
            }
            const qrTokenFromUrl = parseQrLoginFromLocation();
            if (qrTokenFromUrl && authProfile?.appUserId) {
                openQrApproveModal(qrTokenFromUrl);
            } else if (qrTokenFromUrl && !authProfile) {
                pendingQrLoginToken = qrTokenFromUrl;
            }
        }

        function buildMicCaptureConstraintsRich() {
            const dev = selectedMicDeviceId ? { deviceId: { exact: selectedMicDeviceId } } : {};
            const on = (x) => (x ? { ideal: true } : false);
            const audio = {
                echoCancellation: on(!!echoCancellationEnabled),
                noiseSuppression: false,
                autoGainControl: on(!!autoGainControlEnabled),
                channelCount: { ideal: 1 },
                sampleRate: { ideal: 48000 },
                latency: { ideal: 0 },
                // Professional voice quality
                sampleSize: { ideal: 16 },
                // Additional professional settings
                suppressLocalAudioPlayback: { ideal: true },
                ...dev
            };
            try {
                const ua = navigator.userAgent || '';
                if (/Chrome/i.test(ua) && !/Edg\//i.test(ua)) {
                    audio.googEchoCancellation = on(!!echoCancellationEnabled);
                    audio.googAutoGainControl = on(!!autoGainControlEnabled);
                    audio.googNoiseSuppression = false;
                    audio.googHighpassFilter = { ideal: false };
                }
            } catch (_) {}
            return { audio };
        }

        function buildMicCaptureConstraintsPlain() {
            const dev = selectedMicDeviceId ? { deviceId: { exact: selectedMicDeviceId } } : {};
            return {
                audio: {
                    echoCancellation: !!echoCancellationEnabled,
                    noiseSuppression: false,
                    autoGainControl: !!autoGainControlEnabled,
                    channelCount: 1,
                    ...dev
                }
            };
        }

        async function acquireMicMediaStream() {
            try {
                return await navigator.mediaDevices.getUserMedia(buildMicCaptureConstraintsRich());
            } catch (_) {
                return navigator.mediaDevices.getUserMedia(buildMicCaptureConstraintsPlain());
            }
        }

        async function getMedia() {
            try {
                const stream = await acquireMicMediaStream();
                try {
                    rawMicTrack = stream.getAudioTracks()[0] || null;
                } catch (_) {}
                return stream;
            } catch (error) {
                showNotification('Ошибка', 'Нет доступа к микрофону', 'error');
                return null;
            }
        }

        function hasLiveCallAudioTrack(stream) {
            if (!stream || typeof stream.getAudioTracks !== 'function') return false;
            return stream.getAudioTracks().some((track) => track && track.readyState === 'live' && track.enabled !== false);
        }

        function hasUsableLocalCallStream() {
            return hasLiveCallAudioTrack(localStream);
        }

        async function restoreCallMediaIfNeeded() {
            if (!roomId) return true;
            if (hasUsableLocalCallStream()) return true;
            try {
                if (localStream) {
                    localStream.getTracks().forEach((track) => {
                        try { track.stop(); } catch (_) {}
                    });
                }
            } catch (_) {}
            const stream = await getMedia();
            if (!stream) return false;
            localStream = stream;
            setupAudioDetection(stream);
            applyMicOutgoingChain();
            return true;
        }

        function setupAudioDetection(stream) {
            try {
                cancelAnimationFrame(animationId);
            } catch (_) {}
            if (detectLoopTimer) {
                clearTimeout(detectLoopTimer);
                detectLoopTimer = null;
            }
            if (audioContextRef && audioContextRef.state !== 'closed') {
                try { audioContextRef.close(); } catch (_) {}
            }
            audioContextRef = null;
            const audioContext = new (window.AudioContext || window.webkitAudioContext)();
            audioContextRef = audioContext;
            audioPlaybackUnlocked = true;
            if (audioContext.state === 'suspended') {
                audioContext.resume().catch(() => {});
            }
            const source = audioContext.createMediaStreamSource(stream);
            const analyser = audioContext.createAnalyser();
            analyser.fftSize = 1024;
            source.connect(analyser);
            const buffer = new Uint8Array(analyser.fftSize);
            let open = 0.08;
            let close = 0.04;
            const track = stream.getAudioTracks()[0] || null;
            const scheduleDetectNext = () => {
                if (document.hidden) {
                    detectLoopTimer = setTimeout(detect, 220);
                } else {
                    animationId = requestAnimationFrame(detect);
                }
            };
            function stopDetect() {
                try { cancelAnimationFrame(animationId); } catch (_) {}
                if (detectLoopTimer) {
                    clearTimeout(detectLoopTimer);
                    detectLoopTimer = null;
                }
                if (isSpeaking) {
                    isSpeaking = false;
                    ws?.send(JSON.stringify({ type: 'speaking', isSpeaking }));
                    updateUI();
                }
            }
            if (track) {
                track.onended = () => stopDetect();
            }
            function detect() {
                if (!audioEnabled || !track || track.readyState !== 'live' || audioContext.state === 'closed') {
                    if (isSpeaking) {
                        isSpeaking = false;
                        ws?.send(JSON.stringify({ type: 'speaking', isSpeaking }));
                        updateUI();
                    }
                    scheduleDetectNext();
                    return;
                }
                if (audioContext.state === 'suspended') {
                    audioContext.resume().catch(() => {});
                }
                try {
                    analyser.getByteTimeDomainData(buffer);
                    let sum = 0;
                    for (let i = 0; i < buffer.length; i++) {
                        const v = (buffer[i] - 128) / 128;
                        sum += v * v;
                    }
                    const rms = Math.sqrt(sum / buffer.length);
                    const nextSpeaking = isSpeaking ? rms > close : rms > open;
                    if (nextSpeaking !== isSpeaking) {
                        isSpeaking = nextSpeaking;
                        ws?.send(JSON.stringify({ type: 'speaking', isSpeaking }));
                        updateUI();
                    }
                } catch (_) {
                    stopDetect();
                    return;
                }
                scheduleDetectNext();
            }
            if (audioContext.state === 'suspended') {
                const resumeAudio = () => audioContext.resume().catch(() => {});
                document.addEventListener('click', resumeAudio, { once: true });
                document.addEventListener('touchstart', resumeAudio, { once: true, passive: true });
            }
            detect();
        }

        function toDomSafeIdKey(value) {
            return String(value || '')
                .replace(/[^a-zA-Z0-9_-]/g, '_')
                .slice(0, 120);
        }

        function playRemoteAudio(participantId, stream) {
            if (!participantId || !stream) return;
            const key = String(participantId);
            let el = remoteAudioEls.get(key);
            if (!el) {
                el = document.createElement('audio');
                el.id = `remoteAudio-${toDomSafeIdKey(key)}`;
                el.autoplay = true;
                el.playsInline = true;
                el.setAttribute('playsinline', '');
                el.style.display = 'none';
                document.body.appendChild(el);
                remoteAudioEls.set(key, el);
            }
            el.srcObject = stream;
            el.muted = false;
            el.volume = 1;
            try {
                if (selectedSpeakerDeviceId && typeof el.setSinkId === 'function') {
                    el.setSinkId(selectedSpeakerDeviceId).catch(() => {});
                }
            } catch (_) {}
            const attemptPlay = () => {
                if (!el.srcObject) return;
                let p;
                try {
                    p = el.play();
                } catch (_) {
                    p = null;
                }
                if (!p || typeof p.then !== 'function') {
                    if (!String(participantId).startsWith('screen:')) connectingAudioParticipants.add(String(participantId));
                    return;
                }
                p.then(() => {
                    if (!String(participantId).startsWith('screen:')) connectingAudioParticipants.delete(String(participantId));
                }).catch(() => {
                    if (!String(participantId).startsWith('screen:')) connectingAudioParticipants.add(String(participantId));
                });
            };
            const schedulePlayRetries = () => {
                attemptPlay();
                [80, 250, 600, 1200, 2500, 5000].forEach((ms) => {
                    setTimeout(attemptPlay, ms);
                });
            };
            el.onloadedmetadata = schedulePlayRetries;
            el.oncanplay = schedulePlayRetries;
            schedulePlayRetries();
        }

        function stopRemoteAudio(participantId = null) {
            if (participantId) {
                const key = String(participantId);
                const el = remoteAudioEls.get(key);
                if (el) {
                    el.srcObject = null;
                    el.remove();
                    remoteAudioEls.delete(key);
                }
                if (!key.startsWith('screen:')) connectingAudioParticipants.delete(key);
                return;
            }
            remoteAudioEls.forEach((el) => {
                el.srcObject = null;
                el.remove();
            });
            remoteAudioEls.clear();
            connectingAudioParticipants = new Set();
        }

        function applySpeakerDeviceToAllAudio() {
            try {
                if (!selectedSpeakerDeviceId) return;
                remoteAudioEls.forEach((el) => {
                    try {
                        if (el && typeof el.setSinkId === 'function') {
                            el.setSinkId(selectedSpeakerDeviceId).catch(() => {});
                        }
                    } catch (_) {}
                });
                if (watchPartyMediaElement && typeof watchPartyMediaElement.setSinkId === 'function') {
                    watchPartyMediaElement.setSinkId(selectedSpeakerDeviceId).catch(() => {});
                }
            } catch (_) {}
        }

        function ensureScreenSharePeersForParticipants() {
            if (!isScreenSharing || !screenStreamLocal || !localScreenShareId) return;
            participants.forEach((name, id) => {
                if (id === myId) return;
                const screenKey = `screen-local-${id}`;
                const existing = peers.get(screenKey);
                if (existing && !existing.destroyed) return;
                const connId = `${localScreenShareId}:${id}`;
                const screenPeer = createPeer(screenStreamLocal, 'screen', true, id, name, connId);
                peers.set(screenKey, screenPeer);
                screenConnMap.set(connId, screenKey);
            });
        }

        function syncLocalMediaStateToServer() {
            if (!ws || ws.readyState !== WebSocket.OPEN) return;
            ws.send(JSON.stringify({ type: 'toggle-audio', enabled: !!audioEnabled }));
            ws.send(JSON.stringify({ type: 'toggle-video', enabled: !!videoEnabled }));
            ws.send(JSON.stringify({ type: 'speaking', isSpeaking: !!isSpeaking }));
            if (videoEnabled) {
                syncCameraFacingMode();
            }
            if (isScreenSharing && screenStreamLocal) {
                ensureScreenSharePeersForParticipants();
                ws.send(JSON.stringify({ type: 'start-screen', from: userName }));
            }
        }

        function cleanupConnectionsForReconnect() {
            peers.forEach((peer) => {
                try { peer.destroy(); } catch (_) {}
            });
            peers.clear();
            screenConnMap.clear();
            avPeerRecoverTimers.forEach((timerId) => clearTimeout(timerId));
            avPeerRecoverTimers.clear();
            audioRecoverCooldown.clear();
            remoteMediaStreams.clear();
            stopRemoteAudio();
            videoTiles.forEach((tile, key) => {
                if (key === 'self') return;
                try { tile.remove(); } catch (_) {}
            });
            Array.from(videoTiles.keys()).forEach((key) => {
                if (key !== 'self') videoTiles.delete(key);
            });
            screenTiles.forEach((tile, key) => {
                if (key === 'self-screen' && isScreenSharing && screenStreamLocal) return;
                try { tile.remove(); } catch (_) {}
            });
            Array.from(screenTiles.keys()).forEach((key) => {
                if (key !== 'self-screen') screenTiles.delete(key);
            });
            if (isScreenSharing && screenStreamLocal) {
                addScreenTile('self-screen', userName, screenStreamLocal);
            }
            updatePrimaryRemoteState();
            updateUI();
            updateEmptyState();
        }

        function canReconnectWsSession() {
            if (!wsLastInitialMsg) return false;
            const type = String(wsLastInitialMsg?.type || '').trim();
            if (type === 'messenger-register') {
                return !!String(authProfile?.appUserId || appUserId || '').trim() && !roomId;
            }
            return !!roomId && !!localStream;
        }

        function teardownActiveWsSocket() {
            wsConnectSessionId += 1;
            if (wsHeartbeatTimer) {
                clearInterval(wsHeartbeatTimer);
                wsHeartbeatTimer = null;
            }
            if (wsReconnectTimer) {
                clearTimeout(wsReconnectTimer);
                wsReconnectTimer = null;
            }
            wsReconnectInProgress = false;
            wsReconnectAttempts = 0;
            wsLastPingAt = 0;
            const activeSocket = ws;
            if (activeSocket) {
                try {
                    activeSocket.onopen = null;
                    activeSocket.onmessage = null;
                    activeSocket.onerror = null;
                    activeSocket.onclose = null;
                    activeSocket.__closingByUser = true;
                    activeSocket.close();
                } catch (_) {}
            }
            ws = null;
            currentWsType = '';
            isConnected = false;
        }

        function ensureMessengerWsConnection() {
            if (!authProfile || roomId) return;
            if (ws && ws.readyState === WebSocket.OPEN && currentWsType === 'messenger-register') {
                syncMessengerIdentity();
                flushPendingMessengerEvents();
                return;
            }
            if (ws && ws.readyState === WebSocket.CONNECTING) return;
            const msg = {
                type: 'messenger-register',
                appUserId: authProfile.appUserId || appUserId,
                deviceSessionId: getDeviceSessionId(),
                userName: authProfile.name || userName || '',
                userAvatar: authProfile.avatar || userAvatar || ''
            };
            connectWS(msg);
        }

        function markWsActivity(kind = 'message') {
            const now = Date.now();
            wsLastActivityAt = now;
            if (kind === 'pong' || kind === 'open') {
                wsLastPongAt = now;
            }
        }

        function isWsHealthy() {
            if (!ws || ws.readyState !== WebSocket.OPEN) return false;
            const now = Date.now();
            const lastSignalAt = Math.max(wsLastActivityAt || 0, wsLastPongAt || 0, 0);
            if (!lastSignalAt) return false;
            const idleLimitMs = document.hidden ? 90000 : 45000;
            if (now - lastSignalAt > idleLimitMs) return false;
            if (wsLastPingAt > 0 && wsLastPongAt < wsLastPingAt && (now - wsLastPingAt) > 18000) return false;
            return true;
        }

        function performWsReconnect(reason = 'manual-reconnect') {
            if (wsReconnectInProgress || !canReconnectWsSession()) return false;
            const now = Date.now();
            if (now - wsLastForcedReconnectAt < 2500) return false;
            wsLastForcedReconnectAt = now;
            if (wsReconnectTimer) {
                clearTimeout(wsReconnectTimer);
                wsReconnectTimer = null;
            }
            if (wsHeartbeatTimer) {
                clearInterval(wsHeartbeatTimer);
                wsHeartbeatTimer = null;
            }
            const activeSocket = ws;
            if (activeSocket) {
                try {
                    activeSocket.onmessage = null;
                    activeSocket.onerror = null;
                } catch (_) {}
                try {
                    activeSocket.__closingByUser = true;
                    activeSocket.close(4001, reason);
                } catch (_) {}
                if (ws === activeSocket) {
                    ws = null;
                }
            }
            currentWsType = '';
            isConnected = false;
            wsReconnectInProgress = true;
            updateMessengerSidebarStatus();
            cleanupConnectionsForReconnect();
            connectWS(wsLastInitialMsg, true);
            wsReconnectInProgress = false;
            updateMessengerSidebarStatus();
            updateUI();
            updateEmptyState();
            return true;
        }

        function ensureWsAlive(reason = 'health-check') {
            if (!canReconnectWsSession()) return false;
            if (!ws || ws.readyState !== WebSocket.OPEN) {
                reconnectNow();
                return true;
            }
            if (!isWsHealthy()) {
                updateMessengerSidebarStatus();
                return performWsReconnect(reason);
            }
            return false;
        }

        function scheduleWsReconnect() {
            if (wsReconnectTimer || wsReconnectInProgress) return;
            if (!canReconnectWsSession()) return;
            if (typeof navigator !== 'undefined' && navigator.onLine === false) return;
            const delay = Math.min(8000, Math.floor(900 * Math.pow(1.8, wsReconnectAttempts)));
            wsReconnectAttempts += 1;
            updateMessengerSidebarStatus();
            wsReconnectTimer = setTimeout(() => {
                wsReconnectTimer = null;
                if (!canReconnectWsSession()) return;
                wsReconnectInProgress = true;
                updateMessengerSidebarStatus();
                cleanupConnectionsForReconnect();
                connectWS(wsLastInitialMsg, true);
                wsReconnectInProgress = false;
                updateMessengerSidebarStatus();
            }, delay);
        }

        function reconnectNow() {
            if (wsReconnectInProgress) return;
            if (!canReconnectWsSession()) return;
            if (ws && ws.readyState === WebSocket.OPEN) {
                if (isWsHealthy()) return;
                performWsReconnect('forced-reconnect-now');
                return;
            }
            if (wsReconnectTimer) {
                clearTimeout(wsReconnectTimer);
                wsReconnectTimer = null;
            }
            wsReconnectInProgress = true;
            cleanupConnectionsForReconnect();
            connectWS(wsLastInitialMsg, true);
            wsReconnectInProgress = false;
        }

        function recoverAfterTabWakeup() {
            const continueRecovery = () => {
                if (!canReconnectWsSession()) return;
                if (audioContextRef && audioContextRef.state === 'suspended') {
                    audioContextRef.resume().catch(() => {});
                }
                ensureWsAlive('tab-wakeup');
                if (ws?.readyState === WebSocket.OPEN) {
                    syncLocalMediaStateToServer();
                } else {
                    reconnectNow();
                }
                if (!roomId) return;
                getRemoteParticipantIds().forEach((participantId) => {
                    ensureAvPeerForParticipant(participantId, shouldInitiatePeer(myId, participantId));
                    syncRemoteAudioPlayback(participantId);
                });
                setTimeout(healRemoteAudioLinks, 700);
                setTimeout(healRemoteAudioLinks, 1800);
                if (isScreenSharing) {
                    ensureScreenSharePeersForParticipants();
                }
            };
            if (roomId) {
                void restoreCallMediaIfNeeded().then((ready) => {
                    if (!ready) {
                        showNotification('Звонок', 'Не удалось восстановить микрофон после паузы вкладки', 'warning');
                        return;
                    }
                    continueRecovery();
                });
                return;
            }
            continueRecovery();
        }

        function recoverRemoteAudioForParticipant(participantId) {
            if (!participantId || participantId === myId) return;
            const now = Date.now();
            const lastAttempt = audioRecoverCooldown.get(participantId) || 0;
            if (now - lastAttempt < 2500) return;
            audioRecoverCooldown.set(participantId, now);
            recreateAvPeerForParticipant(participantId);
        }

        function syncRemoteAudioPlayback(participantId) {
            if (!participantId) return;
            const state = getParticipantState(participantId);
            if (!state || !state.audio) {
                stopRemoteAudio(participantId);
                connectingAudioParticipants.delete(String(participantId));
                return;
            }
            const mediaStream = remoteMediaStreams.get(participantId);
            if (!mediaStream || !mediaStream.getAudioTracks) {
                connectingAudioParticipants.add(String(participantId));
                stopRemoteAudio(participantId);
                recoverRemoteAudioForParticipant(participantId);
                return;
            }
            const activeAudioTracks = mediaStream.getAudioTracks().filter((track) => track && track.readyState !== 'ended');
            if (!activeAudioTracks.length) {
                connectingAudioParticipants.add(String(participantId));
                stopRemoteAudio(participantId);
                recoverRemoteAudioForParticipant(participantId);
                return;
            }
            playRemoteAudio(participantId, new MediaStream(activeAudioTracks));
            connectingAudioParticipants.delete(String(participantId));
        }

        function healRemoteAudioLinks() {
            if (roomId) {
                tryPlayAllRemoteAudio();
            }
            getRemoteParticipantIds().forEach((participantId) => {
                const state = getParticipantState(participantId);
                if (!state || !state.audio) return;
                ensureAvPeerForParticipant(participantId, shouldInitiatePeer(myId, participantId));
                syncRemoteAudioPlayback(participantId);
            });
            if (roomId) {
                setTimeout(tryPlayAllRemoteAudio, 300);
            }
        }

        function stopCallAudioHealTimer() {
            if (callAudioHealTimer) {
                clearInterval(callAudioHealTimer);
                callAudioHealTimer = null;
            }
        }

        function startCallAudioHealTimer() {
            stopCallAudioHealTimer();
            callAudioHealTimer = setInterval(() => {
                if (!roomId || !isConnected) return;
                healRemoteAudioLinks();
            }, 40000);
        }

        function addVideoTile(userId, userName, stream) {
            const container = document.getElementById('videosContainer');
            let tile = videoTiles.get(userId);
            
            if (tile) {
                tile.style.display = '';
                const video = tile.querySelector('video');
                if (video && video.srcObject !== stream) {
                    video.srcObject = stream;
                    if (userId === 'self') {
                        video.muted = true;
                    }
                    video.play().catch(() => {});
                }
                applyVideoTileMirroring(userId);
                updateEmptyState();
                return;
            }

            tile = document.createElement('div');
            tile.id = `video-${userId}`;
            tile.className = 'video-tile camera-tile';
            
            if (userId === 'self') {
                tile.classList.add('self-video');
            }

            const video = document.createElement('video');
            video.autoplay = true;
            video.playsInline = true;
            video.srcObject = stream;
            
            if (userId === 'self') {
                video.muted = true;
            }
            
            video.play().catch(() => {});
            applyVideoTileMirroring(userId);

            const label = document.createElement('div');
            label.className = 'video-label';
            label.innerHTML = `<i class="fas fa-video"></i> ${escapeHtml(userName)}`;

            tile.appendChild(video);
            tile.appendChild(label);
            tile.onclick = () => toggleFullscreen(tile);
            container.appendChild(tile);
            videoTiles.set(userId, tile);
            
            updateEmptyState();
        }

        function setVideoTileVisibility(userId, visible) {
            const tile = videoTiles.get(userId);
            if (!tile) return;
            tile.style.display = visible ? '' : 'none';
            updateEmptyState();
        }

        function removeVideoTile(userId) {
            const tile = videoTiles.get(userId);
            if (tile) {
                tile.remove();
                videoTiles.delete(userId);
            }
            updateEmptyState();
        }

        function addScreenTile(userId, userName, stream) {
            const container = document.getElementById('videosContainer');
            let tile = screenTiles.get(userId);
            if (tile) tile.remove();

            tile = document.createElement('div');
            tile.id = `screen-${userId}`;
            tile.className = 'video-tile screen-tile';

            const video = document.createElement('video');
            video.autoplay = true;
            video.playsInline = true;
            video.muted = true;
            video.srcObject = stream;
            
            const tryPlay = () => video.play().catch(() => {});
            video.onloadedmetadata = tryPlay;
            video.oncanplay = tryPlay;
            setTimeout(() => {
                if (video.readyState < 2 || video.videoWidth === 0) {
                    tryPlay();
                }
            }, 500);

            const label = document.createElement('div');
            label.className = 'video-label';
            label.innerHTML = `<i class="fas fa-desktop"></i> ${escapeHtml(userName)} - экран`;

            tile.appendChild(video);
            tile.appendChild(label);
            tile.onclick = () => toggleFullscreen(tile);
            container.appendChild(tile);
            screenTiles.set(userId, tile);

            try {
                const audioTracks = stream && stream.getAudioTracks ? stream.getAudioTracks() : [];
                const liveAudio = (audioTracks || []).filter((t) => t && t.readyState === 'live');
                if (userId !== 'self-screen' && liveAudio.length) {
                    playRemoteAudio(`screen:${userId}`, new MediaStream(liveAudio));
                }
            } catch (_) {}
            
            updateEmptyState();
        }

        function removeScreenTile(userId) {
            const tile = screenTiles.get(userId);
            if (tile) {
                tile.remove();
                screenTiles.delete(userId);
            }
            stopRemoteAudio(`screen:${userId}`);
            updateEmptyState();
        }

        function normalizeWatchUrl(input) {
            let value = String(input || '').trim();
            if (!value) return '';
            if (!/^https?:\/\//i.test(value)) {
                value = `https://${value}`;
            }
            try {
                const parsed = new URL(value);
                if (!/^https?:$/i.test(parsed.protocol)) return '';
                return parsed.toString();
            } catch (_) {
                return '';
            }
        }

        function extractYoutubeId(url) {
            try {
                const parsed = new URL(url);
                const host = parsed.hostname.toLowerCase();
                if (host.includes('youtu.be')) {
                    return parsed.pathname.replace(/\//g, '').trim();
                }
                if (host.includes('youtube.com')) {
                    const fromQuery = parsed.searchParams.get('v');
                    if (fromQuery) return fromQuery.trim();
                    const parts = parsed.pathname.split('/').filter(Boolean);
                    const markerIndex = parts.findIndex((part) => ['embed', 'shorts', 'live'].includes(part));
                    if (markerIndex >= 0 && parts[markerIndex + 1]) {
                        return parts[markerIndex + 1].trim();
                    }
                }
            } catch (_) {}
            return '';
        }

        function applyWatchPartyVolume() {
            const level = Math.max(0, Math.min(100, Math.round(watchPartyVolume)));
            const normalized = Math.max(0, Math.min(1, level / 100));
            if (watchPartyMediaElement) {
                watchPartyMediaElement.volume = normalized;
            }
            if (typeof watchPartyVolumeApplier === 'function') {
                try { watchPartyVolumeApplier(level, normalized); } catch (_) {}
            }
        }

        function createWatchMediaNode(url) {
            const youtubeId = extractYoutubeId(url);
            if (youtubeId) {
                const frame = document.createElement('iframe');
                const origin = encodeURIComponent(window.location.origin);
                frame.src = `https://www.youtube.com/embed/${encodeURIComponent(youtubeId)}?autoplay=1&rel=0&modestbranding=1&playsinline=1&enablejsapi=1&origin=${origin}`;
                frame.allow = 'autoplay; fullscreen; picture-in-picture';
                frame.allowFullscreen = true;
                return {
                    node: frame,
                    supportsVolume: true,
                    mediaElement: null,
                    afterMount: () => {
                        watchPartyVolumeApplier = (level) => {
                            const target = frame.contentWindow;
                            if (!target) return;
                            const payload = (func, args = []) => JSON.stringify({ event: 'command', func, args });
                            try {
                                target.postMessage(payload('setVolume', [level]), '*');
                                target.postMessage(payload('unMute'), '*');
                            } catch (_) {}
                        };
                        const tryApply = () => {
                            if (!frame.isConnected) return;
                            applyWatchPartyVolume();
                        };
                        frame.addEventListener('load', tryApply);
                        setTimeout(tryApply, 250);
                        setTimeout(tryApply, 900);
                    }
                };
            }
            if (/\.(mp4|webm|mov|m3u8)(\?|#|$)/i.test(url)) {
                const video = document.createElement('video');
                video.autoplay = true;
                video.playsInline = true;
                video.controls = true;
                video.src = url;
                video.volume = Math.max(0, Math.min(1, watchPartyVolume / 100));
                video.play().catch(() => {});
                return {
                    node: video,
                    supportsVolume: true,
                    mediaElement: video,
                    afterMount: () => {
                        watchPartyVolumeApplier = null;
                        applyWatchPartyVolume();
                    }
                };
            }
            if (/\.(mp3|wav|ogg|m4a|aac|flac)(\?|#|$)/i.test(url)) {
                const audioWrap = document.createElement('div');
                audioWrap.style.width = '100%';
                audioWrap.style.height = '100%';
                audioWrap.style.display = 'flex';
                audioWrap.style.alignItems = 'center';
                audioWrap.style.justifyContent = 'center';
                audioWrap.style.background = 'radial-gradient(circle at 30% 30%, rgba(102,126,234,0.35), rgba(17,12,33,0.95))';
                const audio = document.createElement('audio');
                audio.autoplay = true;
                audio.controls = true;
                audio.src = url;
                audio.volume = Math.max(0, Math.min(1, watchPartyVolume / 100));
                audio.style.width = '86%';
                audio.play().catch(() => {});
                audioWrap.appendChild(audio);
                return {
                    node: audioWrap,
                    supportsVolume: true,
                    mediaElement: audio,
                    afterMount: () => {
                        watchPartyVolumeApplier = null;
                        applyWatchPartyVolume();
                    }
                };
            }
            const frame = document.createElement('iframe');
            frame.src = url;
            frame.allow = 'autoplay; fullscreen; encrypted-media; picture-in-picture';
            frame.allowFullscreen = true;
            return {
                node: frame,
                supportsVolume: false,
                mediaElement: null,
                afterMount: () => {
                    watchPartyVolumeApplier = null;
                }
            };
        }

        function removeWatchPartyTile() {
            if (watchPartyTile) {
                watchPartyTile.remove();
                watchPartyTile = null;
            }
            watchPartyMediaElement = null;
            watchPartySupportsVolume = false;
            watchPartyVolumeApplier = null;
            updateEmptyState();
        }

        function clearWatchFocusTimer() {
            if (!watchFocusIdleTimer) return;
            clearTimeout(watchFocusIdleTimer);
            watchFocusIdleTimer = null;
        }

        function triggerWatchFocusActivity() {
            if (!watchFocusEnabled) return;
            const callScreen = document.querySelector('.call-screen');
            if (!callScreen) return;
            callScreen.classList.remove('ui-idle');
            clearWatchFocusTimer();
            watchFocusIdleTimer = setTimeout(() => {
                if (!watchFocusEnabled) return;
                const currentCallScreen = document.querySelector('.call-screen');
                if (!currentCallScreen) return;
                currentCallScreen.classList.add('ui-idle');
            }, 2400);
        }

        function applyWatchFocusMode(enabled) {
            const callScreen = document.querySelector('.call-screen');
            if (!callScreen && !enabled) {
                watchFocusEnabled = false;
                clearWatchFocusTimer();
                document.removeEventListener('mousemove', triggerWatchFocusActivity);
                document.removeEventListener('touchstart', triggerWatchFocusActivity);
                document.removeEventListener('keydown', triggerWatchFocusActivity);
                return;
            }
            if (!!enabled === watchFocusEnabled) {
                return;
            }
            watchFocusEnabled = !!enabled;
            if (watchFocusEnabled) {
                if (callScreen) {
                    callScreen.classList.add('watch-focus');
                    callScreen.classList.remove('ui-idle');
                }
                document.addEventListener('mousemove', triggerWatchFocusActivity, { passive: true });
                document.addEventListener('touchstart', triggerWatchFocusActivity, { passive: true });
                document.addEventListener('keydown', triggerWatchFocusActivity);
                triggerWatchFocusActivity();
            } else {
                clearWatchFocusTimer();
                document.removeEventListener('mousemove', triggerWatchFocusActivity);
                document.removeEventListener('touchstart', triggerWatchFocusActivity);
                document.removeEventListener('keydown', triggerWatchFocusActivity);
                if (callScreen) {
                    callScreen.classList.remove('watch-focus');
                    callScreen.classList.remove('ui-idle');
                }
            }
        }

        function renderWatchPartyTile() {
            const container = document.getElementById('videosContainer');
            if (!container) return;
            if (!watchPartyState || !watchPartyState.url) {
                removeWatchPartyTile();
                return;
            }
            removeWatchPartyTile();

            const tile = document.createElement('div');
            tile.id = 'watch-party-tile';
            tile.className = 'video-tile screen-tile watch-tile';

            const media = createWatchMediaNode(watchPartyState.url);
            watchPartyMediaElement = media.mediaElement || null;
            watchPartySupportsVolume = !!media.supportsVolume;

            const label = document.createElement('div');
            label.className = 'video-label';
            label.innerHTML = `<i class="fas fa-users-viewfinder"></i> ${watchPartyState.ownerName || 'Совместный просмотр'}`;

            const controls = document.createElement('div');
            controls.className = 'watch-controls';
            controls.innerHTML = `
                <i class="fas fa-volume-up"></i>
                <input id="watchVolumeRange" type="range" min="0" max="100" step="1" value="${watchPartyVolume}">
                <span id="watchVolumeValue">${watchPartyVolume}%</span>
            `;
            controls.onclick = (event) => event.stopPropagation();
            controls.ontouchstart = (event) => event.stopPropagation();

            tile.appendChild(media.node);
            tile.appendChild(label);
            tile.appendChild(controls);
            tile.onclick = () => toggleFullscreen(tile);
            container.appendChild(tile);
            if (typeof media.afterMount === 'function') {
                media.afterMount();
            }

            watchPartyTile = tile;
            const volumeRange = tile.querySelector('#watchVolumeRange');
            const volumeValue = tile.querySelector('#watchVolumeValue');
            if (volumeRange) {
                volumeRange.oninput = (event) => {
                    const nextValue = Number(event.target.value);
                    watchPartyVolume = Number.isFinite(nextValue) ? nextValue : watchPartyVolume;
                    if (volumeValue) {
                        volumeValue.textContent = `${watchPartyVolume}%`;
                    }
                    applyWatchPartyVolume();
                };
                volumeRange.onchange = volumeRange.oninput;
            }
            updateEmptyState();
        }

        function canStartWatchParty() {
            if (!watchPartyState) return true;
            return watchPartyState.ownerId === myId || isCreator || isGuestAdmin;
        }

        function canStopWatchParty() {
            if (!watchPartyState) return false;
            return watchPartyState.ownerId === myId || isCreator || isGuestAdmin;
        }

        function showWatchPartyModal() {
            if (!ws || ws.readyState !== WebSocket.OPEN) return;
            if (!canStartWatchParty()) {
                showNotification('Совместный просмотр', 'Только владелец просмотра, админ или создатель могут заменить ссылку', 'warning');
                return;
            }
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.innerHTML = `
                <div class="modal-content">
                    <h2><i class="fas fa-users-viewfinder"></i> Совместный просмотр</h2>
                    <input type="text" id="watchUrlInput" class="modal-input" placeholder="Вставьте ссылку на VK, YouTube, музыку или видео" value="${watchPartyState?.url ? escapeHtml(watchPartyState.url) : ''}">
                    <div class="modal-buttons">
                        <button class="modal-btn cancel" id="watchCancelBtn">Отмена</button>
                        <button class="modal-btn confirm" id="watchStartBtn">Запустить</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);

            const closeModal = () => modal.remove();
            document.getElementById('watchCancelBtn').onclick = closeModal;
            document.getElementById('watchStartBtn').onclick = () => {
                const inputEl = document.getElementById('watchUrlInput');
                const normalizedUrl = normalizeWatchUrl(inputEl?.value || '');
                if (!normalizedUrl) {
                    showNotification('Совместный просмотр', 'Укажите корректную ссылку', 'error');
                    return;
                }
                ws.send(JSON.stringify({ type: 'start-watch', url: normalizedUrl }));
                closeModal();
            };
        }

        function stopWatchParty() {
            if (!ws || ws.readyState !== WebSocket.OPEN || !watchPartyState) return;
            if (!canStopWatchParty()) {
                showNotification('Совместный просмотр', 'Остановить просмотр может владелец, админ или создатель', 'warning');
                return;
            }
            ws.send(JSON.stringify({ type: 'stop-watch' }));
        }

        function updateEmptyState() {
            const visibleVideoTiles = Array.from(videoTiles.values()).filter(tile => tile && tile.style.display !== 'none');
            const visibleVideoCount = visibleVideoTiles.length;
            const watchTileCount = watchPartyTile ? 1 : 0;
            const hasAnyTile = visibleVideoCount > 0 || screenTiles.size > 0 || watchTileCount > 0;
            const emptyDiv = document.getElementById('emptyCallDiv');
            const videosContainer = document.getElementById('videosContainer');
            const waitingMsg = document.getElementById('waitingMsg');
            const callTopbar = document.getElementById('callTopbar');
            const tilesCount = visibleVideoCount + screenTiles.size + watchTileCount;
            const singleScreenOnly = tilesCount === 1 && visibleVideoCount === 0;
            const watchOnlySingle = watchTileCount === 1 && visibleVideoCount === 0 && screenTiles.size === 0;
            
            if (videosContainer) {
                if (tilesCount <= 1) {
                    videosContainer.classList.add('single-view');
                } else {
                    videosContainer.classList.remove('single-view');
                }
                if (singleScreenOnly) {
                    videosContainer.classList.add('single-screen-mode');
                } else {
                    videosContainer.classList.remove('single-screen-mode');
                }
                if (waitingMsg && tilesCount > 0) {
                    waitingMsg.style.display = 'none';
                }
            }
            applyWatchFocusMode(watchOnlySingle);
            if (callTopbar) {
                callTopbar.classList.toggle('hidden', !hasAnyTile);
            }
            
            if (!hasAnyTile && videosContainer) {
                if (waitingMsg) waitingMsg.style.display = 'none';
                if (!emptyDiv) {
                    const empty = document.createElement('div');
                    empty.id = 'emptyCallDiv';
                    empty.className = 'empty-call';
                    empty.innerHTML = `
                        <i class="fas fa-phone-alt"></i>
                        <div class="empty-time-pill">
                            <div class="call-time" id="emptyCallTimer">00:00</div>
                        </div>
                        <div class="privacy-island" id="privacyIsland">
                            <span id="privacyIslandBadge" class="room-status ${roomIsPrivate ? 'private' : 'public'}" title="${roomIsPrivate ? 'Закрытая' : 'Публичная'}"><i class="fas ${roomIsPrivate ? 'fa-lock' : 'fa-globe'}"></i></span>
                            <span id="privacyIslandLabel" class="privacy-island-label">${roomIsPrivate ? 'Приватный' : 'Публичный'}</span>
                        </div>
                    `;
                    videosContainer.appendChild(empty);
                }
            } else if (emptyDiv) {
                emptyDiv.remove();
            }
        }

        function applyCallScreenPerformanceMode() {
            const callScreen = document.getElementById('callScreenRoot');
            if (!callScreen) return;
            const prefersReducedMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
            const isLowPowerDevice = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent || '')
                || (typeof navigator.hardwareConcurrency === 'number' && navigator.hardwareConcurrency <= 4)
                || (typeof navigator.deviceMemory === 'number' && navigator.deviceMemory <= 4);
            callScreen.classList.toggle('low-motion', prefersReducedMotion || isLowPowerDevice);
        }

        function toggleFullscreen(tile) {
            if (tile.classList.contains('fullscreen')) {
                tile.classList.remove('fullscreen');
                if (tile.__originParent) {
                    if (tile.__originNext && tile.__originNext.parentElement === tile.__originParent) {
                        tile.__originParent.insertBefore(tile, tile.__originNext);
                    } else {
                        tile.__originParent.appendChild(tile);
                    }
                }
                tile.__originParent = null;
                tile.__originNext = null;
                const btn = tile.querySelector('.close-fullscreen');
                if (btn) btn.remove();
            } else {
                document.querySelectorAll('.video-tile.fullscreen').forEach(t => {
                    t.classList.remove('fullscreen');
                    const btn = t.querySelector('.close-fullscreen');
                    if (btn) btn.remove();
                });
                tile.classList.add('fullscreen');
                tile.__originParent = tile.parentElement;
                tile.__originNext = tile.nextSibling;
                const callScreen = document.querySelector('.call-screen');
                if (callScreen) {
                    callScreen.appendChild(tile);
                } else {
                    document.body.appendChild(tile);
                }
                const closeBtn = document.createElement('button');
                closeBtn.className = 'close-fullscreen';
                closeBtn.innerHTML = '<i class="fas fa-times"></i>';
                closeBtn.onclick = (e) => {
                    e.stopPropagation();
                    tile.classList.remove('fullscreen');
                    if (tile.__originParent) {
                        if (tile.__originNext && tile.__originNext.parentElement === tile.__originParent) {
                            tile.__originParent.insertBefore(tile, tile.__originNext);
                        } else {
                            tile.__originParent.appendChild(tile);
                        }
                    }
                    tile.__originParent = null;
                    tile.__originNext = null;
                    closeBtn.remove();
                };
                tile.appendChild(closeBtn);
            }
            updateEmptyState();
        }

        function ensureInviteRoomId() {
            if (roomId) return roomId;
            roomId = generateRoomId();
            history.replaceState(null, '', `${getBasePath().replace(/\/$/, '')}/${roomId}`);
            return roomId;
        }

        async function createRoom(options = {}) {
            const privateRoom = !!options.privateRoom;
            const silent = !!options.silent;
            const friendCallTargetId = String(options.friendCallTargetId || '').trim();
            const friendCallMode = !!friendCallTargetId;
            const fixedRoomId = String(options.fixedRoomId || '').trim();
            const groupChatId = String(options.groupChatId || '').trim();
            const groupCallAllowedUserIds = Array.isArray(options.groupCallAllowedUserIds) ? options.groupCallAllowedUserIds : [];
            if (!authProfile) {
                showNotification('Авторизация', 'Сначала войдите через Telegram или VK', 'warning');
                renderAuthScreen();
                return null;
            }
            primeCallAudioSession();
            resetCallState();
            userName = authProfile.name;
            userAvatar = authProfile.avatar || '';
            isCreator = true;
            videoEnabled = false;
            audioEnabled = true;

            const stream = await getMedia();
            if (!stream) return null;
            localStream = stream;
            setupAudioDetection(stream);
            applyMicOutgoingChain();

            roomId = fixedRoomId || generateRoomId();
            roomIsPrivate = privateRoom;
            currentGroupCallChatId = groupChatId;
            currentGroupCallTitle = String(options.groupTitle || '').trim();

            connectWS({
                type: 'create',
                roomId,
                userName,
                userAvatar,
                appUserId: authProfile?.appUserId || appUserId,
                privateRoom,
                friendCallMode,
                friendTargetAppUserId: friendCallTargetId,
                groupChatId,
                groupCallAllowedUserIds,
                reconnectKey: getReconnectKey()
            });
            history.replaceState(null, '', `${getBasePath().replace(/\/$/, '')}/${roomId}`);

            renderCallScreen();
            startCallTimer();
            if (!silent) {
                showNotification('Комната создана', 'Ссылка готова для отправки', 'success');
            }
            return roomId;
        }

        async function joinRoom(id, options = {}) {
            if (!authProfile) {
                pendingRoomJoin = id;
                renderAuthScreen();
                return;
            }
            primeCallAudioSession();
            resetCallState();
            userName = authProfile.name;
            userAvatar = authProfile.avatar || '';
            isCreator = false;
            videoEnabled = false;
            audioEnabled = true;

            const stream = await getMedia();
            if (!stream) return;
            localStream = stream;
            setupAudioDetection(stream);
            applyMicOutgoingChain();

            roomId = id;
            currentGroupCallChatId = String(options.groupChatId || '').trim();
            currentGroupCallTitle = String(options.groupTitle || '').trim();
            history.replaceState(null, '', `${getBasePath().replace(/\/$/, '')}/${roomId}`);

            connectWS({
                type: 'join',
                roomId,
                userName,
                userAvatar,
                appUserId: authProfile?.appUserId || appUserId,
                groupChatId: currentGroupCallChatId,
                reconnectKey: getReconnectKey()
            });

            renderCallScreen();
            startCallTimer();
            showNotification('Подключение', 'Поиск комнаты...', 'info');
        }

        function connectWS(initialMsg, isReconnect = false) {
            wsLastInitialMsg = initialMsg ? { ...initialMsg } : wsLastInitialMsg;
            const connectSession = wsConnectSessionId;
            if (wsHeartbeatTimer) {
                clearInterval(wsHeartbeatTimer);
                wsHeartbeatTimer = null;
            }
            const newType = String((initialMsg || wsLastInitialMsg || {}).type || '');
            // Разрешаем переподключение если тип изменился (мессенджер <-> звонок)
            const typeChanged = newType && newType !== currentWsType;
            // Не подключаем если уже подключен и тип не изменился
            if (!typeChanged && ws && ws.readyState === WebSocket.OPEN) {
                return;
            }
            // Закрываем старое соединение только если тип изменился или соединение не открыто
            if (typeChanged && ws && ws.readyState === WebSocket.OPEN) {
                try {
                    ws.__closingByUser = true;
                    ws.close();
                } catch (_) {}
            }
            const payload = {
                ...(initialMsg || wsLastInitialMsg || {}),
                reconnectKey: getReconnectKey()
            };
            const candidates = resolveWsUrls();
            const tryConnectCandidate = (index) => {
                if (connectSession !== wsConnectSessionId) return;
                if (index >= candidates.length) {
                    isConnected = false;
                    if (!canReconnectWsSession()) return;
                    updateMessengerSidebarStatus();
                    scheduleWsReconnect();
                    return;
                }
                const socket = new WebSocket(candidates[index]);
                ws = socket;
                let opened = false;
                const connectTimeout = setTimeout(() => {
                    if (connectSession !== wsConnectSessionId) return;
                    if (!opened) {
                        console.error('[WS] Connection timeout for:', candidates[index]);
                        try { socket.close(); } catch(_) {}
                        tryConnectCandidate(index + 1);
                    }
                }, 8000);
                socket.onopen = () => {
                    if (connectSession !== wsConnectSessionId) {
                        try { socket.close(); } catch (_) {}
                        return;
                    }
                    clearTimeout(connectTimeout);
                    console.log('[WS] Connected to:', candidates[index]);
                    opened = true;
                    wsLastActivityAt = Date.now();
                    wsLastPongAt = wsLastActivityAt;
                    wsLastPingAt = 0;
                    currentWsType = String((payload || {}).type || '');
                    if (wsReconnectTimer) {
                        clearTimeout(wsReconnectTimer);
                        wsReconnectTimer = null;
                    }
                    wsReconnectAttempts = 0;
                    socket.send(JSON.stringify(payload));
                    socket.onmessage = handleMessage;
                    socket.onerror = (e) => {
                    console.error('[WS] Error connecting to:', candidates[index], e);
                };
                    syncMessengerIdentity();
                    flushPendingMessengerEvents();
                    if (authProfile?.appUserId) {
                        registerCurrentDeviceSession().then(() => {
                            const sid = getDeviceSessionId();
                            if (sid && socket === ws && socket.readyState === WebSocket.OPEN) {
                                try {
                                    socket.send(JSON.stringify({
                                        type: 'messenger-register',
                                        appUserId: authProfile.appUserId,
                                        deviceSessionId: sid,
                                        userName: authProfile.name || userName || '',
                                        userAvatar: authProfile.avatar || userAvatar || ''
                                    }));
                                } catch (_) {}
                            }
                        });
                    }
                    if (wsHeartbeatTimer) {
                        clearInterval(wsHeartbeatTimer);
                    }
                    wsHeartbeatTimer = setInterval(() => {
                        if (socket !== ws) return;
                        if (socket.readyState === WebSocket.OPEN) {
                            const now = Date.now();
                            const lastSignalAt = Math.max(wsLastActivityAt || 0, wsLastPongAt || 0, 0);
                            const pingTimedOut = wsLastPingAt > 0 && wsLastPongAt < wsLastPingAt && (now - wsLastPingAt) > 18000;
                            const idleTooLong = lastSignalAt > 0 && (now - lastSignalAt) > (document.hidden ? 90000 : 45000);
                            if (pingTimedOut || idleTooLong) {
                                performWsReconnect(pingTimedOut ? 'pong-timeout' : 'idle-timeout');
                                return;
                            }
                            wsLastPingAt = now;
                            safeWsSend({ type: 'ping', ts: now });
                        }
                    }, 10000);
                    updateMessengerSidebarStatus();
                };
                socket.onclose = (ev) => {
                    if (connectSession !== wsConnectSessionId) return;
                    clearTimeout(connectTimeout);
                    console.log('[WS] Closed:', candidates[index], 'code:', ev.code, 'reason:', ev.reason);
                    currentWsType = '';
                    if (socket.__closingByUser) {
                        socket.__closingByUser = false;
                        return;
                    }
                    if (!opened) {
                        tryConnectCandidate(index + 1);
                        return;
                    }
                    isConnected = false;
                    wsLastPingAt = 0;
                    if (wsHeartbeatTimer) {
                        clearInterval(wsHeartbeatTimer);
                        wsHeartbeatTimer = null;
                    }
                    if (!canReconnectWsSession()) {
                        if (!roomId && authProfile) {
                            ensureMessengerWsConnection();
                        }
                        return;
                    }
                    updateMessengerSidebarStatus();
                    scheduleWsReconnect();
                };
            };
            tryConnectCandidate(0);
        }

        function createPeer(stream, type, isInitiator, targetId = null, label = null, connId = null) {
            const bitrateKbps = type === 'screen' ? 1500 : 1200;
            const opts = {
                initiator: isInitiator,
                trickle: true,
                sdpTransform: (sdp) => improveVideoSdpQuality(sdp, bitrateKbps),
                config: {
                    iceServers: rtcIceServers.length ? rtcIceServers : DEFAULT_ICE_SERVERS,
                    iceCandidatePoolSize: 8,
                    sdpSemantics: 'unified-plan',
                    bundlePolicy: 'max-bundle',
                    rtcpMuxPolicy: 'require'
                }
            };
            if (stream) {
                opts.stream = stream;
            }
            const peer = new SimplePeer(opts);
            
            peer.on('signal', s => {
                const sigType = type === 'video' ? 'signal' : `${type}-signal`;
                safeWsSend({
                    type: sigType,
                    signal: s,
                    target: targetId,
                    peerType: type,
                    connId: connId
                });
            });
            
            peer.on('stream', stream => {
                if (type === 'video') {
                    if (targetId && stream) {
                        remoteMediaStreams.set(targetId, stream);
                    }
                    const hasVideo = stream.getVideoTracks().length > 0;
                    if (hasVideo) {
                        addVideoTile(targetId || 'remote', label || remoteName, stream);
                    }
                    syncRemoteAudioPlayback(targetId);
                } else if (type === 'screen') {
                    const hasVideo = stream && stream.getVideoTracks && stream.getVideoTracks().length > 0;
                    if (hasVideo) {
                        addScreenTile(targetId || 'remote-screen', label || remoteName, stream);
                    }
                }
            });
            
            peer.on('track', (track, stream) => {
                if (type === 'video') {
                    if (targetId && stream) {
                        remoteMediaStreams.set(targetId, stream);
                    }
                    const hasVideo = stream.getVideoTracks().length > 0;
                    if (hasVideo) {
                        addVideoTile(targetId || 'remote', label || remoteName, stream);
                    }
                    syncRemoteAudioPlayback(targetId);
                } else if (type === 'screen') {
                    try {
                        const hasVideo = stream && stream.getVideoTracks && stream.getVideoTracks().length > 0;
                        if (hasVideo) {
                            addScreenTile(targetId || 'remote-screen', label || remoteName, stream);
                        } else if (track && track.kind === 'video') {
                            addScreenTile(targetId || 'remote-screen', label || remoteName, new MediaStream([track]));
                        }
                        const audioTracks = stream && stream.getAudioTracks ? stream.getAudioTracks() : [];
                        const liveAudio = (audioTracks || []).filter((t) => t && t.readyState === 'live');
                        if (targetId && liveAudio.length) {
                            playRemoteAudio(`screen:${targetId}`, new MediaStream(liveAudio));
                        }
                    } catch (_) {}
                }
            });
            
            peer.on('error', (err) => {});
            peer.on('connect', () => {
                if (targetId && type === 'video') {
                    const timerId = avPeerRecoverTimers.get(targetId);
                    if (timerId) {
                        clearTimeout(timerId);
                        avPeerRecoverTimers.delete(targetId);
                    }
                }
                if (targetId) {
                    syncRemoteAudioPlayback(targetId);
                }
            });
            if (type === 'video' && targetId && peer._pc) {
                const pc = peer._pc;
                const getIceKey = () => `ice:${String(targetId)}`;
                const scheduleRecover = () => {
                    const ice = String(pc.iceConnectionState || '').toLowerCase();
                    const conn = String(pc.connectionState || '').toLowerCase();
                    const hardFail = ice === 'failed' || conn === 'failed' || ice === 'closed' || conn === 'closed';
                    const softDisc = ice === 'disconnected';
                    if (!hardFail && !softDisc) {
                        const existing = avPeerRecoverTimers.get(targetId);
                        if (existing) {
                            clearTimeout(existing);
                            avPeerRecoverTimers.delete(targetId);
                        }
                        const iceT = iceRestartTimers.get(getIceKey());
                        if (iceT) {
                            clearTimeout(iceT);
                            iceRestartTimers.delete(getIceKey());
                        }
                        return;
                    }

                    // На disconnected: пробуем ICE restart (лёгкое восстановление) вместо уничтожения peer.
                    if (softDisc) {
                        if (!iceRestartTimers.has(getIceKey())) {
                            const t = setTimeout(() => {
                                iceRestartTimers.delete(getIceKey());
                                const ice2 = String(pc.iceConnectionState || '').toLowerCase();
                                if (ice2 !== 'disconnected') return;
                                try {
                                    if (typeof pc.restartIce === 'function') {
                                        pc.restartIce();
                                    }
                                } catch (_) {}
                            }, 5000);
                            iceRestartTimers.set(getIceKey(), t);
                        }
                        // Если после попытки всё ещё плохо — тогда уже пересоздаём peer (но не мгновенно).
                        if (!avPeerRecoverTimers.has(targetId)) {
                            const timerId = setTimeout(() => {
                                avPeerRecoverTimers.delete(targetId);
                                const ice2 = String(pc.iceConnectionState || '').toLowerCase();
                                const conn2 = String(pc.connectionState || '').toLowerCase();
                                if (ice2 === 'connected' || ice2 === 'completed') return;
                                if (conn2 === 'connected') return;
                                if (ice2 === 'disconnected') {
                                    recreateAvPeerForParticipant(targetId);
                                }
                            }, 20000);
                            avPeerRecoverTimers.set(targetId, timerId);
                        }
                        return;
                    }

                    // На failed/closed — пересоздаём peer (как и раньше).
                    if (hardFail && !avPeerRecoverTimers.has(targetId)) {
                        const timerId = setTimeout(() => {
                            avPeerRecoverTimers.delete(targetId);
                            const ice2 = String(pc.iceConnectionState || '').toLowerCase();
                            const conn2 = String(pc.connectionState || '').toLowerCase();
                            if (ice2 === 'connected' || ice2 === 'completed') return;
                            if (conn2 === 'connected') return;
                            recreateAvPeerForParticipant(targetId);
                        }, 2500);
                        avPeerRecoverTimers.set(targetId, timerId);
                    }
                };
                pc.addEventListener('iceconnectionstatechange', scheduleRecover);
                pc.addEventListener('connectionstatechange', scheduleRecover);
            }
            
            return peer;
        }

        function recreateAvPeerForParticipant(participantId) {
            if (!participantId || participantId === myId) return;
            ensureAvPeerForParticipant(participantId, shouldInitiatePeer(myId, participantId));
        }

        function ensureVideoTrackForAllPeers(track) {
            if (!track || !localStream) return;
            getRemoteParticipantIds().forEach((participantId) => {
                const avKey = `av-${participantId}`;
                let peer = peers.get(avKey);
                if (!peer || peer.destroyed) {
                    recreateAvPeerForParticipant(participantId);
                    peer = peers.get(avKey);
                }
                if (!peer || peer.destroyed || typeof peer.addTrack !== 'function') return;
                try {
                    peer.addTrack(track, localStream);
                } catch (error) {
                    const text = String(error?.message || '');
                    if (/already|exist|added/i.test(text)) {
                        return;
                    }
                    recreateAvPeerForParticipant(participantId);
                }
            });
        }

        async function validateScreenCaptureDimensions(track) {
            if (!track) return false;
            const readDims = () => {
                try {
                    const s = track.getSettings ? track.getSettings() : {};
                    return { w: Number(s.width || 0), h: Number(s.height || 0) };
                } catch (_) {
                    return { w: 0, h: 0 };
                }
            };
            let { w, h } = readDims();
            if (w > 0 && h > 0) return true;
            await new Promise((r) => setTimeout(r, 450));
            ({ w, h } = readDims());
            return w > 0 && h > 0;
        }

        function buildDisplayMediaConstraintAttempts() {
            const videoAdvanced = {
                surfaceSwitching: 'include',
                monitorTypeSurfaces: 'include',
                logicalSurface: true
            };
            const videoBase = {
                cursor: 'always',
                displaySurface: 'monitor',
                frameRate: { ideal: 30, max: 30 },
                width: { ideal: 1920, max: 2560 },
                height: { ideal: 1080, max: 1440 }
            };
            const attempts = [
                {
                    video: { ...videoBase, ...videoAdvanced },
                    audio: { echoCancellation: true, noiseSuppression: false }
                },
                {
                    video: {
                        ...videoBase,
                        width: { ideal: 1280, max: 1920 },
                        height: { ideal: 720, max: 1080 }
                    },
                    audio: true
                },
                { video: true, audio: true }
            ];
            try {
                const ua = navigator.userAgent || '';
                if (/Chrome/i.test(ua) && !/Edg\//i.test(ua)) {
                    attempts.push({
                        video: {
                            preferCurrentTab: true,
                            frameRate: { ideal: 30, max: 30 }
                        },
                        audio: false
                    });
                }
            } catch (_) {}
            return attempts;
        }

        async function startScreenShare() {
            if (isScreenSharing) {
                stopScreenShare();
                return;
            }
            const isMobileDevice = /Android|webOS|iPhone|iPad|iPod|Windows Phone|Mobile/i.test(navigator.userAgent || '');
            if (isMobileDevice) {
                showNotification('Демонстрация', 'Демонстрация экрана поддерживается только на ПК', 'warning');
                return;
            }
            let stream = null;
            let lastError = null;
            const displayMediaRequest = navigator.mediaDevices?.getDisplayMedia
                ? (constraints) => navigator.mediaDevices.getDisplayMedia(constraints)
                : typeof navigator.getDisplayMedia === 'function'
                    ? (constraints) => navigator.getDisplayMedia(constraints)
                    : null;
            try {
                if (displayMediaRequest) {
                    const attempts = buildDisplayMediaConstraintAttempts();
                    for (const constraints of attempts) {
                        try {
                            stream = await displayMediaRequest(constraints);
                            const track = stream?.getVideoTracks?.()[0] || null;
                            if (track && await validateScreenCaptureDimensions(track)) {
                                break;
                            }
                            try {
                                stream?.getTracks?.().forEach((t) => t.stop());
                            } catch (_) {}
                            stream = null;
                        } catch (error) {
                            lastError = error;
                            if (error?.name === 'NotAllowedError') {
                                throw error;
                            }
                        }
                    }
                }
                if (!stream || !stream.getVideoTracks || !stream.getVideoTracks().length) {
                    throw lastError || new Error('Screen stream unavailable');
                }
                const screenTrack = stream.getVideoTracks()[0];
                if (screenTrack && !(await validateScreenCaptureDimensions(screenTrack))) {
                    try {
                        stream.getTracks().forEach((t) => t.stop());
                    } catch (_) {}
                    showNotification(
                        'Демонстрация',
                        'Чёрный экран у других: в панели NVIDIA укажите браузеру «Высокопроизводительный процессор» (дискретная видеокарта) и перезапустите Chrome.',
                        'warning',
                        '<i class="fas fa-desktop"></i>'
                    );
                    return;
                }
                if (screenTrack) {
                    screenTrack.contentHint = 'detail';
                    try {
                        await screenTrack.applyConstraints({
                            frameRate: { ideal: 30, max: 30 },
                            width: { ideal: 1920, max: 2560 },
                            height: { ideal: 1080, max: 1440 }
                        });
                    } catch (_) {}
                }
                isScreenSharing = true;
                screenStreamLocal = stream;
                localScreenShareId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
                addScreenTile('self-screen', userName, stream);
                ensureScreenSharePeersForParticipants();
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'start-screen', from: userName }));
                } else {
                    showNotification('Демонстрация', 'Связь нестабильна, поток синхронизируется после переподключения', 'warning');
                }
                
                stream.getVideoTracks()[0].onended = () => stopScreenShare();
                updateUI();
                showNotification('Демонстрация', 'Демонстрация началась', 'success');
            } catch (error) {
                if (error.name === 'NotAllowedError') {
                    showNotification('Демонстрация', 'Вы отменили выбор экрана', 'warning');
                } else if (error.name === 'NotFoundError') {
                    showNotification('Демонстрация', 'Нет доступных экранов для демонстрации', 'warning');
                } else if (!displayMediaRequest) {
                    showNotification('Демонстрация', 'Браузер не поддерживает демонстрацию экрана', 'warning');
                } else {
                    showNotification('Демонстрация', 'Не удалось начать демонстрацию экрана', 'error');
                }
            }
        }

        function stopScreenShare() {
            Array.from(peers.keys()).forEach(key => {
                if (key.startsWith('screen-local-')) {
                    peers.get(key).destroy();
                    peers.delete(key);
                }
            });
            Array.from(screenConnMap.entries())
                .filter(([, v]) => typeof v === 'string' && v.startsWith('screen-local-'))
                .forEach(([k]) => screenConnMap.delete(k));
            
            isScreenSharing = false;
            try {
                if (screenStreamLocal) {
                    screenStreamLocal.getTracks().forEach(t => {
                        try { t.stop(); } catch (_) {}
                    });
                }
            } catch (_) {}
            screenStreamLocal = null;
            localScreenShareId = null;
            removeScreenTile('self-screen');
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'stop-screen', from: userName }));
            }
            updateUI();
            showNotification('Демонстрация', 'Демонстрация завершена', 'info');
        }

        async function toggleVideo() {
            if (!localStream || cameraSwitchInProgress) return;
            const next = !videoEnabled;
            
            if (next) {
                try {
                    cameraFacingMode = 'user';
                    if (!videoTrack || videoTrack.readyState !== 'live') {
                        const cameraTracks = await createCameraTracks(cameraFacingMode);
                        const track = cameraTracks?.outgoingTrack || null;
                        if (!track) {
                            throw new Error('No video track');
                        }
                        videoTrack = track;
                        cameraSourceTrack = cameraTracks?.sourceTrack || null;
                        selfPreviewTrack = cameraTracks?.previewTrack || null;
                        outgoingTrackCleanup = cameraTracks?.cleanup || null;
                    }
                    if (videoTrack && videoTrack.readyState === 'live') {
                        attachVideoTrack(videoTrack);
                    } else {
                        throw new Error('No video track');
                    }

                    videoEnabled = true;
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'toggle-video', enabled: true }));
                    }
                    syncCameraFacingMode();
                    updateUI();
                    showNotification('Камера', 'Камера включена', 'success', '<i class="fas fa-video"></i>');
                } catch (err) {
                    detachCurrentVideoTrack();
                    videoEnabled = false;
                    removeVideoTile('self');
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'toggle-video', enabled: false }));
                    }
                    updateUI();
                    showNotification('Ошибка', 'Не удалось включить камеру', 'error');
                }
            } else {
                detachCurrentVideoTrack();
                cameraFacingMode = 'user';

                videoEnabled = false;
                removeVideoTile('self');
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'toggle-video', enabled: false }));
                }
                updateUI();
                    showNotification('Камера', 'Камера выключена', 'info', '<i class="fas fa-video-slash"></i>');
            }
        }

        async function switchCameraFacingMode() {
            if (!localStream || !videoEnabled || !videoTrack || videoTrack.readyState !== 'live' || cameraSwitchInProgress) return;
            const previousFacingMode = cameraFacingMode;
            const nextFacingMode = cameraFacingMode === 'user' ? 'environment' : 'user';
            cameraSwitchInProgress = true;
            updateUI();
            videoEnabled = false;
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'toggle-video', enabled: false }));
            }
            detachCurrentVideoTrack();
            removeVideoTile('self');
            updateUI();
            try {
                const cameraTracks = await createCameraTracks(nextFacingMode);
                const newTrack = cameraTracks?.outgoingTrack || null;
                if (!newTrack) {
                    throw new Error('No video track');
                }
                videoTrack = newTrack;
                cameraSourceTrack = cameraTracks?.sourceTrack || null;
                selfPreviewTrack = cameraTracks?.previewTrack || null;
                outgoingTrackCleanup = cameraTracks?.cleanup || null;
                attachVideoTrack(newTrack);
                cameraFacingMode = normalizeFacingMode(cameraTracks?.facingMode, nextFacingMode);
                videoEnabled = true;
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'toggle-video', enabled: true }));
                }
                syncCameraFacingMode();
                showNotification('Камера', cameraFacingMode === 'environment' ? 'Переключено на заднюю камеру' : 'Переключено на переднюю камеру', 'success', '<i class="fas fa-sync-alt"></i>');
            } catch (error) {
                try {
                    const fallbackCameraTracks = await createCameraTracks(previousFacingMode);
                    const fallbackTrack = fallbackCameraTracks?.outgoingTrack || null;
                    if (!fallbackTrack) throw new Error('No fallback video track');
                    videoTrack = fallbackTrack;
                    cameraSourceTrack = fallbackCameraTracks?.sourceTrack || null;
                    selfPreviewTrack = fallbackCameraTracks?.previewTrack || null;
                    outgoingTrackCleanup = fallbackCameraTracks?.cleanup || null;
                    attachVideoTrack(fallbackTrack);
                    cameraFacingMode = normalizeFacingMode(fallbackCameraTracks?.facingMode, previousFacingMode);
                    videoEnabled = true;
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'toggle-video', enabled: true }));
                    }
                    syncCameraFacingMode();
                    showNotification('Ошибка', 'Не удалось переключить камеру, восстановлен прошлый режим', 'error');
                } catch (_) {
                    videoTrack = null;
                    videoEnabled = false;
                    cameraFacingMode = 'user';
                    removeVideoTile('self');
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'toggle-video', enabled: false }));
                    }
                    showNotification('Ошибка', 'Не удалось переключить камеру', 'error');
                }
            } finally {
                cameraSwitchInProgress = false;
                updateUI();
            }
        }

        function toggleAudio() {
            primeCallAudioSession();
            audioEnabled = !audioEnabled;
            localStream.getAudioTracks().forEach(t => t.enabled = audioEnabled);
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'toggle-audio', enabled: audioEnabled }));
            }
            showNotification('Микрофон', audioEnabled ? 'Микрофон включен' : 'Микрофон выключен', 'info', audioEnabled ? '<i class="fas fa-microphone"></i>' : '<i class="fas fa-microphone-slash"></i>');
            updateUI();
        }

        async function refreshAudioDevices() {
            try {
                if (!navigator.mediaDevices || typeof navigator.mediaDevices.enumerateDevices !== 'function') {
                    return { mics: [], speakers: [] };
                }
                const all = await navigator.mediaDevices.enumerateDevices();
                const mics = (all || []).filter((d) => d && d.kind === 'audioinput');
                const speakers = (all || []).filter((d) => d && d.kind === 'audiooutput');
                return { mics, speakers };
            } catch (_) {
                return { mics: [], speakers: [] };
            }
        }

        async function reconfigureAudioInput({ silent = true } = {}) {
            if (!localStream) return false;
            try {
                applyMicOutgoingChain();
                const s = await acquireMicMediaStream();
                const newTrack = s.getAudioTracks()[0] || null;
                if (!newTrack) return false;
                const oldTrack = rawMicTrack || (localStream.getAudioTracks()[0] || null);
                rawMicTrack = newTrack;
                try {
                    localStream.getAudioTracks().forEach((t) => {
                        try { localStream.removeTrack(t); } catch (_) {}
                    });
                    localStream.addTrack(rawMicTrack);
                } catch (_) {}
                if (oldTrack && oldTrack !== rawMicTrack) {
                    replaceAudioTrackForAllPeers(oldTrack, rawMicTrack);
                    try { oldTrack.stop(); } catch (_) {}
                }
                applyMicOutgoingChain();
                if (!silent) showNotification('Устройства', 'Микрофон переключен', 'success');
                return true;
            } catch (_) {
                if (!silent) showNotification('Устройства', 'Не удалось переключить микрофон', 'warning');
                return false;
            }
        }

        async function switchMicDevice(deviceId) {
            selectedMicDeviceId = String(deviceId || '');
            await reconfigureAudioInput({ silent: false });
        }

        function switchSpeakerDevice(deviceId) {
            selectedSpeakerDeviceId = String(deviceId || '');
            applySpeakerDeviceToAllAudio();
        }

        function showCallSettingsModal() {
            const prev = document.getElementById('callSettingsModal');
            if (prev) prev.remove();
            const modal = document.createElement('div');
            modal.className = 'modal call-settings-backdrop';
            modal.id = 'callSettingsModal';
            modal.onclick = (e) => {
                if (e.target === modal) modal.remove();
            };
            modal.innerHTML = `
                <div class="modal-content call-settings-modal">
                    <h2><i class="fas fa-sliders-h"></i> Настройки звонка</h2>
                    <div class="cs-grid">
                        <div>
                            <div class="cs-title">Микрофон</div>
                            <div class="cs-help">Источник входящего звука.</div>
                            <div id="csMicDd"></div>
                        </div>
                        <div>
                            <div class="cs-title">Динамики</div>
                            <div class="cs-help">Устройство вывода голоса собеседников.</div>
                            <div id="csSpkDd"></div>
                        </div>
                    </div>

                    <div class="modal-buttons" style="margin-top:16px">
                        <button class="modal-btn cancel" onclick="document.getElementById('callSettingsModal')?.remove()">Закрыть</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);

            (async () => {
                const { mics, speakers } = await refreshAudioDevices();
                const buildDd = ({ mountId, items, value, onSelect, disabledLabel = '' }) => {
                    const mount = document.getElementById(mountId);
                    if (!mount) return;
                    const safeItems = Array.isArray(items) ? items : [];
                    const cur = String(value || (safeItems[0] && safeItems[0].deviceId) || '');
                    const formatDevLabel = (raw) => {
                        let s = String(raw || '').trim();
                        // Часто label = "Microphone (NAME) (1234:abcd)" или "Speakers (NAME) (1234:abcd)" — это слишком длинно.
                        // Убираем хвост вида "(3142:006c)" и лишние пробелы.
                        s = s.replace(/\s*\([0-9a-fA-F]{4}:[0-9a-fA-F]{4}\)\s*$/g, '');
                        s = s.replace(/\s+/g, ' ').trim();
                        return s || 'Устройство';
                    };
                    const labelOf = (id) => {
                        const it = safeItems.find((x) => String(x.deviceId) === String(id));
                        return (it && formatDevLabel(it.label || '')) || '(по умолчанию)';
                    };
                    mount.innerHTML = `
                        <div class="cs-dd" id="${mountId}__dd">
                            <button type="button" class="cs-dd-btn" id="${mountId}__btn">
                                <span class="cs-dd-label">${escapeHtml(disabledLabel || labelOf(cur))}</span>
                                <span class="cs-dd-caret"><i class="fas fa-chevron-down"></i></span>
                            </button>
                            <div class="cs-dd-menu" id="${mountId}__menu"></div>
                        </div>
                    `;
                    const dd = document.getElementById(`${mountId}__dd`);
                    const btn = document.getElementById(`${mountId}__btn`);
                    const menu = document.getElementById(`${mountId}__menu`);
                    if (!dd || !btn || !menu) return;
                    if (disabledLabel) {
                        try { btn.disabled = true; btn.style.opacity = '0.7'; btn.style.cursor = 'not-allowed'; } catch (_) {}
                        return;
                    }
                    menu.innerHTML = safeItems.length
                        ? safeItems.map((d) => {
                            const did = String(d.deviceId || '');
                            const lab = formatDevLabel(String(d.label || 'Устройство'));
                            const active = did && did === cur ? ' active' : '';
                            return `<div class="cs-dd-item${active}" data-id="${durakEscapeDataAttr(did)}"><span class="cs-dd-item-label">${escapeHtml(lab)}</span>${active ? '<span>✓</span>' : ''}</div>`;
                        }).join('')
                        : `<div class="cs-dd-item active" data-id=""><span class="cs-dd-item-label">(по умолчанию)</span><span>✓</span></div>`;
                    const close = () => { try { dd.classList.remove('open'); } catch (_) {} };
                    btn.onclick = (e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        dd.classList.toggle('open');
                    };
                    menu.onclick = (e) => {
                        const item = e.target && e.target.closest ? e.target.closest('.cs-dd-item') : null;
                        if (!item) return;
                        const id = String(item.getAttribute('data-id') || '');
                        try { btn.querySelector('.cs-dd-label').textContent = labelOf(id); } catch (_) {}
                        close();
                        try { onSelect(id); } catch (_) {}
                    };
                    setTimeout(() => document.addEventListener('click', close, { once: true }), 0);
                };

                // Микрофоны
                buildDd({
                    mountId: 'csMicDd',
                    items: mics,
                    value: selectedMicDeviceId || (mics[0] && mics[0].deviceId) || '',
                    onSelect: (id) => switchMicDevice(id)
                });

                // Динамики (setSinkId)
                const supported = typeof HTMLMediaElement !== 'undefined' && HTMLMediaElement.prototype && typeof HTMLMediaElement.prototype.setSinkId === 'function';
                if (!supported) {
                    buildDd({
                        mountId: 'csSpkDd',
                        items: [],
                        value: '',
                        onSelect: () => {},
                        disabledLabel: '(не поддерживается браузером)'
                    });
                } else {
                    buildDd({
                        mountId: 'csSpkDd',
                        items: speakers,
                        value: selectedSpeakerDeviceId || (speakers[0] && speakers[0].deviceId) || '',
                        onSelect: (id) => {
                            switchSpeakerDevice(id);
                            showNotification('Устройства', 'Динамики переключены', 'success');
                        }
                    });
                }
            })();
        }

        function forceToggleRemoteVideo() {
            const targetId = currentContextTargetId;
            if (!targetId || !ws || ws.readyState !== WebSocket.OPEN) return;
            const state = getParticipantState(targetId);
            if (state.video) {
                ws.send(JSON.stringify({ type: 'force-video-off', targetId, from: userName, enabled: false }));
            } else {
                ws.send(JSON.stringify({ type: 'request-video', targetId, from: userName }));
                showNotification('Запрос отправлен', 'Пользователю отправлен запрос', 'info');
            }
            remoteVideo = !!state.video;
        }

        function forceToggleRemoteAudio() {
            const targetId = currentContextTargetId;
            if (!targetId || !ws || ws.readyState !== WebSocket.OPEN) return;
            const state = getParticipantState(targetId);
            if (state.audio) {
                ws.send(JSON.stringify({ type: 'force-audio-off', targetId, from: userName, enabled: false }));
                showNotification('Микрофон выключен', 'Вы выключили микрофон участнику', 'info');
            } else {
                ws.send(JSON.stringify({ type: 'request-audio', targetId, from: userName }));
                showNotification('Запрос отправлен', 'Пользователю отправлен запрос', 'info');
            }
            remoteAudio = !!state.audio;
        }

        function toggleAdmin() {
            const targetId = currentContextTargetId;
            if (!targetId || !ws || ws.readyState !== WebSocket.OPEN) return;
            const state = getParticipantState(targetId);
            if (window.remoteIsAdmin) {
                ws.send(JSON.stringify({ type: 'remove-admin', targetId, from: userName }));
                showNotification('Права', 'Снимаем администратора', 'info');
            } else {
                ws.send(JSON.stringify({ type: 'make-admin', targetId, from: userName }));
                showNotification('Права', `Назначаем админа: ${state.userName || 'участник'}`, 'info');
            }
        }

        function toggleParticipantsPanel() {
            const panel = document.querySelector('.participants-panel');
            if (panel) {
                panel.classList.toggle('open');
            }
        }

        function closeParticipantsPanel() {
            const panel = document.querySelector('.participants-panel');
            if (panel) {
                panel.classList.remove('open');
            }
        }

        function isMobileLayout() {
            return window.innerWidth <= 768;
        }

        function updateParticipantsResponsiveUI() {
            const closeBtn = document.getElementById('participantsCloseBtn');
            const panel = document.querySelector('.participants-panel');
            const isCompact = isMobileLayout();
            if (closeBtn) {
                closeBtn.style.display = isCompact ? 'inline-flex' : 'none';
            }
            if (!isCompact && panel) {
                panel.classList.remove('open');
            }
        }

        function canManageRoom() {
            return isCreator || isGuestAdmin;
        }

        function closeJoinPendingModal() {
            if (!joinPendingModal) return;
            try { joinPendingModal.remove(); } catch (_) {}
            joinPendingModal = null;
        }

        function showJoinPendingModal() {
            if (joinPendingModal) return;
            const modal = document.createElement('div');
            modal.className = 'request-modal';
            modal.innerHTML = `
                <div class="request-content">
                    <div style="font-size: 42px;"><i class="fas fa-user-lock"></i></div>
                    <h3>Эта комната приватная</h3>
                    <p>Ждем пока администратор впустит вас</p>
                    <div class="request-buttons">
                        <button class="request-btn cancel" id="privateJoinCancelBtn">Отмена</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            joinPendingModal = modal;
            const cancelBtn = modal.querySelector('#privateJoinCancelBtn');
            if (cancelBtn) {
                cancelBtn.onclick = () => {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'cancel-join-request' }));
                    }
                    closeJoinPendingModal();
                    endCall();
                };
            }
        }

        function setRoomPrivacy(enabled) {
            if (!canManageRoom() || !ws || ws.readyState !== WebSocket.OPEN) return;
            ws.send(JSON.stringify({ type: 'set-room-private', enabled: !!enabled }));
        }

        function toggleRoomPrivacy() {
            setRoomPrivacy(!roomIsPrivate);
            if (roomSettingsMenu) {
                try { roomSettingsMenu.remove(); } catch (_) {}
                roomSettingsMenu = null;
            }
        }

        function closeRoomForEveryone() {
            if (!canManageRoom() || !ws || ws.readyState !== WebSocket.OPEN) return;
            showCustomConfirm('Закрыть комнату', 'Завершить комнату для всех участников?', () => {
                ws.send(JSON.stringify({ type: 'close-room' }));
            });
            if (roomSettingsMenu) {
                try { roomSettingsMenu.remove(); } catch (_) {}
                roomSettingsMenu = null;
            }
        }

        function showRoomSettingsMenu(event) {
            event.preventDefault();
            event.stopPropagation();
            if (!canManageRoom()) return;
            if (roomSettingsMenu) {
                try { roomSettingsMenu.remove(); } catch (_) {}
                roomSettingsMenu = null;
            }
            const menu = document.createElement('div');
            menu.className = 'context-menu';
            const rect = event.currentTarget.getBoundingClientRect();
            menu.innerHTML = `
                <div class="context-item" onclick="toggleRoomPrivacy()">
                    <i class="fas ${roomIsPrivate ? 'fa-toggle-on' : 'fa-toggle-off'}"></i> Приватная комната: ${roomIsPrivate ? 'Вкл' : 'Выкл'}
                </div>
                <div class="divider"></div>
                <div class="context-item" onclick="closeRoomForEveryone()">
                    <i class="fas fa-door-closed"></i> Закрыть комнату
                </div>
            `;
            document.body.appendChild(menu);
            placeContextMenu(menu, rect.right - menu.offsetWidth, rect.bottom + 8, rect.top - 8);
            roomSettingsMenu = menu;
            const removeMenu = () => {
                if (!roomSettingsMenu) return;
                try { roomSettingsMenu.remove(); } catch (_) {}
                roomSettingsMenu = null;
            };
            setTimeout(() => document.addEventListener('click', removeMenu, { once: true }), 0);
        }

        function approveJoinRequest(requestId) {
            if (!requestId || !canManageRoom() || !ws || ws.readyState !== WebSocket.OPEN) return;
            ws.send(JSON.stringify({ type: 'approve-join-request', requestId }));
            pendingJoinRequests = pendingJoinRequests.filter((item) => item.id !== requestId);
            updateUI();
        }

        function rejectJoinRequest(requestId) {
            if (!requestId || !canManageRoom() || !ws || ws.readyState !== WebSocket.OPEN) return;
            ws.send(JSON.stringify({ type: 'reject-join-request', requestId }));
            pendingJoinRequests = pendingJoinRequests.filter((item) => item.id !== requestId);
            updateUI();
        }

        function handleMessage(e) {
            const data = JSON.parse(e.data);
            markWsActivity(data?.type === 'pong' ? 'pong' : 'message');
            applyIceServersFromPayload(data);
            const fromId = data.fromId;
            const fromName = data.from;

            switch (data.type) {
                case 'device-session-kicked':
                    handleDeviceSessionKicked('revoked');
                    break;
                case 'device-session-invalidated':
                    if (String(data.deviceSessionId || '').trim() === getDeviceSessionId()) {
                        handleDeviceSessionKicked('revoked');
                    }
                    break;
                case 'messenger-sync':
                    loadMessengerNotifications();
                    if (data.selfProfile && typeof data.selfProfile === 'object') {
                        const nextName = typeof data.selfProfile.name === 'string'
                            ? String(data.selfProfile.name || '').trim()
                            : '';
                        const nextAvatar = typeof data.selfProfile.avatar === 'string'
                            ? proxifyAvatarUrl(data.selfProfile.avatar || '')
                            : '';
                        const nextCoverUrl = typeof data.selfProfile.coverUrl === 'string'
                            ? proxifyAvatarUrl(data.selfProfile.coverUrl || '')
                            : '';
                        const syncPrivacy = data.selfProfile.privacy && typeof data.selfProfile.privacy === 'object'
                            ? data.selfProfile.privacy
                            : {};
                        const nextAppearance = data.selfProfile.appearance && typeof data.selfProfile.appearance === 'object'
                            ? data.selfProfile.appearance
                            : null;
                        if (nextAppearance) {
                            const nextTheme = String(nextAppearance.theme || '').trim() === 'dark' ? 'dark' : 'classic';
                            const nextWallpaper = typeof nextAppearance.chatWallpaper === 'string' ? String(nextAppearance.chatWallpaper || '').trim() : '';
                            const nextBlur = nextAppearance.chatWallpaperBlur !== undefined ? !!nextAppearance.chatWallpaperBlur : true;
                            messengerAppearance = {
                                ...messengerAppearance,
                                theme: nextTheme,
                                chatWallpaper: nextWallpaper,
                                chatWallpaperBlur: nextBlur
                            };
                            applyMessengerTheme();
                        }
                        messengerProfile = {
                            ...messengerProfile,
                            username: typeof data.selfProfile.username === 'string'
                                ? ensureGeneratedMessengerUsername(data.selfProfile.username || '', authProfile?.appUserId || appUserId)
                                : messengerProfile.username,
                            statusText: typeof data.selfProfile.statusText === 'string'
                                ? String(data.selfProfile.statusText || '').trim()
                                : messengerProfile.statusText,
                            privacy: {
                                canWrite: ['all', 'friends', 'nobody'].includes(syncPrivacy.canWrite) ? syncPrivacy.canWrite : (messengerProfile.privacy?.canWrite || 'all'),
                                canCall: ['all', 'friends', 'nobody'].includes(syncPrivacy.canCall) ? syncPrivacy.canCall : (messengerProfile.privacy?.canCall || 'all'),
                                canViewProfile: ['all', 'friends', 'nobody'].includes(syncPrivacy.canViewProfile) ? syncPrivacy.canViewProfile : (messengerProfile.privacy?.canViewProfile || 'all'),
                                canSeeStories: ['all', 'friends', 'nobody'].includes(syncPrivacy.canSeeStories) ? syncPrivacy.canSeeStories : (messengerProfile.privacy?.canSeeStories || 'friends'),
                                canJoinGroups: ['all', 'friends', 'nobody'].includes(syncPrivacy.canJoinGroups) ? syncPrivacy.canJoinGroups : (messengerProfile.privacy?.canJoinGroups || 'friends')
                            },
                            blacklist: Array.isArray(data.selfProfile.blacklist)
                                ? data.selfProfile.blacklist.map((v) => String(v || '').trim()).filter(Boolean)
                                : (Array.isArray(messengerProfile.blacklist) ? messengerProfile.blacklist : [])
                        };
                        persistMessengerProfileLocal();
                        if (authProfile?.appUserId) {
                            const prevName = String(authProfile.name || '').trim();
                            const prevAvatar = proxifyAvatarUrl(authProfile.avatar || '');
                            const mergedProfile = {
                                ...authProfile,
                                name: nextName || prevName || authProfile.appUserId,
                                avatar: nextAvatar || prevAvatar || '',
                                coverUrl: nextCoverUrl || authProfile.coverUrl || ''
                            };
                            saveProfile(mergedProfile);
                            if ((nextName && nextName !== prevName) || (nextAvatar && nextAvatar !== prevAvatar)) {
                                registerFriendsAccount().catch(() => {});
                            }
                        }
                    }
                    // JSON API работает всегда, нет ошибок хранения
                    (friendsState.friends || []).forEach((f) => {
                        if (!f?.id) return;
                        applyMessengerPeerHint(
                            f.id,
                            f.displayName || f.name || '',
                            f.avatar || '',
                            f.initials || ''
                        );
                    });
                    messengerChats = mergeMessengerChatsWithHints(Array.isArray(data.chats) ? data.chats : []);
                    hydrateMessengerHintsFromChats(messengerChats);
                    if (Array.isArray(data.chats) && data.chats.length) {
                        clearMessengerAutoResync();
                    } else {
                        const reason = String(data.reason || '').trim();
                        const storageErr = String(data.messengerStorageError || '').trim();
                        if (storageErr === 'storage_unavailable' || reason === 'init' || reason === 'storage-ready') {
                            scheduleMessengerAutoResync(storageErr || reason);
                        }
                    }
                    {
                        let savedChat = '';
                        let savedPeer = '';
                        try {
                            savedChat = String(sessionStorage.getItem(MESSENGER_SESSION_CHAT_KEY) || '').trim();
                            savedPeer = String(sessionStorage.getItem(MESSENGER_SESSION_PEER_KEY) || '').trim();
                        } catch (_) {}
                        if (!messengerActiveChatId && !messengerActivePeerId && !savedPeer && !savedChat && messengerChats.length) {
                            messengerActiveChatId = messengerChats[0].id || '';
                            messengerActivePeerId = messengerChats[0].kind === 'group' ? '' : (messengerChats[0].peer?.id || '');
                        }
                    }
                    if (messengerActiveChatId && !messengerChats.some((c) => c.id === messengerActiveChatId)) {
                        // Для прямых чатов (dm:...) не сбрасываем выбор, так как они могут быть восстановлены
                        const isDirectChat = String(messengerActiveChatId || '').startsWith('dm:');
                        if (!isDirectChat) {
                            messengerActiveChatId = '';
                            messengerActivePeerId = '';
                            persistMessengerSessionChat('');
                            persistMessengerSessionPeer('');
                        }
                    }
                    if (messengerActiveChatId && getMessengerSocketReady()) {
                        const openPayload = { type: 'messenger-open-chat', chatId: messengerActiveChatId };
                        if (String(messengerActiveChatId || '').startsWith('dm:')) {
                            const peerId = String(messengerActivePeerId || '').trim()
                                || parsePeerIdFromDirectChatId(messengerActiveChatId, authProfile?.appUserId || '');
                            if (peerId) openPayload.withUserId = peerId;
                        }
                        sendMessengerEvent(openPayload);
                    }
                    if (shouldRenderMessengerUi()) renderMainScreen();
                    break;
                case 'messenger-presence':
                    if (data.userId == null) break;
                    applyMessengerPresencePatch(data.userId, data.online, data.lastSeenAt);
                    if (shouldRenderMessengerUi() && !shouldDeferTransientMessengerRender()) renderMainScreen();
                    break;
                case 'messenger-compose-status':
                    {
                        if (!data.chatId) break;
                        // Применяем только к активному открытому чату, чтобы не было «переброса» интерфейса.
                        if (messengerActiveChatId !== data.chatId) break;
                        if (data.withUserId && String(messengerActivePeerId || '') !== String(data.withUserId || '')) break;
                        messengerComposeBlocked = !!data.composeBlocked;
                        messengerComposeHint = String(data.composeHint || '');
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-profile-patch':
                    {
                        const tid = data.targetUserId;
                        const prof = data.profile;
                        if (!tid || !prof) break;
                        applyMessengerPeerHint(tid, prof.displayName, prof.avatar, prof.initials, prof.username, prof.statusText, prof.name);
                        messengerProfileOverrides.set(String(tid), {
                            displayName: prof.displayName,
                            name: prof.name || prof.displayName,
                            avatar: prof.avatar,
                            coverUrl: prof.coverUrl || '',
                            initials: prof.initials,
                            username: prof.username || '',
                            statusText: prof.statusText || ''
                        });
                        messengerChats = mergeMessengerChatsWithHints(messengerChats);
                        if (Array.isArray(friendsState.friends)) {
                            friendsState.friends = friendsState.friends.map((f) => {
                                if (String(f.id) !== String(tid)) return f;
                                return {
                                    ...f,
                                    name: prof.name || prof.displayName || f.name,
                                    displayName: prof.displayName || prof.name || f.displayName,
                                    avatar: prof.avatar || f.avatar,
                                    username: prof.username || f.username || '',
                                    statusText: prof.statusText || f.statusText || '',
                                    initials: prof.initials || f.initials
                                };
                            });
                        }
                        if (Array.isArray(friendsSearchResults)) {
                            friendsSearchResults = friendsSearchResults.map((f) => {
                                if (String(f.id) !== String(tid)) return f;
                                return {
                                    ...f,
                                    name: prof.name || prof.displayName || f.name,
                                    displayName: prof.displayName || prof.name || f.displayName,
                                    avatar: prof.avatar || f.avatar,
                                    username: prof.username || f.username || '',
                                    statusText: prof.statusText || f.statusText || '',
                                    initials: prof.initials || f.initials || ''
                                };
                            });
                        }
                        if (messengerViewedProfile?.profile && String(messengerViewedProfile.profile.id || '') === String(tid)) {
                            messengerViewedProfile = {
                                ...messengerViewedProfile,
                                profile: {
                                    ...messengerViewedProfile.profile,
                                    displayName: prof.displayName,
                                    name: prof.name || prof.displayName,
                                    avatar: prof.avatar,
                                    coverUrl: prof.coverUrl || '',
                                    username: prof.username,
                                    statusText: prof.statusText
                                }
                            };
                        }
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-username-status':
                    {
                        const input = document.getElementById('profileUsernameInput');
                        const statusEl = document.getElementById('profileUsernameStatus');
                        if (!input || !statusEl) break;
                        const current = String(input.value || '').replace(/^@+/, '').trim().toLowerCase();
                        const checked = String(data.username || '').trim().toLowerCase();
                        if (current !== checked) break;
                        const hasValue = !!checked;
                        const available = !!data.available;
                        profileUsernameLastChecked = checked;
                        profileUsernameLastAvailable = available || !hasValue;
                        statusEl.dataset.state = hasValue ? (available ? 'ok' : 'taken') : 'idle';
                        statusEl.textContent = !hasValue ? 'Введите username' : (available ? 'Username свободен' : 'Username уже занят');
                    }
                    break;
                case 'messenger-username-resolved':
                    {
                        const uname = normalizeMentionUsername(data.username || '');
                        const pid = String(data.userId || '').trim();
                        const prof = data.profile && typeof data.profile === 'object' ? data.profile : null;
                        if (pid && prof) {
                            applyMessengerPeerHint(
                                pid,
                                prof.displayName || prof.name || '',
                                prof.avatar || '',
                                prof.initials || '',
                                prof.username || uname || ''
                            );
                            messengerChats = mergeMessengerChatsWithHints(messengerChats);
                        }
                        if (uname && pendingMentionProfileOpens.has(uname) && pid) {
                            pendingMentionProfileOpens.delete(uname);
                            openUserProfile(pid);
                        }
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-chat-history':
                    if (!data.chatId) break;
                    messengerActiveChatId = data.chatId;
                    messengerActivePeerId = data.withUserId || '';
                    persistMessengerSessionChat(data.chatId);
                    if (data.withUserId) persistMessengerSessionPeer(data.withUserId);
                    else persistMessengerSessionPeer('');
                    const msgs = Array.isArray(data.messages) ? data.messages : [];
                    {
                        if (data.chat && String(data.chat.kind || '') === 'group') {
                            const groupChat = buildGroupChatClientModel(data.chat);
                            if (groupChat) {
                                const idxChat = messengerChats.findIndex((item) => String(item?.id || '') === String(data.chatId || ''));
                                const existingChat = idxChat >= 0 ? messengerChats[idxChat] : null;
                                if (existingChat) {
                                    groupChat.lastMessage = existingChat.lastMessage;
                                    messengerChats[idxChat] = groupChat;
                                } else {
                                    messengerChats.unshift(groupChat);
                                }
                            }
                        }
                        // Важно: пришедшая история с сервера перетирает локальные флаги.
                        // Нам нужно сохранить delivered/read для исходящих сообщений,
                        // чтобы галочки не исчезали после messenger-sync.
                        const prevMsgs = messengerMessages.get(data.chatId) || [];
                        const prevById = new Map();
                        if (Array.isArray(prevMsgs)) {
                            prevMsgs.forEach((pm) => {
                                if (!pm || !pm.id) return;
                                prevById.set(String(pm.id), pm);
                            });
                        }
                        hydrateMessengerHintsFromMessages(msgs);
                        if (data.chat && String(data.chat.kind || '') === 'group') {
                            hydrateMessengerHintsFromChats([data.chat]);
                        }
                        const merged = msgs.map((m) => {
                            if (!m || !m.id) return m;
                            const pm = prevById.get(String(m.id));
                            if (!pm) return m;
                            const out = { ...m };
                            if (typeof pm.delivered === 'boolean') out.delivered = pm.delivered;
                            if (typeof pm.read === 'boolean') out.read = pm.read;
                            // Серверная история — это уже доставленные сообщения.
                            out.uploading = false;
                            delete out.uploadProgress;
                            return out;
                        });
                        messengerMessages.set(data.chatId, merged);
                        syncChatLastMessagePreviewFromMessages(data.chatId);
                        _hasRealMsgCache.delete(data.chatId);
                        messengerChats = mergeMessengerChatsWithHints(messengerChats);
                    }
                    messengerComposeBlocked = !!data.composeBlocked;
                    messengerComposeHint = String(data.composeHint || '');
                    // Когда пользователь открыл диалог — помечаем входящие сообщения как "прочитано",
                    // иначе read-гистограмма появляется только для новых сообщений, но не для уже загруженных.
                    try {
                        const myId = String(authProfile?.appUserId || '');
                        const shouldMarkRead = messengerView === 'chats' && String(messengerActiveChatId || '') === String(data.chatId || '');
                        if (shouldMarkRead && myId) {
                            msgs.forEach((m) => {
                                if (!m || !m.id) return;
                                if (String(m.fromId || '') === myId) return; // мои сообщения не помечаем
                                const mid = String(m.id);
                                if (messengerReadAckedMessageIds.has(mid)) return;
                                messengerReadAckedMessageIds.add(mid);
                                sendMessengerEvent({
                                    type: 'messenger-message-read',
                                    chatId: data.chatId,
                                    messageId: mid,
                                    senderId: String(m.fromId || '')
                                });
                            });
                        }
                    } catch (_) {}
                    if (shouldRenderMessengerUi()) renderMainScreen();
                    break;
                case 'messenger-message':
                    if (!data.chatId || !data.message) break;
                    {
                        const m = data.message;
                        const myId = String(authProfile?.appUserId || '');
                        const chatId = data.chatId;
                        const chatModel = findMessengerChatById(chatId) || resolveActiveMessengerChat();
                        const leftState = chatModel && String(chatModel.id || '') === String(chatId || '')
                            ? getGroupLeaveStateClient(chatModel, myId)
                            : null;
                        const frozenAt = Number(leftState?.frozenAt || leftState?.leftAt || 0);
                        if (chatModel && isGroupMessengerChat(chatModel) && frozenAt > 0 && Number(m.createdAt || 0) > frozenAt) {
                            break;
                        }
                        const isMine = String(m.fromId || '') === myId;
                        const isActiveChat = String(messengerActiveChatId || '') === String(chatId || '');
                        if (m.fromId) {
                            applyMessengerPeerHint(m.fromId, m.senderDisplayName, m.senderAvatar, m.senderInitials);
                            messengerChats = mergeMessengerChatsWithHints(messengerChats);
                        }
                        const prev = messengerMessages.get(data.chatId) || [];
                        const next = [...prev];
                        const idx = next.findIndex((item) => item && item.id && item.id === m.id);
                        const isMessageUpdate = idx >= 0;
                        if (idx >= 0) {
                            next[idx] = {
                                ...next[idx],
                                ...m,
                                uploading: false,
                                delivered: isMine ? true : next[idx]?.delivered || false,
                                read: isMine ? (next[idx]?.read || false) : next[idx]?.read
                            };
                        } else {
                            next.push({
                                ...m,
                                uploading: false,
                                delivered: isMine ? true : false
                            });
                        }
                        // Непрочитанные / read-ack для входящих сообщений.
                        if (!isMine && m.id) {
                            if (messengerView === 'chats' && isActiveChat) {
                                const key = String(m.id || '');
                                if (!messengerReadAckedMessageIds.has(key)) {
                                    messengerReadAckedMessageIds.add(key);
                                    sendMessengerEvent({
                                        type: 'messenger-message-read',
                                        chatId,
                                        messageId: String(m.id || ''),
                                        senderId: String(m.fromId || '')
                                    });
                                }
                            } else {
                                const mid = String(m.id || '');
                                if (!messengerUnreadMessageIds.has(mid)) {
                                    messengerUnreadMessageIds.add(mid);
                                    const prevCnt = getMessengerUnreadForChat(chatId);
                                    setMessengerUnreadForChat(chatId, prevCnt + 1);
                                    updateCallMinimizeUnreadBadge();
                                }
                            }
                        }

                        // ПОВЕДЕНИЕ СКРОЛЛА И КНОПКИ "ВНИЗ" В ЭТОМ ДИАЛОГЕ:
                        // 1) если ты отправил сам — всегда едем вниз
                        // 2) если пришло от собеседника — если ты не снизу, показываем кнопку с бейджем
                        //    (и не дёргаем скролл).
                        let nearBottom = true;
                        if (messengerView === 'chats' && isActiveChat) {
                            const dist = getChatHistoryDistFromBottom();
                            nearBottom = dist < 80;
                            if (isMessageUpdate) {
                                // Реакции/редактирование/служебные апдейты не должны самопроизвольно крутить историю.
                                messengerShouldAutoScroll = false;
                            } else if (isMine) {
                                messengerNewWhileScrolledCount = 0;
                                updateMessengerNewWhileScrolledFabUI();
                                messengerShouldAutoScroll = true;
                            } else if (!nearBottom) {
                                messengerNewWhileScrolledCount = Math.max(0, messengerNewWhileScrolledCount) + 1;
                                updateMessengerNewWhileScrolledFabUI();
                                messengerShouldAutoScroll = false;
                            } else {
                                messengerNewWhileScrolledCount = 0;
                                updateMessengerNewWhileScrolledFabUI();
                                messengerShouldAutoScroll = true;
                            }
                        }
                        if (!isMine && isGroupMessengerChat(findMessengerChatById(chatId)) && doesMessageMentionMe(m.text || '')) {
                            recordMessengerMention(chatId, m);
                            if (messengerView === 'chats' && isActiveChat) {
                                if (!nearBottom) {
                                    const key = String(chatId || '').trim();
                                    const prevMentions = messengerPendingMentionIdsByChat.get(key) || [];
                                    messengerPendingMentionIdsByChat.set(key, [...prevMentions, String(m.id || '')].filter(Boolean).slice(-99));
                                    messengerMentionWhileScrolledCount = Math.max(0, Number(messengerMentionWhileScrolledCount) || 0) + 1;
                                    updateMessengerMentionFabUI();
                                } else {
                                    const key = String(chatId || '').trim();
                                    messengerPendingMentionIdsByChat.delete(key);
                                    messengerMentionWhileScrolledCount = 0;
                                    updateMessengerMentionFabUI();
                                }
                            }
                        }
                        if (!isMine) {
                            const systemNotification = buildSystemNotificationFromMessage(chatId, m);
                            if (systemNotification) pushMessengerNotification(systemNotification);
                        }
                        if (isMine && m.id) {
                            for (let i = next.length - 1; i >= 0; i -= 1) {
                                const row = next[i];
                                if (!row || !row.uploading || String(row.fromId || '') !== myId) continue;
                                if (String(row.id || '') === String(m.id || '')) continue;
                                if (String(row.text || '') === String(m.text || '') && Math.abs(Number(row.createdAt || 0) - Number(m.createdAt || 0)) < 60000) {
                                    next.splice(i, 1);
                                }
                            }
                        }
                        messengerMessages.set(data.chatId, next.slice(-300));
                        syncChatLastMessagePreviewFromMessages(chatId);
                        if (
                            shouldRenderMessengerUi() &&
                            String(data.message.fromId || '') !== String(authProfile?.appUserId || '')
                        ) {
                            playIncomingMessengerSound();
                        }
                        if (shouldRenderMessengerUi()) {
                            if (shouldDeferTransientMessengerRender()) messengerRenderPendingAfterScroll = true;
                            else renderMainScreen();
                        }
                    }
                    break;
                case 'messenger-group-created':
                    if (data.chat?.id) {
                        upsertGroupChatModel(data.chat);
                        hydrateMessengerHintsFromChats([data.chat]);
                        if (!messengerActiveChatId || String(data.chat.createdBy || '') === String(authProfile?.appUserId || '')) {
                            messengerActiveChatId = data.chat.id;
                            messengerActivePeerId = '';
                            persistMessengerSessionChat(data.chat.id);
                            if (shouldRenderMessengerUi()) renderMainScreen();
                            sendMessengerEvent({ type: 'messenger-open-chat', chatId: data.chat.id });
                        }
                    }
                    break;
                case 'messenger-group-updated':
                    if (data.chat?.id) {
                        upsertGroupChatModel(data.chat);
                        hydrateMessengerHintsFromChats([data.chat]);
                        if (String(messengerActiveChatId || '') === String(data.chat.id || '')) {
                            const openChat = findMessengerChatById(data.chat.id);
                            if (openChat && isGroupMessengerChat(openChat)) {
                                messengerComposeBlocked = !!openChat.group?.restriction;
                                messengerComposeHint = getGroupRestrictionHintClient(openChat.group?.restriction);
                            }
                        }
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-group-call-created':
                case 'messenger-group-call-ended':
                    if (data.chatId) {
                        if (getMessengerSocketReady()) {
                            sendMessengerEvent({ type: 'messenger-open-chat', chatId: data.chatId });
                        }
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-group-call-ready':
                    if (!data.roomId || !data.chatId) break;
                    {
                        const activeChat = findMessengerChatById(data.chatId);
                        createRoom({
                            silent: true,
                            fixedRoomId: String(data.roomId || ''),
                            groupChatId: String(data.chatId || ''),
                            groupCallAllowedUserIds: Array.isArray(data.members) ? data.members : (activeChat?.group?.members || []),
                            groupTitle: activeChat?.peer?.displayName || activeChat?.peer?.name || 'Групповой звонок'
                        }).catch((error) => {
                            showNotification('Звонок', error?.message || 'Не удалось создать групповой звонок', 'error');
                        });
                    }
                    break;
                case 'messenger-group-left':
                    if (data.chatId) {
                        messengerChats = messengerChats.filter((item) => String(item?.id || '') !== String(data.chatId || ''));
                        messengerMessages.delete(data.chatId);
                        _hasRealMsgCache.delete(data.chatId);
                        if (String(messengerActiveChatId || '') === String(data.chatId || '')) {
                            messengerActiveChatId = '';
                            messengerActivePeerId = '';
                            persistMessengerSessionChat('');
                            persistMessengerSessionPeer('');
                        }
                    }
                    if (shouldRenderMessengerUi()) renderMainScreen();
                    break;
                case 'messenger-group-kicked':
                    // Исключение из чата — удаляем полностью
                    if (data.chatId) {
                        messengerChats = messengerChats.filter(c => c.id !== data.chatId);
                        messengerMessages.delete(data.chatId);
                        _hasRealMsgCache.delete(data.chatId);
                        if (String(messengerActiveChatId || '') === String(data.chatId || '')) {
                            messengerActiveChatId = '';
                            messengerActivePeerId = '';
                            persistMessengerSessionChat('');
                            persistMessengerSessionPeer('');
                        }
                    }
                    if (shouldRenderMessengerUi()) renderMainScreen();
                    break;
                case 'messenger-group-joined':
                    if (data.chat?.id) {
                        upsertGroupChatModel(data.chat);
                        hydrateMessengerHintsFromChats([data.chat]);
                        messengerActiveChatId = data.chat.id;
                        messengerActivePeerId = '';
                        persistMessengerSessionChat(data.chat.id);
                        persistMessengerSessionPeer('');
                        sendMessengerEvent({ type: 'messenger-open-chat', chatId: data.chat.id });
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-group-invite-preview':
                    if (data.chat?.id) {
                        openGroupInvitePreviewModal(data.chat, data.inviteCode || '', !!data.canJoin);
                    }
                    break;
                      case 'messenger-message-receipt':
                      {
                          if (!data.chatId || !data.messageId) break;
                          const chatId = data.chatId;
                          const msgId = String(data.messageId || '');
                          const receipt = String(data.receipt || '');
                          const prev = messengerMessages.get(chatId) || [];
                          const next = prev.map((it) => {
                              if (!it || String(it.id || '') !== msgId) return it;
                              const rb = Array.isArray(it.readBy) ? it.readBy.map(String) : [];
                              const add = String(data.readBy || '').trim();
                              const nextRb = add ? Array.from(new Set([...rb, add])) : rb;
                              if (receipt === 'read') {
                                  return { ...it, read: true, delivered: true, readBy: nextRb };
                              }
                              return { ...it };
                          });
                          messengerMessages.set(chatId, next);
                          // Direct DOM update for immediate visual feedback (bypasses scroll guard)
                          if (receipt === 'read') {
                              try {
                                  const safeId = messengerSafeId(msgId);
                                  const msgEl = document.getElementById('chatMsg-' + safeId);
                                  if (msgEl) {
                                      const checksEl = msgEl.querySelector('.chat-msg-checks');
                                      if (checksEl) {
                                          checksEl.innerHTML = '<i class="fas fa-check"></i><i class="fas fa-check"></i>';
                                          checksEl.classList.add('read');
                                      }
                                  }
                              } catch (_) {}
                          }
                          // Always re-render when receipt arrives for active chat
                          if (shouldRenderMessengerUi() || String(chatId) === String(messengerActiveChatId)) {
                              renderMainScreen();
                          }
                      }
                      break;
                case 'messenger-chat-deleted':
                    if (data.chatId) {
                        messengerMessages.delete(data.chatId);
                        if (String(data.scope || '') === 'all' && messengerActiveChatId === data.chatId) {
                            messengerActiveChatId = '';
                            messengerActivePeerId = '';
                            persistMessengerSessionPeer('');
                        }
                    }
                    if (shouldRenderMessengerUi()) renderMainScreen();
                    break;
                case 'messenger-message-updated':
                    if (!data.chatId || !data.message?.id) break;
                    {
                        const m = data.message;
                        if (m.fromId) {
                            applyMessengerPeerHint(m.fromId, m.senderDisplayName, m.senderAvatar, m.senderInitials);
                            messengerChats = mergeMessengerChatsWithHints(messengerChats);
                        }
                        const prev = messengerMessages.get(data.chatId) || [];
                        messengerMessages.set(data.chatId, prev.map((item) => item.id === m.id ? m : item));
                        syncChatLastMessagePreviewFromMessages(data.chatId);
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-message-deleted':
                    if (!data.chatId || !data.messageId) break;
                    {
                        const prev = messengerMessages.get(data.chatId) || [];
                        messengerMessages.set(
                            data.chatId,
                            prev.filter((item) => item.id !== data.messageId)
                        );
                        syncChatLastMessagePreviewFromMessages(data.chatId);
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-message-reactions':
                    if (!data.chatId || !data.messageId || !data.reactions) break;
                    {
                        const prev = messengerMessages.get(data.chatId) || [];
                        const prevMessage = prev.find((it) => it && String(it.id || '') === String(data.messageId || '')) || null;
                        const next = prev.map((it) => {
                            if (!it || String(it.id || '') !== String(data.messageId || '')) return it;
                            return { ...it, reactions: data.reactions };
                        });
                        if (prevMessage) {
                            recordMessengerReactionNotifications(data.chatId, data.messageId, prevMessage, data.reactions);
                        }
                        messengerMessages.set(data.chatId, next);
                        _hasRealMsgCache.delete(data.chatId);
                        if (shouldRenderMessengerUi()) renderMainScreen();
                    }
                    break;
                case 'messenger-typing':
                    if (!data.fromUserId) break;
                    {
                        const fromId = String(data.fromUserId || '').trim();
                        if (!fromId) break;
                        const activity = String(data.activity || '').trim() === 'voice' ? 'voice' : 'text';
                        const isTyping = !!data.isTyping;
                        const chatId = String(data.chatId || '').trim();
                        const withUserId = String(data.withUserId || '').trim();
                        if (chatId) {
                            const groupChat = findMessengerChatById(chatId);
                            if (getGroupLeaveStateClient(groupChat, authProfile?.appUserId || '')) break;
                        }
                        if (isTyping) {
                            messengerTypingByUser.set(fromId, {
                                isTyping: true,
                                activity,
                                ts: Date.now(),
                                chatId,
                                withUserId
                            });
                        }
                        else messengerTypingByUser.delete(fromId);
                        const prevTimer = messengerTypingTimersByUser.get(fromId);
                        if (prevTimer) clearTimeout(prevTimer);
                        if (isTyping) {
                            messengerTypingTimersByUser.set(
                                fromId,
                                setTimeout(() => {
                                    messengerTypingByUser.delete(fromId);
                                    messengerTypingTimersByUser.delete(fromId);
                                    if (shouldRenderMessengerUi() && !shouldDeferTransientMessengerRender()) renderMainScreen();
                                }, 2600)
                            );
                        } else {
                            messengerTypingTimersByUser.delete(fromId);
                        }
                    }
                    if (shouldRenderMessengerUi() && !shouldDeferTransientMessengerRender()) {
                        clearTimeout(messengerUiTypingTimer);
                        messengerUiTypingTimer = setTimeout(() => {
                            messengerUiTypingTimer = null;
                            const ae = document.activeElement;
                            if (messengerView === 'chats' && ae && ae.id === 'chatComposerInput') return;
                            renderMainScreen();
                        }, 240);
                    }
                    break;
                case 'messenger-error':
                    if (['save_failed', 'write_forbidden', 'group_edit_forbidden', 'group_settings_forbidden', 'invite_taken'].includes(String(data.code || ''))) {
                        // Убираем pending-сообщение, если оно было.
                        // Поскольку сервер может не отправить финальное сообщение при ошибке,
                        // проще всего зачистить все локальные uploading:true по отправителю.
                        try {
                            const chatId = messengerActiveChatId;
                            const myId = String(authProfile?.appUserId || '');
                            if (chatId && myId) {
                                const prev = messengerMessages.get(chatId) || [];
                                messengerMessages.set(
                                    chatId,
                                    prev.filter((m) => !(m && m.uploading && String(m.fromId || '') === myId))
                                );
                            }
                        } catch (_) {}
                    }
                    showNotification('Мессенджер', data.message || 'Ошибка мессенджера', 'warning');
                    break;
                case 'messenger-profile':
                    messengerViewedProfile = {
                        ...(data.view || {}),
                        targetUserId: data.targetUserId || (data.view && data.view.profile && data.view.profile.id) || ''
                    };
                    if (shouldRenderMessengerUi()) renderMainScreen();
                    break;
                case 'messenger-stories':
                    if (data.targetUserId && data.stories) {
                        const ownerId = String(data.targetUserId || '').trim();
                        stories.set(ownerId, data.stories);
                        const viewedProfileId = String(messengerViewedProfile?.targetUserId || messengerViewedProfile?.profile?.id || '').trim();
                        const shouldRefreshProfileView =
                            messengerView === 'profile' &&
                            (
                                ownerId === String(authProfile?.appUserId || '') ||
                                ownerId === viewedProfileId
                            );
                        if (shouldRenderMessengerUi()) {
                            if (shouldRefreshProfileView) renderMainScreen();
                            else renderStories();
                        }
                    }
                    break;
                case 'messenger-story-uploaded':
                    showNotification('', 'История опубликована', 'success');
                    loadStories(); // Reload stories
                    break;
                case 'messenger-story-deleted':
                    showNotification('', 'История удалена', 'info');
                    loadStories(); // Reload stories
                    break;
                case 'messenger-story-privacy-updated':
                    showNotification('', 'Приватность публикации сохранена', 'success');
                    loadStories();
                    break;
                case 'messenger-story-state-changed':
                    handleRemoteStoryStateChange(data.ownerUserId);
                    break;
                case 'messenger-story-like-result':
                    if (data.storyId && data.liked !== undefined) {
                        const likeBtn = document.getElementById('storyLikeBtn');
                        if (likeBtn) {
                            if (data.liked) {
                                likeBtn.classList.add('liked');
                            } else {
                                likeBtn.classList.remove('liked');
                            }
                        }
                    }
                    break;
                case 'messenger-story-comment-result':
                    if (data.storyId) {
                        const input = document.getElementById('storyReplyInput');
                        if (input) input.value = '';
                        showNotification('', 'Комментарий добавлен', 'success');
                    }
                    break;
                case 'messenger-story-like-status':
                    if (data.storyId && data.liked !== undefined) {
                        const likeBtn = document.getElementById('storyLikeBtn');
                        if (likeBtn) {
                            if (data.liked) {
                                likeBtn.classList.add('liked');
                            } else {
                                likeBtn.classList.remove('liked');
                            }
                        }
                    }
                    break;
                case 'messenger-story-views':
                    if (data.storyId && data.views) {
                        showStoryViewsModal(data.views);
                    }
                    break;
                case 'signal':
                case 'video-signal':
                    {
                        const peerKey = `av-${fromId}`;
                        let vPeer = peers.get(peerKey);
                        if (!vPeer) {
                            vPeer = createPeer(localStream, 'video', false, fromId, fromName);
                            peers.set(peerKey, vPeer);
                        }
                        if (vPeer && !vPeer.destroyed) vPeer.signal(data.signal);
                    }
                    break;

                case 'screen-signal':
                    {
                        const connId = data.connId || null;
                        let peerKey = connId ? screenConnMap.get(connId) : null;
                        let sPeer = peerKey ? peers.get(peerKey) : null;

                        if (!sPeer) {
                            if (data.signal && data.signal.type === 'offer') {
                                peerKey = connId ? `screen-remote-${fromId}-${connId}` : `screen-remote-${fromId}`;
                                sPeer = peers.get(peerKey);
                                if (!sPeer) {
                                    sPeer = createPeer(null, 'screen', false, fromId, fromName, connId);
                                    peers.set(peerKey, sPeer);
                                }
                                if (connId) screenConnMap.set(connId, peerKey);
                            } else {
                                const localPeerKey = `screen-local-${fromId}`;
                                sPeer = peers.get(localPeerKey);
                                if (!sPeer) return;
                                if (connId) screenConnMap.set(connId, localPeerKey);
                            }
                        }
                        if (sPeer && !sPeer.destroyed) sPeer.signal(data.signal);
                    }
                    break;

                case 'created':
                case 'joined':
                    isConnected = true;
                    markWsActivity('message');
                    myId = data.myId;
                    ownerId = data.ownerId || ownerId;
                    closeJoinPendingModal();
                    updateCreatorFlag();
                    const waitingMsg0 = document.getElementById('waitingMsg');
                    if (waitingMsg0) waitingMsg0.style.display = 'none';
                    syncLocalMediaStateToServer();
                    healRemoteAudioLinks();
                    setTimeout(healRemoteAudioLinks, 700);
                    setTimeout(healRemoteAudioLinks, 1800);
                    startCallAudioHealTimer();
                    startConnectionQualityMonitor();
                    primeCallAudioSession();
                    updateUI();
                    updateEmptyState();
                    break;

                case 'room-state':
                    {
                        isConnected = true;
                        markWsActivity('message');
                        myId = data.myId || myId;
                        ownerId = data.ownerId || ownerId;
                        participants.clear();
                        participantAvatars.clear();
                        participantStates.clear();
                        participantConnectionQuality.clear();
                        connectionNoticeCooldown.clear();
                        audioRecoverCooldown.clear();
                        const list = Array.isArray(data.participants) ? data.participants : [];
                        list.forEach((p) => upsertParticipantState(p));
                        const myState = participantStates.get(myId);
                        if (myState) {
                            isGuestAdmin = !!myState.isAdmin && String(ownerId ?? '') !== String(myId ?? '');
                        } else {
                            isGuestAdmin = false;
                        }
                        updateCreatorFlag();
                        updatePrimaryRemoteState();
                        watchPartyState = data.watchParty || null;
                        roomIsPrivate = !!data.isPrivate;
                        pendingJoinRequests = Array.isArray(data.pendingJoinRequests) ? data.pendingJoinRequests : [];
                        renderWatchPartyTile();
                        getRemoteParticipantIds().forEach((participantId) => {
                            ensureAvPeerForParticipant(participantId, shouldInitiatePeer(myId, participantId));
                            syncRemoteAudioPlayback(participantId);
                        });
                        syncLocalMediaStateToServer();
                        healRemoteAudioLinks();
                        setTimeout(healRemoteAudioLinks, 700);
                        setTimeout(healRemoteAudioLinks, 1800);
                        startCallAudioHealTimer();
                        startConnectionQualityMonitor();
                        primeCallAudioSession();
                        const waitingMsgState = document.getElementById('waitingMsg');
                        if (waitingMsgState) waitingMsgState.style.display = 'none';
                        updateUI();
                        updateEmptyState();
                    }
                    break;

                case 'watch-started':
                    watchPartyState = data.watchParty || null;
                    renderWatchPartyTile();
                    updateUI();
                    if (watchPartyState?.ownerName) {
                        showNotification('Совместный просмотр', `${watchPartyState.ownerName} запустил просмотр`, 'info');
                    }
                    break;

                case 'join-pending':
                    showJoinPendingModal();
                    showNotification('Приватная комната', 'Ожидаем подтверждения администратора', 'info');
                    break;

                case 'error':
                    {
                        const message = data.message || 'Ошибка подключения';
                        showNotification('Комната', message, 'warning');
                        if (roomId) {
                            endCall(false);
                        }
                    }
                    break;

                case 'join-request':
                    {
                        const request = data.request || null;
                        if (!request || !request.id) break;
                        const existingIdx = pendingJoinRequests.findIndex((item) => item.id === request.id);
                        if (existingIdx >= 0) {
                            pendingJoinRequests[existingIdx] = request;
                        } else {
                            pendingJoinRequests.push(request);
                        }
                        updateUI();
                        showNotification('Заявка', `${request.userName || 'Участник'} хочет войти`, 'info');
                    }
                    break;

                case 'join-request-cancelled':
                    {
                        const requestId = data.requestId || '';
                        if (!requestId) break;
                        pendingJoinRequests = pendingJoinRequests.filter((item) => item.id !== requestId);
                        updateUI();
                    }
                    break;

                case 'join-rejected':
                    closeJoinPendingModal();
                    showNotification('Приватная комната', 'Администратор отклонил запрос на вход', 'warning');
                    endCall(false);
                    break;

                case 'room-privacy-updated':
                    roomIsPrivate = !!data.enabled;
                    updateUI();
                    showNotification('Комната', roomIsPrivate ? 'Приватный режим включен' : 'Приватный режим выключен', 'info');
                    break;

                case 'watch-stopped':
                    watchPartyState = null;
                    removeWatchPartyTile();
                    updateUI();
                    showNotification('Совместный просмотр', 'Просмотр остановлен', 'info');
                    break;

                case 'durak-state': {
                    const prevDurak = durakGameState;
                    const wasDurakPlaying = prevDurak && prevDurak.phase === 'playing';
                    durakGameState = data.game;
                    // Sync card pack from server
                    if (data.game && data.game.cardPack) {
                        durakCardPack = data.game.cardPack;
                        updateDurakCardBackStyle();
                    }
                    if (data.game && data.game.phase === 'showdown' && wasDurakPlaying) {
                        durakNotifyGameEnded(data.game);
                    }
                    renderDurakUi();
                    break;
                }

                case 'durak-error':
                    showNotification('Дурак', data.message || 'Ошибка', 'warning');
                    break;

                case 'guest-joined':
                    const guest = data.guest || {
                        id: data.guestId,
                        userName: data.guestName,
                        userAvatar: data.guestAvatar || '',
                        video: data.guestVideo,
                        audio: data.guestAudio,
                        screen: false,
                        isAdmin: false,
                        appUserId: data.guestAppUserId || ''
                    };
                    if (outgoingFriendCallSession && !outgoingFriendCallSession.answered) {
                        const joinedAppUserId = String(guest.appUserId || '').trim();
                        if (!outgoingFriendCallSession.targetId || (joinedAppUserId && joinedAppUserId === outgoingFriendCallSession.targetId)) {
                            acceptOutgoingFriendCallSession();
                        }
                    }
                    if (data.ownerId) ownerId = data.ownerId;
                    upsertParticipantState(guest);
                    updateCreatorFlag();
                    updatePrimaryRemoteState();
                    isConnected = true;

                    const joinedId = guest.id;
                    if (joinedId && joinedId !== myId) {
                        const targetId = joinedId;
                        const state = getParticipantState(targetId);
                        ensureAvPeerForParticipant(targetId, shouldInitiatePeer(myId, targetId));
                        setTimeout(() => syncRemoteAudioPlayback(targetId), 350);
                        setTimeout(healRemoteAudioLinks, 700);
                        setTimeout(healRemoteAudioLinks, 1800);

                        if (isScreenSharing && screenStreamLocal) {
                            const screenKey = `screen-local-${targetId}`;
                            if (!peers.get(screenKey)) {
                                const connId = `${localScreenShareId}:${targetId}`;
                                const screenPeer = createPeer(screenStreamLocal, 'screen', true, targetId, state.userName || '', connId);
                                peers.set(screenKey, screenPeer);
                                screenConnMap.set(connId, screenKey);
                            }
                        }
                    }

                    const waitingMsg = document.getElementById('waitingMsg');
                    if (waitingMsg) waitingMsg.style.display = 'none';
                    updateUI();
                    updateEmptyState();
                    const shouldPlayJoinSound = !!joinedId && !!myId && joinedId !== myId;
                    if (shouldPlayJoinSound) {
                        playSoundEffect(joinSoundEffect);
                    }
                    showNotification('Участник подключился', `${data.guest?.userName || data.guestName || 'Участник'} присоединился`, 'success');
                    break;

                case 'creator-info':
                    {
                        myId = data.myId || myId;
                        const creator = {
                            id: data.creatorId,
                            userName: data.creatorName,
                            userAvatar: data.creatorAvatar || '',
                            video: !!data.creatorVideo,
                            audio: typeof data.creatorAudio === 'boolean' ? data.creatorAudio : true,
                            screen: false,
                            isAdmin: !!data.isAdmin,
                            appUserId: data.creatorAppUserId || ''
                        };
                        upsertParticipantState(creator);
                        if (creator.isAdmin) {
                            ownerId = creator.id;
                        }
                        isConnected = true;
                        updateCreatorFlag();
                        updatePrimaryRemoteState();
                        const avKeyCreator = `av-${creator.id}`;
                        if (!peers.get(avKeyCreator)) {
                            const avPeer = createPeer(localStream, 'video', shouldInitiatePeer(myId, creator.id), creator.id, creator.userName || '');
                            peers.set(avKeyCreator, avPeer);
                        }
                        const waitingMsg2 = document.getElementById('waitingMsg');
                        if (waitingMsg2) waitingMsg2.style.display = 'none';
                        updateUI();
                        updateEmptyState();
                    }
                    break;

                case 'screen-started':
                    if (fromId) {
                        const state = getParticipantState(fromId);
                        state.screen = true;
                    }
                    updatePrimaryRemoteState();
                    showNotification('Демонстрация', `${fromName} начал демонстрацию`, 'info');
                    updateUI();
                    break;

                case 'screen-stopped':
                    if (fromId) {
                        const state = getParticipantState(fromId);
                        state.screen = false;
                    }
                    updatePrimaryRemoteState();
                    removeScreenTile(fromId);
                    Array.from(peers.keys())
                        .filter(k => k.startsWith(`screen-remote-${fromId}`))
                        .forEach(k => {
                            const p = peers.get(k);
                            if (p) p.destroy();
                            peers.delete(k);
                        });
                    Array.from(screenConnMap.entries())
                        .filter(([, v]) => typeof v === 'string' && v.startsWith(`screen-remote-${fromId}`))
                        .forEach(([k]) => screenConnMap.delete(k));
                    showNotification('Демонстрация', `${fromName} завершил демонстрацию`, 'info');
                    updateUI();
                    break;

                case 'video-toggle':
                    if (fromId) {
                        const state = getParticipantState(fromId);
                        state.video = !!data.enabled;
                    }
                    if (!data.enabled) {
                        removeVideoTile(fromId);
                    }
                    if (fromId) {
                        applyVideoTileMirroring(fromId);
                    }
                    syncRemoteAudioPlayback(fromId);
                    updatePrimaryRemoteState();
                    updateUI();
                    break;

                case 'camera-facing':
                    if (fromId) {
                        const state = getParticipantState(fromId);
                        state.cameraFacingMode = normalizeFacingMode(data.mode, '');
                        applyVideoTileMirroring(fromId);
                    }
                    break;

                case 'audio-toggle':
                    if (fromId) {
                        const state = getParticipantState(fromId);
                        state.audio = !!data.enabled;
                    }
                    syncRemoteAudioPlayback(fromId);
                    updatePrimaryRemoteState();
                    updateUI();
                    showNotification('Статус', `${fromName} ${data.enabled ? 'включил' : 'выключил'} микрофон`, 'info', data.enabled ? '<i class="fas fa-microphone"></i>' : '<i class="fas fa-microphone-slash"></i>');
                    break;

                case 'speaking':
                    if (fromId) {
                        const state = getParticipantState(fromId);
                        state.speaking = !!data.isSpeaking;
                    }
                    updatePrimaryRemoteState();
                    updateUI();
                    break;

                case 'participant-updated':
                    {
                        const targetId = data.participantId;
                        if (!targetId) break;
                        const state = getParticipantState(targetId);
                        const changes = data.changes || {};
                        if (typeof changes.video === 'boolean') state.video = changes.video;
                        if (typeof changes.audio === 'boolean') state.audio = changes.audio;
                        if (typeof changes.screen === 'boolean') state.screen = changes.screen;
                        if (typeof changes.speaking === 'boolean') state.speaking = changes.speaking;
                        if (typeof changes.isAdmin === 'boolean') state.isAdmin = changes.isAdmin;
                        if (typeof changes.cameraFacingMode === 'string') state.cameraFacingMode = normalizeFacingMode(changes.cameraFacingMode, '');
                        if (changes.userName) state.userName = changes.userName;
                        if (typeof changes.userAvatar === 'string') state.userAvatar = changes.userAvatar;
                        if (typeof changes.appUserId === 'string') state.appUserId = changes.appUserId;
                        if (data.ownerId) ownerId = data.ownerId;
                        updateCreatorFlag();
                        const myState = participantStates.get(myId);
                    isGuestAdmin = !!myState?.isAdmin && String(ownerId ?? '') !== String(myId ?? '');
                        if (targetId && typeof changes.video === 'boolean' && !changes.video) {
                            removeVideoTile(targetId);
                        }
                        if (targetId && typeof changes.screen === 'boolean' && !changes.screen) {
                            removeScreenTile(targetId);
                            Array.from(peers.keys())
                                .filter(k => k.startsWith(`screen-remote-${targetId}`))
                                .forEach(k => {
                                    const p = peers.get(k);
                                    if (p) p.destroy();
                                    peers.delete(k);
                                });
                            Array.from(screenConnMap.entries())
                                .filter(([, v]) => typeof v === 'string' && v.startsWith(`screen-remote-${targetId}`))
                                .forEach(([k]) => screenConnMap.delete(k));
                        }
                        if (targetId) {
                            applyVideoTileMirroring(targetId);
                            syncRemoteAudioPlayback(targetId);
                        }
                        updatePrimaryRemoteState();
                        updateUI();
                    }
                    break;

                case 'force-video-off':
                    if (data.enabled !== videoEnabled && videoEnabled) {
                        showNotification('Действие', `${fromName} выключил вашу камеру`, 'warning');
                        toggleVideo();
                    }
                    break;

                case 'force-audio-off':
                    if (data.enabled !== audioEnabled && audioEnabled) {
                        showNotification('Действие', `${fromName} выключил ваш микрофон`, 'warning');
                        toggleAudio();
                    }
                    break;

                case 'request-video':
                    showCustomConfirm('📹 Запрос на включение камеры', `${fromName} просит включить камеру`, () => {
                        if (!videoEnabled) toggleVideo();
                    });
                    break;

                case 'request-audio':
                    showCustomConfirm('🎤 Запрос на включение микрофона', `${fromName} просит включить микрофон`, () => {
                        if (!audioEnabled) toggleAudio();
                    });
                    break;

                case 'friend-request':
                    {
                        const fromAccountId = String(data.fromAccountId || '').trim();
                        if (!fromAccountId) break;
                        showIncomingFriendRequestModal(fromAccountId, data.fromName || fromName || 'Пользователь');
                    }
                    break;

                case 'made-admin':
                case 'admin-removed':
                case 'admin-state':
                    break;

                case 'owner-changed':
                    ownerId = data.ownerId || ownerId;
                    const ownerState = participantStates.get(ownerId);
                    if (ownerState) ownerState.isAdmin = true;
                    updateCreatorFlag();
                    const myUpdatedState = participantStates.get(myId);
                    isGuestAdmin = !!myUpdatedState?.isAdmin && String(ownerId ?? '') !== String(myId ?? '');
                    updatePrimaryRemoteState();
                    updateUI();
                    break;

                case 'room-closed':
                    closeJoinPendingModal();
                    showNotification('Комната закрыта', data.byName ? `${data.byName} завершил комнату` : 'Комната была закрыта', 'warning');
                    endCall(false);
                    break;

                case 'guest-left':
                    removeVideoTile(fromId);
                    removeScreenTile(fromId);
                    removeParticipantState(fromId);
                    
                    const avPeerKey = `av-${fromId}`;
                    if (peers.get(avPeerKey)) {
                        peers.get(avPeerKey).destroy();
                        peers.delete(avPeerKey);
                    }
                    Array.from(peers.keys())
                        .filter(k => k.startsWith(`screen-remote-${fromId}`))
                        .forEach(k => {
                            const p = peers.get(k);
                            if (p) p.destroy();
                            peers.delete(k);
                        });
                    const screenLocalKeyLeft = `screen-local-${fromId}`;
                    if (peers.get(screenLocalKeyLeft)) {
                        peers.get(screenLocalKeyLeft).destroy();
                        peers.delete(screenLocalKeyLeft);
                    }
                    Array.from(screenConnMap.entries())
                        .filter(([, v]) => typeof v === 'string' && (v.startsWith(`screen-remote-${fromId}`) || v === screenLocalKeyLeft))
                        .forEach(([k]) => screenConnMap.delete(k));
                    if (data.ownerId) ownerId = data.ownerId;
                    updateCreatorFlag();
                    updatePrimaryRemoteState();
                    updateUI();
                    if (data.friendCallEnded) {
                        showNotification('Звонок другу', `${fromName || 'Участник'} завершил звонок`, 'warning');
                        endCall(false);
                        break;
                    }
                    showNotification('Участник покинул', `${fromName} вышел`, 'warning');
                    break;

                case 'kicked':
                    playSoundEffect(kickSoundEffect);
                    endCall(false);
                    showNotification('Исключение', 'Вас исключили из звонка', 'error');
                    break;
            }
        }

        function updateUI() {
            const videoBtn = document.getElementById('videoBtn');
            const flipCameraBtn = document.getElementById('flipCameraBtn');
            const audioBtn = document.getElementById('audioBtn');
            const screenBtn = document.getElementById('screenBtn');
            const watchBtn = document.getElementById('watchBtn');
            const stopWatchBtn = document.getElementById('stopWatchBtn');
            const roomSettingsBtn = document.getElementById('roomSettingsBtn');
            const copyInviteIcon = document.getElementById('copyInviteIcon');
            if (videoBtn) {
                videoBtn.innerHTML = videoEnabled ? '<i class="fas fa-video"></i>' : '<i class="fas fa-video-slash"></i>';
                videoBtn.className = `ctrl-btn ${videoEnabled ? 'active' : 'disabled'}`;
            }
            if (audioBtn) {
                audioBtn.innerHTML = audioEnabled ? '<i class="fas fa-microphone"></i>' : '<i class="fas fa-microphone-slash"></i>';
                audioBtn.className = `ctrl-btn ${audioEnabled ? 'active' : 'disabled'}`;
            }
            if (flipCameraBtn) {
                flipCameraBtn.style.display = videoEnabled ? 'inline-flex' : 'none';
                flipCameraBtn.disabled = !videoEnabled || cameraSwitchInProgress;
                if (cameraSwitchInProgress) {
                    flipCameraBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
                } else {
                    flipCameraBtn.innerHTML = '<i class="fas fa-sync-alt"></i>';
                }
                flipCameraBtn.className = `ctrl-btn flip ${videoEnabled ? 'active' : ''}`;
                flipCameraBtn.title = cameraFacingMode === 'environment' ? 'Переключить на переднюю камеру' : 'Переключить на заднюю камеру';
            }
            if (screenBtn) {
                screenBtn.innerHTML = isScreenSharing ? '<i class="fas fa-stop"></i>' : '<i class="fas fa-desktop"></i>';
                screenBtn.className = `ctrl-btn screen ${isScreenSharing ? 'active' : ''}`;
            }
            if (watchBtn) {
                watchBtn.innerHTML = '<i class="fas fa-users-viewfinder"></i>';
                watchBtn.className = `ctrl-btn watch ${watchPartyState ? 'active' : ''}`;
            }
            if (stopWatchBtn) {
                const showStop = !!watchPartyState && canStopWatchParty();
                stopWatchBtn.style.display = showStop ? '' : 'none';
            }
            if (roomSettingsBtn) {
                roomSettingsBtn.style.display = canManageRoom() ? 'inline-flex' : 'none';
            }
            applyCallConnectionBadges();
            if (copyInviteIcon) {
                copyInviteIcon.className = `fas ${shouldCopyTelegramInvite() ? 'fa-link' : 'fa-id-card'}`;
            }

            const list = document.getElementById('participantsList');
            if (list) {
                const myWaveHtml = `<span class="wave" style="display: ${isSpeaking ? 'inline-flex' : 'none'}"><span></span><span></span><span></span><span></span></span>`;
                const canControl = isCreator || isGuestAdmin;
                const canOpenContext = true;
                
                let participantsHtml = '';
                if (canControl && pendingJoinRequests.length) {
                    pendingJoinRequests.forEach((request) => {
                        participantsHtml += `
                            <div class="participant">
                                <div class="participant-info">
                                    <div class="participant-avatar">${avatarMarkup(request.userName, request.userAvatar)}</div>
                                    <div class="participant-name">
                                        <div class="participant-title">
                                            ${renderMaybeMarqueeText(request.userName || 'Участник', 100, 'participant-title-text')}
                                        </div>
                                        <div class="participant-badges">
                                            <span class="badge admin-badge"><i class="fas fa-user-clock"></i> Ожидает вход</span>
                                        </div>
                                    </div>
                                </div>
                                <div class="participant-status">
                                    <button class="ctrl-btn active" style="width:30px;height:30px;font-size:14px" onclick="approveJoinRequest('${request.id}')"><i class="fas fa-check"></i></button>
                                    <button class="ctrl-btn end" style="width:30px;height:30px;font-size:14px" onclick="rejectJoinRequest('${request.id}')"><i class="fas fa-times"></i></button>
                                </div>
                            </div>
                        `;
                    });
                }

                participantsHtml += `
                    <div class="participant ${isSpeaking ? 'speaking' : ''}">
                        <div class="participant-info">
                            <div class="participant-avatar">${avatarMarkup(userName, userAvatar)}</div>
                            <div class="participant-name">
                                <div class="participant-title">
                                    ${renderMaybeMarqueeText(userName || 'Вы', 100, 'participant-title-text')}
                                    ${myWaveHtml}
                                </div>
                                <div class="participant-badges">
                                    ${isCreator ? '<span class="badge icon-badge" title="Создатель"><i class="fas fa-crown"></i></span>' : ''}
                                    ${isGuestAdmin ? '<span class="badge icon-badge admin-badge" title="Админ"><i class="fas fa-user-shield"></i></span>' : ''}
                                    ${isScreenSharing ? '<span class="badge icon-badge screen-badge" title="Демонстрация"><i class="fas fa-desktop"></i></span>' : ''}
                                    ${watchPartyState && watchPartyState.ownerId === myId ? '<span class="badge icon-badge" title="Совместный просмотр"><i class="fas fa-users-viewfinder"></i></span>' : ''}
                                </div>
                            </div>
                        </div>
                        <div class="participant-status">
                            <i class="fas ${videoEnabled ? 'fa-video' : 'fa-video-slash'} ${!videoEnabled ? 'off' : ''}"></i>
                            <i class="fas ${audioEnabled ? 'fa-microphone' : 'fa-microphone-slash'} ${!audioEnabled ? 'off' : ''}"></i>
                        </div>
                    </div>
                `;

                getRemoteParticipantIds().forEach((participantId) => {
                    const state = getParticipantState(participantId);
                    const remoteWave = `<span class="wave" style="display: ${state.speaking ? 'inline-flex' : 'none'}"><span></span><span></span><span></span><span></span></span>`;
                    const qualityLevel = getConnectionQuality(participantId);
                    const qualityTitle = qualityLevel === 'reconnecting'
                        ? 'Переподключение…'
                        : (qualityLevel === 'weak' ? 'Плохая связь' : 'Качество связи');
                    const qualityBadge = `<span class="connection-badge ${qualityLevel}" title="${qualityTitle}"><i class="fas fa-signal"></i></span>`;
                    const isConnectingRemote = connectingAudioParticipants.has(String(participantId));
                    const connDots = isConnectingRemote
                        ? '<div class="p-conn-dots" title="Соединение"><span></span><span></span><span></span></div>'
                        : '';
                    const attrs = canOpenContext ? `data-target-id="${participantId}" oncontextmenu="showContextMenu(event,false,'${participantId}')" ontouchstart="handleParticipantTap(event,'${participantId}')" onclick="handleParticipantTap(event,'${participantId}')"` : '';
                    participantsHtml += `
                    <div class="participant ${state.speaking ? 'speaking' : ''} ${isConnectingRemote ? 'connecting' : ''}" ${attrs}>
                        <div class="participant-info">
                            <div class="participant-avatar">${avatarMarkup(state.userName, state.userAvatar)}${connDots}</div>
                            <div class="participant-name">
                                <div class="participant-title">
                                    ${renderMaybeMarqueeText(state.userName || 'Участник', 100, 'participant-title-text')}
                                    ${remoteWave}
                                </div>
                                <div class="participant-badges">
                                    ${participantId === ownerId ? '<span class="badge icon-badge" title="Создатель"><i class="fas fa-crown"></i></span>' : ''}
                                    ${state.screen ? '<span class="badge icon-badge screen-badge" title="Демонстрация"><i class="fas fa-desktop"></i></span>' : ''}
                                    ${state.isAdmin && participantId !== ownerId ? '<span class="badge icon-badge admin-badge" title="Админ"><i class="fas fa-user-shield"></i></span>' : ''}
                                    ${watchPartyState && watchPartyState.ownerId === participantId ? '<span class="badge icon-badge" title="Совместный просмотр"><i class="fas fa-users-viewfinder"></i></span>' : ''}
                                    ${qualityBadge}
                                </div>
                            </div>
                        </div>
                        <div class="participant-status">
                            <i class="fas ${state.video ? 'fa-video' : 'fa-video-slash'} ${!state.video ? 'off' : ''}"></i>
                            <i class="fas ${state.audio ? 'fa-microphone' : 'fa-microphone-slash'} ${!state.audio ? 'off' : ''}"></i>
                        </div>
                    </div>
                    `;
                });
                
                list.innerHTML = participantsHtml;
            }
        }

        function placeContextMenu(menu, preferredLeft, preferredTop, fallbackTop = null) {
            if (!menu) return;
            const margin = 12;
            const menuRect = menu.getBoundingClientRect();
            const maxLeft = window.innerWidth - menuRect.width - margin;
            const safeLeft = Math.min(Math.max(margin, preferredLeft), Math.max(margin, maxLeft));

            let top = preferredTop;
            if (top + menuRect.height > window.innerHeight - margin && fallbackTop !== null) {
                top = fallbackTop - menuRect.height;
            }
            const maxTop = window.innerHeight - menuRect.height - margin;
            const safeTop = Math.min(Math.max(margin, top), Math.max(margin, maxTop));

            menu.style.left = `${safeLeft}px`;
            menu.style.top = `${safeTop}px`;
        }

        function showContextMenu(e, fromTap = false, targetId = null) {
            if (!fromTap) {
                e.preventDefault();
            }
            const resolvedTargetId = targetId || currentContextTargetId || getRemoteParticipantIds()[0];
            if (!resolvedTargetId) return;
            currentContextTargetId = resolvedTargetId;
            const state = getParticipantState(resolvedTargetId);
            if (!state) return;
            remoteName = state.userName || remoteName;
            remoteAvatar = state.userAvatar || remoteAvatar;
            remoteVideo = !!state.video;
            remoteAudio = !!state.audio;
            remoteScreen = !!state.screen;
            remoteSpeaking = !!state.speaking;
            window.remoteIsAdmin = !!state.isAdmin;
            const pointX = fromTap ? (e.touches?.[0]?.pageX || e.pageX || window.innerWidth / 2) : e.pageX;
            const pointY = fromTap ? (e.touches?.[0]?.pageY || e.pageY || window.innerHeight / 2) : e.pageY;

            const menu = document.createElement('div');
            menu.className = 'context-menu';
            
            let html = '';
            
            if (isCreator || isGuestAdmin) {
                if (remoteAudio) {
                    html += `<div class="context-item" onclick="forceToggleRemoteAudio()">
                        <i class="fas fa-microphone-slash"></i> Выключить микрофон
                    </div>`;
                } else {
                    html += `<div class="context-item" onclick="forceToggleRemoteAudio()">
                        <i class="fas fa-microphone"></i> Попросить включить микрофон
                    </div>`;
                }
                
                if (remoteVideo) {
                    html += `<div class="context-item" onclick="forceToggleRemoteVideo()">
                        <i class="fas fa-video-slash"></i> Выключить камеру
                    </div>`;
                } else {
                    html += `<div class="context-item" onclick="forceToggleRemoteVideo()">
                        <i class="fas fa-video"></i> Попросить включить камеру
                    </div>`;
                }
            }
            
            if (isCreator) {
                if (html) html += `<div class="divider"></div>`;
                html += `<div class="context-item" onclick="toggleAdmin()">
                    <i class="fas fa-user-shield"></i> ${window.remoteIsAdmin ? 'Снять администратора' : 'Назначить администратором'}
                </div>`;
                html += `<div class="divider"></div>`;
                html += `<div class="context-item" onclick="kickUser()">
                    <i class="fas fa-user-slash"></i> Исключить из звонка
                </div>`;
            }

            if (html) html += `<div class="divider"></div>`;
            html += `<div class="context-item" onclick="requestFriendFromCall()">
                <i class="fas fa-user-plus"></i> Добавить в друзья
            </div>`;
            
            menu.innerHTML = html;
            document.body.appendChild(menu);
            placeContextMenu(menu, pointX, pointY);
            setTimeout(() => menu.remove(), 5000);
            document.addEventListener('click', () => menu.remove(), { once: true });
        }

        function handleParticipantTap(e) {
            if (window.innerWidth <= 768) {
                const targetId = e?.currentTarget?.dataset?.targetId || currentContextTargetId;
                showContextMenu(e, true, targetId);
            }
        }

        async function requestFriendFromCall() {
            const targetId = currentContextTargetId;
            if (!targetId) return;
            const state = getParticipantState(targetId);
            const targetName = state?.userName || 'Участник';
            const targetAccountId = String(state?.appUserId || '').trim();
            if (!targetAccountId) {
                showNotification('Друзья', 'У пользователя нет ID аккаунта', 'warning');
                return;
            }
            await sendFriendRequest(targetAccountId);
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'friend-request',
                    targetId,
                    fromAccountId: appUserId,
                    fromName: userName
                }));
            }
            showNotification('Друзья', `Запрос отправлен: ${targetName}`, 'info');
        }

        function kickUser() { 
            const targetId = currentContextTargetId;
            if (!targetId || !ws || ws.readyState !== WebSocket.OPEN) return;
            const state = getParticipantState(targetId);
            const targetName = state?.userName || remoteName || 'участника';
            showCustomConfirm('Исключить участника', `Исключить ${targetName}?`, () => {
                ws.send(JSON.stringify({ type: 'kick', targetId }));
            });
        }
        
        function copyRoomId() {
            const value = getRoomInviteToCopy();
            if (!value) return;
            navigator.clipboard.writeText(value);
            showNotification('Скопировано', shouldCopyTelegramInvite() ? 'Ссылка на комнату скопирована' : 'ID комнаты скопирован', 'success');
        }

        /** Спрайт 1408×768 → 11×4 (128×192). Ряды: пики, крести, черви, буби; рубашка — 11-й столбец, 4-й ряд. */
        function durakCardBgStyle(cardId) {
            let s = String(cardId || '').trim().toLowerCase();
            if (!s) return '';
            s = s.replace(/~\d+$/, '');
            if (s.length < 2) return '';
            const suit = s.slice(-1);
            const head = s.slice(0, -1);
            let rank;
            if (head === '10' || head === 't') rank = 'T';
            else if (head.length === 1) {
                const c = head;
                if (c === 't') rank = 'T';
                else if (/^[6-9]$/.test(c)) rank = c;
                else rank = c.toUpperCase();
            } else return '';
            const order = ['6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A'];
            const col = order.indexOf(rank);
            const suitToRow = { s: 0, c: 1, h: 2, d: 3 };
            const row = suitToRow[suit];
            if (col < 0 || row == null) return '';
            /* 11 колонок в спрайте: индексы карт 0…8 → x = 0%…80%, не 100% (иначе в кадр попадает соседняя клетка) */
            const x = (col * 100) / 10;
            const y = (row * 100) / 3;
            return `background-position:${x}% ${y}%`;
        }

        function durakSuitRu(trumpLetter) {
            const m = { s: 'пики', h: 'червы', d: 'бубны', c: 'крести' };
            return m[trumpLetter] || trumpLetter || '—';
        }

        function sendDurak(obj) {
            if (!ws || ws.readyState !== WebSocket.OPEN) return;
            ws.send(JSON.stringify(obj));
        }

        function applyDurakFocusMode(on) {
            const callScreen = document.querySelector('.call-screen');
            if (!callScreen) return;
            if (on) {
                callScreen.classList.add('durak-focus');
                callScreen.classList.remove('ui-idle');
                triggerWatchFocusActivity();
            } else {
                callScreen.classList.remove('durak-focus');
            }
        }

        function ensureDurakTopbarLine() {
            const tb = document.getElementById('callTopbar');
            if (!tb || document.getElementById('durakTopbarLine')) return;
            const s = document.createElement('span');
            s.id = 'durakTopbarLine';
            s.className = 'call-timer';
            const badge = tb.querySelector('#roomPrivacyBadge');
            if (badge) tb.insertBefore(s, badge);
            else tb.appendChild(s);
        }

        function onDurakToolbarClick() {
            if (durakGameState) {
                durakShowdownDismissed = false;
                renderDurakUi();
                return;
            }
            showDurakCardPackModal();
        }

        function showDurakCardPackModal() {
            // Remove existing modal if any
            const existingModal = document.getElementById('durakCardPackModal');
            if (existingModal) existingModal.remove();

            const modal = document.createElement('div');
            modal.id = 'durakCardPackModal';
            modal.className = 'modal-overlay';
            modal.style.cssText = `
                position: fixed;
                inset: 0;
                z-index: 10000;
                display: flex;
                align-items: center;
                justify-content: center;
                background: rgba(4, 6, 18, 0.85);
                backdrop-filter: blur(12px);
                animation: fadeIn 0.3s ease;
            `;

            const modalContent = document.createElement('div');
            modalContent.className = 'modal-content';
            modalContent.style.cssText = `
                background: linear-gradient(145deg, rgba(22, 18, 45, 0.95), rgba(12, 14, 28, 0.95));
                border: 1px solid rgba(255, 255, 255, 0.18);
                border-radius: 20px;
                padding: 32px 24px;
                max-width: 400px;
                width: 90%;
                text-align: center;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
                animation: slideUp 0.3s ease;
            `;

            modalContent.innerHTML = `
                <h3 style="color: white; font-size: 20px; font-weight: 700; margin-bottom: 12px;">Какими картами будем играть?</h3>
                <p style="color: rgba(255, 255, 255, 0.7); font-size: 14px; margin-bottom: 20px; line-height: 1.4;">Выберите дизайн карт для игры</p>
                
                <div style="display: flex; gap: 20px; justify-content: center; margin-bottom: 24px; position: relative;">
                    <!-- Divider Line -->
                    <div style="position: absolute; left: 50%; top: 5%; bottom: 5%; width: 1px; background: linear-gradient(180deg, transparent, rgba(147, 51, 234, 0.8), transparent); z-index: 4;"></div>
                    
                    <!-- Fantasy Preview -->
                    <div class="fantasy-preview" style="text-align: center; cursor: pointer; transition: transform 0.2s ease, box-shadow 0.2s ease;" onmouseover="this.style.transform='translateY(-2px)'; this.querySelector('.preview-container').style.boxShadow='0 8px 25px rgba(147, 51, 234, 0.4)';" onmouseout="this.style.transform='translateY(0)'; this.querySelector('.preview-container').style.boxShadow='0 4px 12px rgba(0,0,0,0.4)';">
                        <div class="preview-container" style="position: relative; width: 100px; height: 140px; margin: 0 auto 8px; transition: box-shadow 0.2s ease;">
                            <div style="position: absolute; inset: -2px; background: rgba(255, 255, 255, 0.1); border: 1px solid rgba(255, 255, 255, 0.2); border-radius: 12px; backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); z-index: 0; box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1), inset 0 1px 0 rgba(255, 255, 255, 0.2);"></div>
                             <img src="${resolveAssetUrl('assets/fantasy/spades_A.png')}" style="position: absolute; width: 60px; height: 90px; top: 0; left: 20px; border-radius: 4px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); z-index: 3; border: 1px solid rgba(255,255,255,0.1);" draggable="false" oncontextmenu="return false;" ondragstart="return false;">
                             <img src="${resolveAssetUrl('assets/fantasy/hearts_K.png')}" style="position: absolute; width: 60px; height: 90px; top: 25px; left: 10px; border-radius: 4px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); z-index: 2; border: 1px solid rgba(255,255,255,0.1);" draggable="false" oncontextmenu="return false;" ondragstart="return false;">
                             <img src="${resolveAssetUrl('assets/fantasy/clubs_10.png')}" style="position: absolute; width: 60px; height: 90px; top: 50px; left: 0; border-radius: 4px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); z-index: 1; border: 1px solid rgba(255,255,255,0.1);" draggable="false" oncontextmenu="return false;" ondragstart="return false;">
                        </div>
                        <div style="color: rgba(255, 255, 255, 0.8); font-size: 14px; font-weight: 600;">
                            <i class="fas fa-dragon" style="margin-right: 6px;"></i>Fantasy
                        </div>
                    </div>
                    
                    <!-- Classic Preview -->
                    <div class="classic-preview" style="text-align: center; cursor: pointer; transition: transform 0.2s ease, box-shadow 0.2s ease;" onmouseover="this.style.transform='translateY(-2px)'; this.querySelector('.preview-container').style.boxShadow='0 8px 25px rgba(34, 197, 94, 0.4)';" onmouseout="this.style.transform='translateY(0)'; this.querySelector('.preview-container').style.boxShadow='0 4px 12px rgba(0,0,0,0.4)';">
                        <div class="preview-container" style="position: relative; width: 100px; height: 140px; margin: 0 auto 8px; transition: box-shadow 0.2s ease;">
                            <div style="position: absolute; inset: -2px; background: rgba(255, 255, 255, 0.1); border: 1px solid rgba(255, 255, 255, 0.2); border-radius: 12px; backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); z-index: 0; box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1), inset 0 1px 0 rgba(255, 255, 255, 0.2);"></div>
                             <img src="${resolveAssetUrl('assets/classic/spades_A.png')}" style="position: absolute; width: 60px; height: 90px; top: 0; left: 20px; border-radius: 4px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); z-index: 3; border: 1px solid rgba(255,255,255,0.1);" draggable="false" oncontextmenu="return false;" ondragstart="return false;">
                             <img src="${resolveAssetUrl('assets/classic/hearts_K.png')}" style="position: absolute; width: 60px; height: 90px; top: 25px; left: 10px; border-radius: 4px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); z-index: 2; border: 1px solid rgba(255,255,255,0.1);" draggable="false" oncontextmenu="return false;" ondragstart="return false;">
                             <img src="${resolveAssetUrl('assets/classic/clubs_10.png')}" style="position: absolute; width: 60px; height: 90px; top: 50px; left: 0; border-radius: 4px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); z-index: 1; border: 1px solid rgba(255,255,255,0.1);" draggable="false" oncontextmenu="return false;" ondragstart="return false;">
                        </div>
                        <div style="color: rgba(255, 255, 255, 0.8); font-size: 14px; font-weight: 600;">
                            <i class="fas fa-chess" style="margin-right: 6px;"></i>Classic
                        </div>
                    </div>
                </div>
                
            `;

            modal.appendChild(modalContent);
            document.body.appendChild(modal);

            // Add click handlers for previews
            const fantasyPreview = modalContent.querySelector('.fantasy-preview');
            const classicPreview = modalContent.querySelector('.classic-preview');

            fantasyPreview.addEventListener('click', () => {
                durakCardPack = 'fantasy';
                updateDurakCardBackStyle();
                modal.remove();
                sendDurak({ type: 'durak-propose', mode: 'perevodnoy', cardPack: 'fantasy' });
            });

            classicPreview.addEventListener('click', () => {
                durakCardPack = 'classic';
                updateDurakCardBackStyle();
                modal.remove();
                sendDurak({ type: 'durak-propose', mode: 'perevodnoy', cardPack: 'classic' });
            });

            // Close modal on backdrop click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.remove();
                }
            });

            // Add CSS animations
            const style = document.createElement('style');
            style.textContent = `
                @keyframes fadeIn {
                    from { opacity: 0; }
                    to { opacity: 1; }
                }
                @keyframes slideUp {
                    from { opacity: 0; transform: translateY(20px); }
                    to { opacity: 1; transform: translateY(0); }
                }
            `;
            document.head.appendChild(style);
        }

        function ensureDurakControlButton() {
            const screen = document.querySelector('.call-screen');
            if (!screen) return;
            let bar = screen.querySelector('.call-bottom-bar');
            const ctr = screen.querySelector('.controls');
            if (!ctr) return;
            const existingBtn = document.getElementById('durakBtn');
            if (!bar) {
                bar = document.createElement('div');
                bar.className = 'call-bottom-bar';
                ctr.parentNode.insertBefore(bar, ctr);
                if (existingBtn && ctr.contains(existingBtn)) {
                    ctr.removeChild(existingBtn);
                    bar.appendChild(existingBtn);
                }
                bar.appendChild(ctr);
            } else if (existingBtn && ctr.contains(existingBtn)) {
                bar.insertBefore(existingBtn, ctr);
            }
            if (document.getElementById('durakBtn')) {
                const b = document.getElementById('durakBtn');
                if (!b.onclick) b.onclick = onDurakToolbarClick;
                const ic = b.querySelector('i');
                if (ic && !ic.classList.contains('fa-gamepad')) {
                    ic.className = 'fas fa-gamepad';
                }
                return;
            }
            const b = document.createElement('button');
            b.type = 'button';
            b.id = 'durakBtn';
            b.className = 'ctrl-btn';
            b.title = 'Дурак';
            b.innerHTML = '<i class="fas fa-gamepad"></i>';
            b.onclick = onDurakToolbarClick;
            bar.insertBefore(b, bar.firstChild);
        }

        function ensureDurakCallPanelToggle() {
            const bar = document.querySelector('.call-bottom-bar');
            if (!bar || document.getElementById('durakCallPanelToggle')) return;
            const t = document.createElement('button');
            t.type = 'button';
            t.id = 'durakCallPanelToggle';
            t.className = 'durak-call-panel-toggle';
            t.title = 'Панель звонка';
            t.innerHTML = '<i class="fas fa-chevron-up"></i>';
            t.onclick = toggleDurakCallPanel;
            bar.appendChild(t);
        }

        function syncDurakPanelToggleIcon() {
            const csr = document.getElementById('callScreenRoot');
            const btn = document.getElementById('durakCallPanelToggle');
            if (!csr || !btn) return;
            const i = btn.querySelector('i');
            if (!i) return;
            i.className = csr.classList.contains('durak-call-drawer-open') ? 'fas fa-chevron-down' : 'fas fa-chevron-up';
        }

        function toggleDurakCallPanel() {
            const csr = document.getElementById('callScreenRoot');
            if (!csr) return;
            csr.classList.toggle('durak-call-drawer-open');
            syncDurakPanelToggleIcon();
            if (typeof triggerWatchFocusActivity === 'function') triggerWatchFocusActivity();
            requestAnimationFrame(() => {
                requestAnimationFrame(updateDurakCallTogglePosition);
            });
        }

        function ensureDurakCallToggleHost() {
            const csr = document.getElementById('callScreenRoot');
            if (!csr) return null;
            let h = document.getElementById('durakCallToggleHost');
            if (!h) {
                h = document.createElement('div');
                h.id = 'durakCallToggleHost';
                h.className = 'durak-call-toggle-host';
                csr.appendChild(h);
            }
            return h;
        }

        function durakNotifyGameEnded(g) {
            if (!g) return;
            const names = g.names || {};
            const w = g.winnerId;
            let body;
            if (w && names[w]) {
                body = `Игра завершена. Победитель: ${String(names[w])}`;
            } else if (w) {
                body = 'Игра завершена. Победитель определён.';
            } else {
                body = 'Игра завершена. Ничья или игра прервана.';
            }
            showNotification('Дурак', body, 'info');
        }

        function durakCountdownSeconds(deadline, totalSeconds = 30) {
            const limitMs = Math.max(0, Number(totalSeconds || 0)) * 1000;
            const leftMs = Math.max(0, Number(deadline || 0) - Date.now());
            if (!limitMs) return Math.max(0, Math.floor(leftMs / 1000));
            const elapsedMs = Math.max(0, limitMs - leftMs);
            return Math.max(0, Math.min(totalSeconds, totalSeconds - Math.floor(elapsedMs / 1000)));
        }

        function durakShowdownPlaceText(place, totalPlayers) {
            if (place === 1) return 'Победитель';
            if (place === 2) return 'Второе место';
            if (place === 3) return 'Третье место';
            if (place >= totalPlayers) return 'Дурак';
            return `${place} место`;
        }

        function durakShowdownPlaceBadge(place, totalPlayers) {
            if (place === 1) return '👑';
            if (place === 2) return '🥈';
            if (place === 3) return '🥉';
            if (place >= totalPlayers) return '🃏';
            return String(place || '•');
        }

        function updateDurakCallTogglePosition() {
            const host = document.getElementById('durakCallToggleHost');
            const btn = document.getElementById('durakCallPanelToggle');
            if (!host || !btn || !host.contains(btn)) return;
            const pad = 8;
            const bw = 44;
            const bh = 44;
            const gapRightOfTile = 10;
            const gapAboveHand = 8;
            const vw = window.innerWidth || document.documentElement.clientWidth || 0;
            const meTile = document.querySelector('#durakOverlay .durak-player-tile.is-me');
            const handWrap = document.querySelector('#durakOverlay .durak-hand-wrap');
            if (meTile && handWrap) {
                const mt = meTile.getBoundingClientRect();
                const hw = handWrap.getBoundingClientRect();
                let left = mt.right + gapRightOfTile;
                let top = hw.top - gapAboveHand - bh;
                if (vw) {
                    left = Math.max(pad, Math.min(left, vw - bw - pad));
                }
                top = Math.max(pad, top);
                host.style.left = `${Math.round(left)}px`;
                host.style.top = `${Math.round(top)}px`;
                host.style.right = 'auto';
                host.style.bottom = 'auto';
                host.style.transform = 'none';
                return;
            }
            if (!handWrap) {
                host.style.left = 'auto';
                host.style.right = `${pad}px`;
                host.style.top = 'auto';
                host.style.bottom = '108px';
                host.style.transform = 'none';
                return;
            }
            const r = handWrap.getBoundingClientRect();
            let left = r.right - bw - 8;
            if (vw) {
                left = Math.max(pad, Math.min(left, vw - bw - pad));
            }
            const top = r.top - 10 - bh;
            host.style.left = `${Math.round(left)}px`;
            host.style.top = `${Math.round(Math.max(pad, top))}px`;
            host.style.right = 'auto';
            host.style.bottom = 'auto';
            host.style.transform = 'none';
        }

        function placeDurakCallPanelToggle() {
            const btn = document.getElementById('durakCallPanelToggle');
            if (!btn) return;
            const csr = document.getElementById('callScreenRoot');
            const bar = document.querySelector('.call-bottom-bar');
            const host = ensureDurakCallToggleHost();
            if (!bar) return;

            const clearHost = () => {
                if (!host) return;
                host.style.display = 'none';
                host.style.left = '';
                host.style.top = '';
                host.style.right = '';
                host.style.bottom = '';
                host.style.transform = '';
            };
            const resetBtnInline = () => {
                btn.style.position = '';
                btn.style.left = '';
                btn.style.top = '';
            };

            if (!csr || !csr.classList.contains('durak-playing')) {
                clearHost();
                resetBtnInline();
                bar.appendChild(btn);
                return;
            }
            const desktop = typeof window.matchMedia === 'function' && window.matchMedia('(min-width: 769px)').matches;
            if (desktop && host) {
                host.style.display = 'block';
                host.appendChild(btn);
                resetBtnInline();
                requestAnimationFrame(() => {
                    requestAnimationFrame(updateDurakCallTogglePosition);
                });
            } else {
                clearHost();
                resetBtnInline();
                bar.appendChild(btn);
            }
        }

        function ensureDurakOverlayDragUi(ov) {
            if (!ov || ov.dataset.durakDragUiBound) return;
            ov.dataset.durakDragUiBound = '1';
            ov.addEventListener(
                'dragstart',
                (e) => {
                    if (!durakGameState || durakGameState.phase !== 'playing') return;
                    const t = e.target;
                    if (!t || !t.classList || !t.classList.contains('durak-card-face') || !t.closest('#durakMyHand')) return;
                    ov.classList.add('durak-dragging-from-hand');
                },
                true
            );
            ov.addEventListener('dragend', () => {
                ov.classList.remove('durak-dragging-from-hand');
                ov.querySelector('.durak-hand-wrap')?.classList.remove('durak-drag-over-return');
                ov.querySelector('#durakTable')?.classList.remove('durak-drag-over-play');
            }, true);
        }

        function syncDurakCallScreenClasses(g) {
            const csr = document.getElementById('callScreenRoot');
            const tgl = document.getElementById('durakCallPanelToggle');
            if (!csr) return;
            const activeDurakPhase = !!g && (g.phase === 'playing' || g.phase === 'showdown');
            if (!activeDurakPhase) {
                csr.classList.remove('durak-playing', 'durak-call-drawer-open');
                window.__durakDealAnimBattle = undefined;
                if (tgl) tgl.style.display = 'none';
                return;
            }
            csr.classList.add('durak-playing');
            if (tgl) tgl.style.display = '';
            syncDurakPanelToggleIcon();
        }

        const DURAK_RANK_ORDER = { '6': 0, '7': 1, '8': 2, '9': 3, T: 4, J: 5, Q: 6, K: 7, A: 8 };

        function durakParseCardClient(rawId) {
            let s = String(rawId || '').trim().toLowerCase();
            if (!s) return null;
            s = s.replace(/~\d+$/, '');
            if (s.length < 2) return null;
            const suit = s.slice(-1);
            if (!'shdc'.includes(suit)) return null;
            const head = s.slice(0, -1);
            let rank;
            if (head === '10' || head === 't') rank = 'T';
            else if (head.length === 1) {
                const c = head;
                if (c === 't') rank = 'T';
                else if (/^[6-9]$/.test(c)) rank = c;
                else if (/^[jqka]$/i.test(c)) rank = c.toUpperCase();
                else return null;
            } else return null;
            if (DURAK_RANK_ORDER[rank] === undefined) return null;
            return { rank, suit };
        }

        let durakCardPack = 'classic'; // 'classic' or 'fantasy'

        function updateDurakCardBackStyle() {
            // Remove existing card back style if any
            const existingStyle = document.getElementById('durakCardBackStyle');
            if (existingStyle) existingStyle.remove();

            // Add new style with selected pack
            const backUrl = resolveAssetUrl(`assets/${durakCardPack}/back.png`);
            const style = document.createElement('style');
            style.id = 'durakCardBackStyle';
            style.textContent = `
                .durak-card-face {
                    background-image: url('${backUrl}');
                }
                .durak-card-face.back,
                .durak-card-back-layer {
                    background-image: url('${backUrl}');
                }
                .durak-deck-stack .durak-card-back-layer {
                    background-image: url('${backUrl}') !important;
                    background-size: cover;
                    background-position: center center;
                    background-repeat: no-repeat;
                }
            `;
            document.head.appendChild(style);
        }

        function durakCardImageUrl(cardId) {
            const p = durakParseCardClient(cardId);
            if (!p) return '';
            const sm = { s: 'spades', h: 'hearts', d: 'diamonds', c: 'clubs' };
            const sn = sm[p.suit];
            if (!sn) return '';
            const rf = p.rank === 'T' ? '10' : p.rank;
            return resolveAssetUrl(`assets/${durakCardPack}/${sn}_${rf}.png`);
        }

        /** Отдельные PNG в assets (spades_A.png …); иначе спрайт. */
        function durakCardFaceStyle(cardId) {
            const u = durakCardImageUrl(cardId);
            if (u) {
                const esc = u.replace(/'/g, "\\'");
                return `background-image:url('${esc}');background-size:cover;background-position:center;background-repeat:no-repeat;background-color:#1a1528`;
            }
            return durakCardBgStyle(cardId);
        }

        function durakEscapeDataAttr(s) {
            return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;');
        }

        function durakCardKeyForMatch(s) {
            return String(s || '')
                .trim()
                .replace(/~\d+$/, '')
                .toLowerCase();
        }

        function durakFindTableCardFace(ov, cardId) {
            const want = durakCardKeyForMatch(cardId);
            if (!ov || !want) return null;
            const faces = ov.querySelectorAll('#durakTable .durak-card-face[data-durak-card]');
            for (let i = 0; i < faces.length; i++) {
                if (durakCardKeyForMatch(faces[i].getAttribute('data-durak-card')) === want) return faces[i];
            }
            return null;
        }

        /** Сразу после innerHTML: спрятать карты lastPlay на столе до полёта (иначе один кадр «мигания» до rAF). */
        function durakPrepFlyTargetsHidden(ov, g) {
            if (!ov || !g || g.phase !== 'playing' || !g.lastPlay || !g.lastPlay.cards || !g.lastPlay.seq) return;
            if (g.lastPlay.kind === 'take') return;
            if (String(g.lastPlay.by) === String(myId)) return;
            const seen = window.__durakLastPlayAnimSeq || 0;
            if (g.lastPlay.seq <= seen) return;
            g.lastPlay.cards.forEach((cid) => {
                const el = durakFindTableCardFace(ov, cid);
                if (el) el.classList.add('durak-card-fly-target-pending');
            });
        }

        function durakClearFlyTargetsPending(ov, cardIds) {
            if (!ov || !cardIds) return;
            cardIds.forEach((cid) => {
                const el = durakFindTableCardFace(ov, cid);
                if (el) el.classList.remove('durak-card-fly-target-pending');
            });
        }

        function durakFlyNormalizeRect(fr) {
            if (!fr) return null;
            if (typeof fr.left === 'number' && typeof fr.top === 'number' && typeof fr.width === 'number' && typeof fr.height === 'number') {
                return { left: fr.left, top: fr.top, width: fr.width, height: fr.height };
            }
            if (typeof fr.getBoundingClientRect === 'function') return fr.getBoundingClientRect();
            return null;
        }

        function durakAnimateCardFly(fromRect, toEl, bgStyle) {
            const fr = durakFlyNormalizeRect(fromRect);
            if (!fr || fr.width < 4 || fr.height < 4 || !toEl || !bgStyle) return;
            const toRect = toEl.getBoundingClientRect();
            if (toRect.width < 2 || toRect.height < 2) return;
            const fly = document.createElement('div');
            fly.className = 'durak-card-face durak-card-fly-ghost';
            fly.setAttribute('aria-hidden', 'true');
            const w = toRect.width;
            const h = toRect.height;
            fly.style.cssText =
                bgStyle +
                `;position:fixed;left:0;top:0;width:${w}px;height:${h}px;margin:0;border-radius:8px;border:1px solid rgba(255,255,255,0.3);box-shadow:0 16px 44px rgba(0,0,0,0.58);transform-origin:center center;pointer-events:none;z-index:10060;will-change:transform,opacity`;
            document.body.appendChild(fly);
            const startCx = fr.left + fr.width / 2;
            /* Старт от верхней части плитки соперника (.durak-opponents-ring), не от низа —
               иначе линия к столу визуально совпадает с колонкой колоды слева. */
            const startCy = fr.top + Math.max(8, fr.height * 0.28);
            const fx = startCx - w / 2;
            const fy = startCy - h / 2;
            const tx = toRect.left + (toRect.width - w) / 2;
            const ty = toRect.top + (toRect.height - h) / 2;
            toEl.classList.add('durak-card-fly-target-pending');
            let finished = false;
            const cleanup = () => {
                if (finished) return;
                finished = true;
                fly.remove();
                toEl.classList.remove('durak-card-fly-target-pending');
            };
            const durMs = 1100;
            const easing = 'cubic-bezier(0.16, 0.8, 0.14, 1)';
            const run = () => {
                if (typeof fly.animate === 'function') {
                    try {
                        const anim = fly.animate(
                            [
                                { transform: `translate(${fx}px, ${fy}px) scale(0.65) rotate(-12deg)`, opacity: 0.82 },
                                { transform: `translate(${tx}px, ${ty}px) scale(1) rotate(0deg)`, opacity: 1 }
                            ],
                            { duration: durMs, easing, fill: 'forwards' }
                        );
                        anim.onfinish = () => cleanup();
                        setTimeout(cleanup, durMs + 200);
                        return;
                    } catch (e) {
                        /* fall through */
                    }
                }
                fly.style.transition = 'none';
                fly.style.opacity = '0.82';
                fly.style.transform = `translate(${fx}px, ${fy}px) scale(0.65) rotate(-12deg)`;
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => {
                        fly.style.transition = `transform ${durMs}ms ${easing}, opacity ${durMs}ms ease-out`;
                        fly.style.opacity = '1';
                        fly.style.transform = `translate(${tx}px, ${ty}px) scale(1) rotate(0deg)`;
                        fly.addEventListener('transitionend', cleanup, { once: true });
                        setTimeout(cleanup, durMs + 200);
                    });
                });
            };
            requestAnimationFrame(() => requestAnimationFrame(run));
        }

        function durakRunPlayFlyAnimations(ov, g, preCapturedFromRect) {
            if (!ov || !g || g.phase !== 'playing') return;
            const lp = g.lastPlay;
            if (!lp || !lp.by || !Array.isArray(lp.cards) || !lp.cards.length || !lp.seq) return;
            const seen = window.__durakLastPlayAnimSeq || 0;
            if (lp.seq <= seen) return;

            if (lp.kind === 'take') {
                window.__durakLastPlayAnimSeq = lp.seq;
                return;
            }

            if (String(lp.by) === String(myId)) {
                window.__durakLastPlayAnimSeq = lp.seq;
                return;
            }

            const tryLaunch = (fromRect) => {
                const fr = durakFlyNormalizeRect(fromRect);
                if (!fr || fr.width < 4 || fr.height < 4) return false;
                let any = false;
                for (let i = 0; i < lp.cards.length; i++) {
                    if (durakFindTableCardFace(ov, lp.cards[i])) {
                        any = true;
                        break;
                    }
                }
                if (!any) return false;
                window.__durakLastPlayAnimSeq = lp.seq;
                lp.cards.forEach((cid, idx) => {
                    const toEl = durakFindTableCardFace(ov, cid);
                    if (!toEl) return;
                    const st = durakCardFaceStyle(cid);
                    setTimeout(() => durakAnimateCardFly(fr, toEl, st), idx * 120);
                });
                return true;
            };

            const findTileRect = () => {
                const by = String(lp.by);
                const ring = ov.querySelector('.durak-opponents-ring');
                if (!ring) return null;
                const tiles = ring.querySelectorAll('.durak-player-tile[data-durak-pid]');
                for (let i = 0; i < tiles.length; i++) {
                    if (String(tiles[i].getAttribute('data-durak-pid')) === by) {
                        return tiles[i].getBoundingClientRect();
                    }
                }
                return null;
            };

            const run = () => {
                if ((window.__durakLastPlayAnimSeq || 0) >= lp.seq) return;
                const rFresh = findTileRect();
                if (tryLaunch(rFresh)) return;
                if (tryLaunch(preCapturedFromRect)) return;
                window.__durakLastPlayAnimSeq = lp.seq;
                durakClearFlyTargetsPending(ov, lp.cards);
            };

            requestAnimationFrame(() => {
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => {
                        run();
                        setTimeout(run, 64);
                    });
                });
            });
        }

        function durakCanBeatClient(attackId, defendId, trumpSuit) {
            const a = durakParseCardClient(attackId);
            const d = durakParseCardClient(defendId);
            if (!a || !d) return false;
            const aTrump = a.suit === trumpSuit;
            const dTrump = d.suit === trumpSuit;
            if (aTrump && dTrump) return DURAK_RANK_ORDER[d.rank] > DURAK_RANK_ORDER[a.rank];
            if (!aTrump && dTrump) return true;
            if (aTrump && !dTrump) return false;
            if (a.suit === d.suit) return DURAK_RANK_ORDER[d.rank] > DURAK_RANK_ORDER[a.rank];
            return false;
        }

        function durakEnsureTransfersClient(row) {
            if (!row || typeof row !== 'object') return [];
            if (Array.isArray(row.transferStack)) return row.transferStack;
            if (row.transferCard) return [{ card: row.transferCard, defense: row.transferDefense || null }];
            return [];
        }

        function durakNextDefendTargetClient(battle) {
            const table = (battle && battle.table) || [];
            for (const row of table) {
                if (!row.defense) return { row, beatAttack: true };
                const ts = durakEnsureTransfersClient(row);
                for (let i = 0; i < ts.length; i++) {
                    if (!ts[i].defense) return { row, beatAttack: false };
                }
            }
            return null;
        }

        function durakEnumerateBeatTargetsClient(battle, defendCardId, trumpSuit) {
            const out = [];
            const table = (battle && battle.table) || [];
            for (const row of table) {
                if (!row.defense && durakCanBeatClient(row.attack, defendCardId, trumpSuit)) {
                    out.push({ row, anchor: row.attack });
                }
                const ts = durakEnsureTransfersClient(row);
                for (let i = 0; i < ts.length; i++) {
                    const t = ts[i];
                    if (!t.defense && durakCanBeatClient(t.card, defendCardId, trumpSuit)) {
                        out.push({ row, anchor: t.card });
                    }
                }
            }
            return out;
        }

        function durakCanTransferOnRowClient(g, row, cardId) {
            if (!g || g.mode !== 'perevodnoy') return false;
            if (row.defense) return false;
            if (row.beatType === 'toss') return false;
            const tr = durakParseCardClient(cardId);
            const lead = durakParseCardClient(row.attack);
            return !!(tr && lead && tr.rank === lead.rank);
        }

        /** Клик/сброс без зоны: нужен явный выбор ряда (несколько целей или побить/перевод). */
        function durakHandPlayNeedsTableChoice(cardId) {
            const g = durakGameState;
            if (!g || !myId || g.battle?.subPhase !== 'defend') return false;
            const pl = g.players || [];
            const defPid = pl[g.defenderIndex] ? pl[g.defenderIndex].id : '';
            if (defPid !== myId) return false;
            const b = g.battle;
            const trump = g.trump;
            const beats = durakEnumerateBeatTargetsClient(b, cardId, trump);
            const xferRows = (b.table || []).filter((row) => durakCanTransferOnRowClient(g, row, cardId));
            if (beats.length > 1) return true;
            if (xferRows.length > 1) return true;
            if (beats.length === 1 && xferRows.length === 1 && xferRows[0] === beats[0].row) {
                const row = beats[0].row;
                const tr = durakParseCardClient(cardId);
                const lead = durakParseCardClient(row.attack);
                if (tr && lead && tr.rank === lead.rank && durakCanBeatClient(row.attack, cardId, trump)) return true;
            }
            if (beats.length === 1 && xferRows.length === 1 && xferRows[0] !== beats[0].row) return true;
            return false;
        }

        function durakPlayCard(cardId, target, against) {
            const action = { type: 'play', card: cardId };
            if (target === 'beat' || target === 'transfer') action.target = target;
            if (against) action.against = against;
            sendDurak({ type: 'durak-action', action });
        }

        function durakDropCardToTarget(card, targetEl) {
            if (!card) return;
            const target = targetEl || null;
            if (target && (target.closest?.('#durakMyHand') || target.closest?.('.durak-hand-wrap'))) {
                window.__durakDragCard = null;
                return;
            }
            const beatEl = target && target.closest ? target.closest('[data-durak-beat-anchor]') : null;
            if (beatEl) {
                durakPlayCard(card, 'beat', beatEl.getAttribute('data-durak-beat-anchor'));
                return;
            }
            const z = target && target.closest ? target.closest('[data-durak-drop]') : null;
            if (z) {
                const drop = z.getAttribute('data-durak-drop');
                const against = z.getAttribute('data-durak-against') || '';
                if (drop === 'transfer') durakPlayCard(card, 'transfer', against || undefined);
                return;
            }
            if (durakHandPlayNeedsTableChoice(card)) {
                showNotification('Дурак', 'Перетащите карту на атакующую карту или в слот перевода', 'info');
                return;
            }
            durakPlayCard(card);
        }

        function durakClearTouchDragUi(ov) {
            try {
                const st = window.__durakTouchDragState;
                if (st && st.ghost && st.ghost.remove) st.ghost.remove();
            } catch (_) {}
            window.__durakTouchDragState = null;
            try { ov?.classList?.remove('durak-dragging-from-hand'); } catch (_) {}
            try { ov?.querySelectorAll?.('[data-durak-drop]')?.forEach((z) => z.classList.remove('durak-drop-hover')); } catch (_) {}
        }

        function durakStartTouchDrag(ov, cardId, touch) {
            if (!ov || !cardId || !touch) return;
            const ghost = document.createElement('div');
            ghost.className = 'durak-card-face durak-card-fly-ghost';
            ghost.style.position = 'fixed';
            ghost.style.zIndex = '2500';
            ghost.style.pointerEvents = 'none';
            ghost.style.width = '68px';
            ghost.style.height = '94px';
            ghost.style.transform = 'translate(-50%, -50%)';
            ghost.style.opacity = '0.94';
            ghost.style.boxShadow = '0 18px 30px rgba(0,0,0,0.42)';
            ghost.style.filter = 'saturate(1.08)';
            const st = durakCardFaceStyle(cardId);
            if (st) ghost.style.cssText += `;${st}`;
            ghost.style.left = `${touch.clientX}px`;
            ghost.style.top = `${touch.clientY}px`;
            document.body.appendChild(ghost);
            window.__durakTouchDragState = { cardId, ghost };
            ov.classList.add('durak-dragging-from-hand');
        }

        function durakMoveTouchDrag(ov, touch) {
            const st = window.__durakTouchDragState;
            if (!st || !touch) return;
            if (st.ghost) {
                st.ghost.style.left = `${touch.clientX}px`;
                st.ghost.style.top = `${touch.clientY}px`;
            }
            ov.querySelectorAll('[data-durak-drop]').forEach((z) => z.classList.remove('durak-drop-hover'));
            const under = document.elementFromPoint(touch.clientX, touch.clientY);
            const drop = under && under.closest ? under.closest('[data-durak-drop]') : null;
            if (drop) drop.classList.add('durak-drop-hover');
        }

        function durakTableDrop(ev) {
            ev.preventDefault();
            const card = ev.dataTransfer?.getData?.('text/plain') || window.__durakDragCard;
            window.__durakDragCard = null;
            durakDropCardToTarget(card, ev.target);
        }

        function durakBeatAnchorAttr(row, defPid, myPid, sub, kind) {
            if (!myPid || defPid !== myPid || sub !== 'defend') return '';
            if (kind === 'attack' && !row.defense) {
                return ` data-durak-beat-anchor="${row.attack}"`;
            }
            return '';
        }

        function durakBeatAnchorTransferCard(row, defPid, myPid, sub, tcard) {
            if (!myPid || defPid !== myPid || sub !== 'defend') return '';
            if (!tcard) return '';
            const slot = durakEnsureTransfersClient(row).find((t) => t.card === tcard);
            if (!slot || slot.defense) return '';
            return ` data-durak-beat-anchor="${tcard}"`;
        }

        function durakHasAnyDefenseOnTableClient(battle) {
            const rows = (battle && battle.table) || [];
            for (const row of rows) {
                if (row.defense) return true;
                const ts = durakEnsureTransfersClient(row);
                for (const t of ts) {
                    if (t.defense) return true;
                }
            }
            return false;
        }

        function durakRowCanTransferFromHand(g, row) {
            if (!g?.myHand?.length) return false;
            const lead = durakParseCardClient(row.attack);
            if (!lead) return false;
            const hasSameRank = g.myHand.some((cid) => {
                const p = durakParseCardClient(cid);
                return p && p.rank === lead.rank;
            });
            if (!hasSameRank) return false;
            const b = g.battle || { table: [] };
            const cap = g.firstDealRules ? 5 : 6;
            const need = durakTotalCardsInAttackPileAfterOneMoreTransferClient(b, row);
            if (need > cap) return false;
            const ni = durakResolveNextDefenderIndexClient(g);
            const np = (g.players || [])[ni];
            const nextDefCount = np && typeof np.cardCount === 'number' ? np.cardCount : 0;
            return nextDefCount >= need;
        }

        function durakResolveNextDefenderIndexClient(g) {
            const players = g?.players || [];
            const n = players.length;
            if (!n) return 0;
            let ni = (((g?.defenderIndex || 0) % n) + 1) % n;
            let steps = 0;
            while (steps < n) {
                const p = players[ni];
                if (p && (p.cardCount || 0) > 0) return ni;
                ni = (ni + 1) % n;
                steps++;
            }
            return (((g?.defenderIndex || 0) % n) + 1) % n;
        }

        function durakTotalCardsInAttackPileAfterOneMoreTransferClient(battle, targetRow) {
            let n = 0;
            const rows = (battle && battle.table) || [];
            for (const row of rows) {
                const ts = durakEnsureTransfersClient(row);
                const addHere = row === targetRow ? 1 : 0;
                if (!row.defense) {
                    n += 1 + ts.length + addHere;
                } else {
                    for (const t of ts) {
                        if (!t.defense) n++;
                    }
                }
            }
            return n;
        }

        function durakTransferSlotBesideRowHtml(g, row, defPid, myPid, sub) {
            if (g.mode !== 'perevodnoy' || !myPid || defPid !== myPid || sub !== 'defend') return '';
            if (durakHasAnyDefenseOnTableClient(g.battle)) return '';
            if (row.defense || row.beatType === 'toss') return '';
            if (!durakRowCanTransferFromHand(g, row)) return '';
            return `<div class="durak-transfer-slot durak-drop-zone" data-durak-drop="transfer" data-durak-against="${row.attack}" title="Перевод"></div>`;
        }

        function renderDurakUi() {
            if (durakUiTickTimer) {
                clearTimeout(durakUiTickTimer);
                durakUiTickTimer = null;
            }
            ensureDurakTopbarLine();
            ensureDurakControlButton();
            ensureDurakCallPanelToggle();
            const vc = document.getElementById('videosContainer');
            if (!vc) return;
            let ov = document.getElementById('durakOverlay');
            if (!durakGameState) {
                window.__durakLastPlayAnimSeq = 0;
                if (ov) ov.remove();
                applyDurakFocusMode(false);
                syncDurakCallScreenClasses(null);
                const line = document.getElementById('durakTopbarLine');
                if (line) line.textContent = '';
                return;
            }
            const g = durakGameState;
            if (g.phase !== 'showdown') {
                durakShowdownDismissed = false;
            }
            if (g.phase === 'ended') {
                window.__durakLastPlayAnimSeq = 0;
                if (ov) ov.remove();
                applyDurakFocusMode(false);
                syncDurakCallScreenClasses(null);
                const line = document.getElementById('durakTopbarLine');
                if (line) line.textContent = '';
                return;
            }
            if (g.phase !== 'playing') {
                window.__durakLastPlayAnimSeq = 0;
            }
            if (g.phase === 'showdown' && durakShowdownDismissed) {
                if (ov) ov.remove();
                applyDurakFocusMode(false);
                syncDurakCallScreenClasses(null);
                const hiddenLine = document.getElementById('durakTopbarLine');
                if (hiddenLine) hiddenLine.textContent = '';
                return;
            }
            applyDurakFocusMode(g.phase === 'lobby' || g.phase === 'playing' || g.phase === 'showdown');
            const line = document.getElementById('durakTopbarLine');
            const starterName = g.names && g.initiatorId ? g.names[g.initiatorId] : (g.players || []).find((p) => p.id === g.initiatorId)?.name || '';
            if (line) {
                if (g.phase === 'lobby') {
                    line.textContent = `${starterName || 'Игрок'} начинает игру`;
                } else if (g.phase === 'showdown') {
                    const left = durakCountdownSeconds(g.resultDeadline, 30);
                    line.textContent = `Итог партии · стол закроется через ${left} сек`;
                } else if (g.phase === 'playing') {
                    line.textContent = `Козырь: ${durakSuitRu(g.trump)} · в колоде: ${g.deckCount}`;
                } else {
                    line.textContent = g.winnerId ? 'Игра окончена' : 'Игра окончена';
                }
            }
            if (!ov) {
                ov = document.createElement('div');
                ov.id = 'durakOverlay';
                ov.className = 'durak-overlay';
                vc.appendChild(ov);
            }
            const inLobby = g.phase === 'lobby';
            const imIn = (g.players || []).some((p) => p.id === myId);
            const mod = isCreator || isGuestAdmin;
            let html = '<div class="durak-overlay-inner">';
            if (inLobby) {
                const canCancel = mod || g.initiatorId === myId;
                {
                    const names = (g.players || [])
                        .map((p) => String(p?.name || '').trim())
                        .filter(Boolean);
                    const n = names.length;
                    const list = n ? names.join(', ') : '—';
                    html += `<div class="durak-banner">Дурак (${n}): ${escapeHtml(list)}</div>`;
                }
                html += '<div class="durak-lobby-actions">';
                if (!imIn) {
                    html += '<button type="button" id="durakActJoin">Присоединиться</button>';
                } else {
                    html += '<button type="button" id="durakActLeaveLobby">Выйти</button>';
                }
                html += `<button type="button" id="durakActStart" ${(g.players || []).length < 2 ? 'disabled' : ''}>Начать</button>`;
                if (canCancel) html += '<button type="button" id="durakActCancel">Отмена</button>';
                html += '</div>';
            } else if (g.phase === 'playing' || g.phase === 'showdown') {
                const battle = g.battle || { table: [], subPhase: '', attackerPid: '', defenderPid: '' };
                const isShowdown = g.phase === 'showdown';
                const pl = g.players || [];
                const defPid = pl[g.defenderIndex] ? pl[g.defenderIndex].id : '';
                const attPid = pl[g.attackerIndex] ? pl[g.attackerIndex].id : '';
                const sub = battle.subPhase || '';
                const takeTossLeftSec =
                    sub === 'take_toss' && g.turnDeadline
                        ? Math.max(0, Math.ceil((Number(g.turnDeadline) - Date.now()) / 1000))
                        : 0;
                const mobUi = isMobileLayout();
                const npl = pl.length;
                const defIdx = typeof g.defenderIndex === 'number' ? g.defenderIndex : -1;
                const leftNeighbor = npl > 0 && defIdx >= 0 ? pl[(defIdx - 1 + npl) % npl]?.id || '' : '';
                const rightNeighbor = npl > 0 && defIdx >= 0 ? pl[(defIdx + 1) % npl]?.id || '' : '';
                const donePidSet = new Set([leftNeighbor, rightNeighbor].filter(Boolean));
                const doneEligiblePids = (sub === 'toss' || sub === 'take_toss')
                    ? pl.filter((p) => donePidSet.has(p.id) && Number(p.cardCount || 0) > 0).map((p) => p.id)
                    : [];
                const neighborTossPids =
                    sub === 'defend' && Array.isArray(battle.neighborTossEligiblePids)
                        ? battle.neighborTossEligiblePids.map((x) => String(x))
                        : [];
                const showTakeBtn = defPid === myId && sub === 'defend' && !!durakNextDefendTargetClient(battle);
                const showDoneBtn = doneEligiblePids.includes(myId) && (sub === 'toss' || sub === 'take_toss');
                const doneLabel = sub === 'take_toss' ? `Бито (${takeTossLeftSec})` : 'Бито';
                const sameAtkDef = !!(attPid && defPid && attPid === defPid);
                const others = pl.filter((p) => p.id !== myId);
                const finishOrder = Array.isArray(g.finishOrder) ? g.finishOrder.map((x) => String(x)) : [];
                const placeOf = (pid) => {
                    const i = finishOrder.indexOf(String(pid || ''));
                    return i >= 0 ? i + 1 : 0;
                };
                const placeLabel = (place) => {
                    if (!place) return '';
                    if (place === 1) return '👑';
                    if (place === 2) return '🥈';
                    if (place === 3) return '🥉';
                    return `${place} место`;
                };
                const showdownHandsMap = new Map(
                    (Array.isArray(g.showdownHands) ? g.showdownHands : []).map((entry) => [
                        String(entry?.id || ''),
                        Array.isArray(entry?.cards) ? entry.cards : []
                    ])
                );
                const renderShowdownCardsRow = (cards) => {
                    const safeCards = Array.isArray(cards) ? cards.filter(Boolean) : [];
                    if (!safeCards.length) {
                        return '<div class="durak-showdown-player-empty">Нет карт</div>';
                    }
                    return `<div class="durak-showdown-player-cards">${safeCards.map((cid) => `<div class="durak-card-face" style="${durakCardFaceStyle(cid)}"></div>`).join('')}</div>`;
                };
                const highlightId = battle.attackerPid || attPid || '';
                const thoughtPid = sub === 'take_toss' ? String(battle.takePid || battle.defenderPid || '') : '';
                let oppHtml = '';
                others.forEach((p) => {
                    const hi = p.id === highlightId ? ' is-highlight' : '';
                    const defenderClass = battle.defenderPid && p.id === battle.defenderPid ? ' is-defender' : '';
                    const tosserClass =
                        (((sub === 'toss' || sub === 'take_toss') && doneEligiblePids.includes(p.id)) ||
                            (sub === 'defend' && neighborTossPids.includes(String(p.id))))
                            ? ' is-tosser'
                            : '';
                    const place = placeOf(p.id);
                    const placeClass = place > 0 && place <= 3 ? ` is-place-${place}` : '';
                    const thought = thoughtPid && String(p.id) === thoughtPid ? '<div class="durak-think-bubble is-bottom">Беру</div>' : '';
                    const roles = [];
                    if (sameAtkDef && battle.defenderPid && p.id === battle.defenderPid) {
                        roles.push('Отбивается');
                    } else {
                        if (battle.defenderPid && p.id === battle.defenderPid) roles.push('Отбивается');
                        if (battle.attackerPid && p.id === battle.attackerPid) roles.push('Ходит');
                        if (
                            ((sub === 'toss' || sub === 'take_toss') && doneEligiblePids.includes(p.id)) ||
                            (sub === 'defend' && neighborTossPids.includes(String(p.id)))
                        ) {
                            if (String(p.id) !== String(battle.defenderPid || '')) roles.push('Подкидывает');
                        }
                    }
                    const roleStr = roles.length ? `<div class="durak-tile-role">${roles.join(' · ')}</div>` : '';
                    const placeBubble = place ? `<div class="durak-place-badge is-bottom">${escapeHtml(placeLabel(place))}</div>` : '';
                    const countText = place ? `${place} место` : `${p.cardCount} карт`;
                    oppHtml += `<div class="durak-player-tile-wrap">${thought}${placeBubble}<div class="durak-player-tile${hi}${defenderClass}${tosserClass}${placeClass}" data-durak-pid="${durakEscapeDataAttr(p.id)}"><div class="durak-tile-name">${escapeHtml(p.name)}</div><div class="durak-tile-count">${countText}</div>${roleStr}</div></div>`;
                });
                const mePl = pl.find((p) => p.id === myId);
                let meRoleLine = '';
                if (mePl) {
                    const mr = [];
                    if (sameAtkDef && battle.defenderPid && mePl.id === battle.defenderPid) mr.push('Отбивается');
                    else {
                        if (battle.defenderPid && mePl.id === battle.defenderPid) mr.push('Отбивается');
                        if (battle.attackerPid && mePl.id === battle.attackerPid) mr.push('Ходит');
                        if (
                            ((sub === 'toss' || sub === 'take_toss') && doneEligiblePids.includes(mePl.id)) ||
                            (sub === 'defend' && neighborTossPids.includes(String(mePl.id)))
                        ) {
                            if (String(mePl.id) !== String(battle.defenderPid || '')) mr.push('Подкидывает');
                        }
                    }
                    if (mr.length) meRoleLine = `<div class="durak-tile-role">${mr.join(' · ')}</div>`;
                }
                const meHi = mePl && highlightId === myId ? ' is-highlight' : '';
                const meDef = mePl && battle.defenderPid && mePl.id === battle.defenderPid ? ' is-defender' : '';
                const meTosser =
                    mePl &&
                    (((sub === 'toss' || sub === 'take_toss') && doneEligiblePids.includes(mePl.id)) ||
                        (sub === 'defend' && neighborTossPids.includes(String(mePl.id))))
                        ? ' is-tosser'
                        : '';
                const meThought = mePl && thoughtPid && String(mePl.id) === thoughtPid ? '<div class="durak-think-bubble">Беру</div>' : '';
                const mePlace = mePl ? placeOf(mePl.id) : 0;
                const mePlaceClass = mePlace > 0 && mePlace <= 3 ? ` is-place-${mePlace}` : '';
                const mePlaceBubble = mePlace ? `<div class="durak-place-badge is-top">${escapeHtml(placeLabel(mePlace))}</div>` : '';
                const meInlineActions =
                    !mobUi && (showTakeBtn || showDoneBtn)
                        ? `<div class="durak-me-inline-actions">${showTakeBtn ? '<button type="button" class="durak-btn-primary" id="durakTake">Беру</button>' : ''}${showDoneBtn ? `<button type="button" class="durak-btn-primary" id="durakDone">${doneLabel}</button>` : ''}</div>`
                        : '';
                const meHtml = mePl
                    ? `<div class="durak-me-strip"><div class="durak-me-tile-row">${meInlineActions}<div class="durak-player-tile-wrap">${meThought}${mePlaceBubble}<div class="durak-player-tile is-me${meHi}${meDef}${meTosser}${mePlaceClass}" data-durak-pid="${durakEscapeDataAttr(mePl.id)}"><div class="durak-tile-name">${escapeHtml(mePl.name)} (вы)</div><div class="durak-tile-count">${mePlace ? `${mePlace} место` : `${mePl.cardCount} карт`}</div>${meRoleLine}</div></div></div></div>`
                    : '';
                const deckN = typeof g.deckCount === 'number' ? g.deckCount : 0;
                const showDeckStack = deckN > 0;
                const trumpSt = g.trumpCard ? durakCardFaceStyle(g.trumpCard) : '';
                const showdownTopRows = isShowdown
                    ? others.map((player) => {
                        const place = placeOf(player.id);
                        const suffix = place ? ` · ${placeLabel(place)}` : '';
                        return `<div class="durak-showdown-player-line">
                            <div class="durak-showdown-player-name">${escapeHtml(player.name)}${suffix ? ` <span style="opacity:.72;">${escapeHtml(suffix)}</span>` : ''}</div>
                            ${renderShowdownCardsRow(showdownHandsMap.get(String(player.id || '')) || [])}
                        </div>`;
                    }).join('')
                    : '';
                const myShowdownRow = isShowdown && mePl
                    ? `<div class="durak-showdown-player-line">
                        <div class="durak-showdown-player-name">Вы${mePlace ? ` <span style="opacity:.72;">${escapeHtml(`· ${placeLabel(mePlace)}`)}</span>` : ''}</div>
                        ${renderShowdownCardsRow((g.myHand && g.myHand.length ? g.myHand : (showdownHandsMap.get(String(myId || '')) || [])))}
                    </div>`
                    : '';
                html += '<div class="durak-stage">';
                html += isShowdown
                    ? `<div class="durak-showdown-side durak-showdown-side--top">${showdownTopRows || '<div class="durak-showdown-player-empty">Нет соперников</div>'}</div>`
                    : `<div class="durak-opponents-ring">${oppHtml}</div>`;
                html += '<div class="durak-middle">';
                html += '<div class="durak-table-board">';
                html += '<div class="durak-deck-column">';
                html += `<div class="durak-deck-count" title="Карт в колоде">${deckN}</div>`;
                if (showDeckStack) {
                    html += '<div class="durak-deck-stack" title="Колода">';
                    html += '<div class="durak-card-face durak-card-back-layer"></div>';
                    html += '<div class="durak-card-face durak-card-back-layer"></div>';
                    html += '<div class="durak-card-face durak-card-back-layer"></div>';
                    html += '</div>';
                } else {
                    html += '<div class="durak-deck-stack durak-deck-empty" title="Колода пуста">';
                    html += '<div class="durak-card-face durak-card-back-layer"></div>';
                    html += '</div>';
                }
                html += '<div class="durak-trump-slot">';
                if (g.trumpCard && trumpSt) {
                    html += '<div class="durak-trump-card-wrap">';
                    html += `<div class="durak-card-face durak-trump-card" style="${trumpSt}"></div>`;
                    html += '</div>';
                }
                html += `<span class="durak-trump-label">Козырь: ${escapeHtml(durakSuitRu(g.trump))}</span>`;
                html += '</div></div>';
                html += `<div class="durak-table" id="durakTable">`;
                (battle.table || []).forEach((row) => {
                    const ts = durakEnsureTransfersClient(row);
                    const hasTr = ts.length > 0;
                    const beat = !!row.defense;
                    const aa = durakBeatAnchorAttr(row, defPid, myId, sub, 'attack');
                    const xferSlot = durakTransferSlotBesideRowHtml(g, row, defPid, myId, sub);
                    const beatMarker = xferSlot ? ' data-durak-beatable="1"' : '';
                    html += `<div class="durak-row-pair ${beat ? 'beat' : ''} ${hasTr ? 'transfer' : ''}">`;
                    if (hasTr) {
                        html += '<div class="durak-row-transfer-beat">';
                        html += `<div class="durak-row-with-transfer"><div class="durak-beat-stack">`;
                        html += `<div class="durak-card-face"${aa}${beatMarker} data-durak-card="${durakEscapeDataAttr(row.attack)}" style="${durakCardFaceStyle(row.attack)}"></div>`;
                        if (row.defense) {
                            html += `<div class="durak-card-face def" data-durak-card="${durakEscapeDataAttr(row.defense)}" style="${durakCardFaceStyle(row.defense)}"></div>`;
                        }
                        html += `</div></div><div class="durak-beat-stack">`;
                        for (const t of ts) {
                            const tb = durakBeatAnchorTransferCard(row, defPid, myId, sub, t.card);
                            html += `<div class="durak-card-face"${tb}${beatMarker} data-durak-card="${durakEscapeDataAttr(t.card)}" style="${durakCardFaceStyle(t.card)}"></div>`;
                            if (t.defense) {
                                html += `<div class="durak-card-face transfer-def" data-durak-card="${durakEscapeDataAttr(t.defense)}" style="${durakCardFaceStyle(t.defense)}"></div>`;
                            }
                        }
                        html += `</div>${xferSlot}</div>`;
                    } else {
                        html += `<div class="durak-row-with-transfer"><div class="durak-beat-stack">`;
                        html += `<div class="durak-card-face"${aa}${beatMarker} data-durak-card="${durakEscapeDataAttr(row.attack)}" style="${durakCardFaceStyle(row.attack)}"></div>`;
                        if (row.defense) {
                            html += `<div class="durak-card-face def" data-durak-card="${durakEscapeDataAttr(row.defense)}" style="${durakCardFaceStyle(row.defense)}"></div>`;
                        }
                        html += `</div>${xferSlot}</div>`;
                    }
                    html += '</div>';
                });
                html += '</div></div></div>';
                if (isShowdown) {
                    html += `<div class="durak-showdown-side durak-showdown-side--bottom">${myShowdownRow || '<div class="durak-showdown-player-empty">Ваши карты отсутствуют</div>'}</div>`;
                } else {
                    html += '<div class="durak-bottom-panel">';
                    html += meHtml;
                    html += '<div class="durak-actions-hand-row">';
                    html += '<div class="durak-actions-col">';
                    if (mobUi && showTakeBtn) {
                        html += '<button type="button" class="durak-btn-primary" id="durakTake">Беру</button>';
                    }
                    if (mobUi && showDoneBtn) {
                        html += `<button type="button" class="durak-btn-primary" id="durakDone">${doneLabel}</button>`;
                    }
                    html +=
                        '<button type="button" class="durak-btn-icon" id="durakLeaveGame" title="Выйти из игры" aria-label="Выйти из игры"><i class="fas fa-sign-out-alt" aria-hidden="true"></i><span class="durak-btn-icon-label">Выйти</span></button>';
                    if (mod) {
                        html +=
                            '<button type="button" class="durak-btn-icon" id="durakEndGame" title="Завершить для всех" aria-label="Завершить для всех"><i class="fas fa-flag-checkered" aria-hidden="true"></i><span class="durak-btn-icon-label">Завершить</span></button>';
                    }
                    html += '</div>';
                    const handMob = mobUi;
                    const tossHint = sub === 'take_toss' && showDoneBtn ? ' is-toss-hint' : '';
                    html += `<div class="durak-hand-wrap${tossHint}"><div class="durak-hand${handMob ? ' durak-hand-mobile' : ''}" id="durakMyHand">`;
                    (g.myHand || []).forEach((cid, idx) => {
                        const st = durakCardFaceStyle(cid);
                        html += `<div class="durak-card-face" draggable="true" data-card="${cid}" style="${st};--deal-i:${idx}" ondragstart="event.dataTransfer.setData('text/plain','${cid}');window.__durakDragCard='${cid}'"></div>`;
                    });
                    html += '</div></div></div></div>';
                }
                html += '</div>';
            }
            html += '</div>';
            const callBarPre = document.querySelector('.call-bottom-bar');
            const togglePre = document.getElementById('durakCallPanelToggle');
            if (ov && callBarPre && togglePre && ov.contains(togglePre)) {
                callBarPre.appendChild(togglePre);
            }
            let durakFlyFromRectSnap = null;
            if (g.phase === 'playing' && ov && g.lastPlay && g.lastPlay.seq && g.lastPlay.cards && g.lastPlay.cards.length) {
                const seenFly = window.__durakLastPlayAnimSeq || 0;
                if (g.lastPlay.seq > seenFly && g.lastPlay.kind !== 'take' && String(g.lastPlay.by) !== String(myId)) {
                    const byFly = String(g.lastPlay.by);
                    const prevRing = ov.querySelector('.durak-opponents-ring');
                    if (prevRing) {
                        const prevTiles = prevRing.querySelectorAll('.durak-player-tile[data-durak-pid]');
                        for (let fi = 0; fi < prevTiles.length; fi++) {
                            if (String(prevTiles[fi].getAttribute('data-durak-pid')) === byFly) {
                                const pr = prevTiles[fi].getBoundingClientRect();
                                if (pr.width >= 4 && pr.height >= 4) {
                                    durakFlyFromRectSnap = { left: pr.left, top: pr.top, width: pr.width, height: pr.height };
                                }
                                break;
                            }
                        }
                    }
                }
            }
            ov.innerHTML = html;
            durakPrepFlyTargetsHidden(ov, g);
            ensureDurakCallPanelToggle();
            ov.style.display = '';
            ensureDurakOverlayDragUi(ov);
            const bind = (id, fn) => {
                const el = document.getElementById(id);
                if (el) el.onclick = fn;
            };
            bind('durakActJoin', () => sendDurak({ type: 'durak-join' }));
            bind('durakActLeaveLobby', () => sendDurak({ type: 'durak-leave' }));
            bind('durakActStart', () => sendDurak({ type: 'durak-start', force: false }));
            bind('durakActCancel', () => sendDurak({ type: 'durak-cancel' }));
            bind('durakTake', () => sendDurak({ type: 'durak-action', action: { type: 'take' } }));
            bind('durakDone', () => sendDurak({ type: 'durak-action', action: { type: 'done' } }));
            bind('durakEndGame', () => sendDurak({ type: 'durak-end' }));
            bind('durakLeaveGame', () => sendDurak({ type: 'durak-leave' }));
            bind('durakCloseEnded', () => {
                durakShowdownDismissed = true;
                renderDurakUi();
            });
            document.querySelectorAll('#durakMyHand .durak-card-face').forEach((el) => {
                el.addEventListener('click', () => {
                    const c = el.getAttribute('data-card');
                    if (durakHandPlayNeedsTableChoice(c)) {
                        showNotification('Дурак', 'Перетащите карту на атакующую карту или в слот перевода', 'info');
                        return;
                    }
                    durakPlayCard(c);
                });
                // Mobile: drag without long-press (touch-drag starts immediately).
                el.addEventListener('touchstart', (e) => {
                    const c = el.getAttribute('data-card');
                    if (!c) return;
                    window.__durakTouchTouchMeta = {
                        card: c,
                        sx: e.touches?.[0]?.clientX || 0,
                        sy: e.touches?.[0]?.clientY || 0,
                        dragging: false,
                        scrolling: false
                    };
                }, { passive: true });
                el.addEventListener('touchmove', (e) => {
                    const meta = window.__durakTouchTouchMeta;
                    const touch = e.touches && e.touches[0];
                    if (!touch) return;
                    if (!meta) return;
                    const dx = touch.clientX - (meta.sx || 0);
                    const dy = touch.clientY - (meta.sy || 0);
                    // Горизонтальный свайп по руке — это скролл списка карт.
                    if (!meta.dragging && !meta.scrolling && Math.abs(dx) > 9 && Math.abs(dx) > Math.abs(dy) * 1.2) {
                        meta.scrolling = true;
                    }
                    if (meta.scrolling) return;
                    // Вертикальный/диагональный сдвиг — старт drag сразу, без long-press.
                    if (!meta.dragging && (Math.abs(dy) > 8 || Math.abs(dx) + Math.abs(dy) > 18)) {
                        meta.dragging = true;
                        window.__durakDragCard = meta.card;
                        durakStartTouchDrag(ov, meta.card, touch);
                    }
                    if (meta.dragging) {
                        durakMoveTouchDrag(ov, touch);
                        e.preventDefault();
                    }
                }, { passive: false });
                el.addEventListener('touchend', (e) => {
                    const meta = window.__durakTouchTouchMeta;
                    window.__durakTouchTouchMeta = null;
                    const card = window.__durakDragCard;
                    window.__durakDragCard = null;
                    const t = e.changedTouches && e.changedTouches[0];
                    const target = t ? document.elementFromPoint(t.clientX, t.clientY) : null;
                    if (meta && meta.dragging) {
                        durakDropCardToTarget(card || meta.card, target);
                        durakClearTouchDragUi(ov);
                        e.preventDefault();
                    }
                }, { passive: false });
                el.addEventListener('touchcancel', () => {
                    window.__durakTouchTouchMeta = null;
                    window.__durakDragCard = null;
                    durakClearTouchDragUi(ov);
                }, { passive: true });
            });
            ov.ondragover = (e) => {
                e.preventDefault();
                if (!ov.classList.contains('durak-dragging-from-hand')) return;
                const handEl = ov.querySelector('.durak-hand-wrap');
                const tableEl = ov.querySelector('#durakTable');
                const overHand = e.target.closest?.('.durak-hand-wrap');
                const overTable = e.target.closest?.('#durakTable');
                const dt = e.dataTransfer;
                if (overHand && handEl) {
                    if (dt) dt.dropEffect = 'copy';
                    handEl.classList.add('durak-drag-over-return');
                    tableEl?.classList.remove('durak-drag-over-play');
                } else if (overTable && tableEl) {
                    if (dt) dt.dropEffect = 'move';
                    tableEl.classList.add('durak-drag-over-play');
                    handEl?.classList.remove('durak-drag-over-return');
                } else {
                    handEl?.classList.remove('durak-drag-over-return');
                    tableEl?.classList.remove('durak-drag-over-play');
                }
            };
            ov.ondrop = durakTableDrop;
            ov.querySelectorAll('[data-durak-drop]').forEach((z) => {
                z.addEventListener('dragover', (e) => {
                    e.preventDefault();
                    z.classList.add('durak-drop-hover');
                });
                z.addEventListener('dragleave', () => z.classList.remove('durak-drop-hover'));
            });
            syncDurakCallScreenClasses(g);
            placeDurakCallPanelToggle();
            if (g.phase === 'playing') {
                const hand = document.getElementById('durakMyHand');
                const bf = typeof g.battlesFinished === 'number' ? g.battlesFinished : 0;
                if (hand && window.__durakDealAnimBattle !== bf) {
                    window.__durakDealAnimBattle = bf;
                    hand.classList.remove('durak-deal-anim');
                    requestAnimationFrame(() => {
                        requestAnimationFrame(() => {
                            hand.classList.add('durak-deal-anim');
                            setTimeout(() => hand.classList.remove('durak-deal-anim'), 780);
                        });
                    });
                }
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => durakRunPlayFlyAnimations(ov, g, durakFlyFromRectSnap));
                });
            } else {
                window.__durakDealAnimBattle = undefined;
            }
            updateEmptyState();
            if (g.phase === 'playing' && g.battle && g.battle.subPhase === 'take_toss') {
                const tickDoneLabel = () => {
                    if (!durakGameState || durakGameState.phase !== 'playing') return;
                    const b = durakGameState.battle || {};
                    if (b.subPhase !== 'take_toss') return;
                    const left = Math.max(0, Math.ceil((Number(durakGameState.turnDeadline || 0) - Date.now()) / 1000));
                    const btn = document.getElementById('durakDone');
                    if (btn) btn.textContent = `Бито (${left})`;
                    if (left <= 0) return;
                    durakUiTickTimer = setTimeout(tickDoneLabel, 300);
                };
                durakUiTickTimer = setTimeout(tickDoneLabel, 300);
            } else if (g.phase === 'showdown') {
                const tickShowdownUi = () => {
                    if (!durakGameState || durakGameState.phase !== 'showdown') return;
                    const left = durakCountdownSeconds(durakGameState.resultDeadline, 30);
                    const topLine = document.getElementById('durakTopbarLine');
                    if (topLine) topLine.textContent = `Итог партии · стол закроется через ${left} сек`;
                    const countdownEl = document.getElementById('durakShowdownCountdown');
                    if (countdownEl) countdownEl.textContent = String(left);
                    if (left <= 0) return;
                    durakUiTickTimer = setTimeout(tickShowdownUi, 250);
                };
                durakUiTickTimer = setTimeout(tickShowdownUi, 250);
            }
        }

        function endCall(playLeaveSound = true) {
            if (playLeaveSound) {
                playSoundEffect(leaveSoundEffect);
            }
            cancelPendingOutgoingFriendCall('caller_left');
            const leavingRoomId = roomId;
            if (leavingRoomId && ws && ws.readyState === WebSocket.OPEN) {
                safeWsSend({ type: 'leave' });
            }
            stopCallTimer();
            clearOutgoingFriendCallSession();
            callMinimized = false;
            cleanupCallMediaResources();
            resetCallState();
            roomId = null;
            isConnected = false;
            currentGroupCallChatId = '';
            currentGroupCallTitle = '';
            connectionNoticeCooldown.clear();
            participantConnectionQuality.clear();
            durakGameState = null;
            applyDurakFocusMode(false);
            syncDurakCallScreenClasses(null);
            applyWatchFocusMode(false);
            wsLastInitialMsg = null;
            teardownActiveWsSocket();
            history.replaceState(null, '', getBasePath());
            syncCallScreenLayoutMode();
            ensureMessengerWsConnection();
            renderMainScreen();
            const appEl = document.getElementById('app');
            if (appEl) {
                appEl.style.display = '';
                appEl.style.pointerEvents = '';
            }
            showNotification('Звонок', 'Вы покинули комнату', 'info');
        }

        function syncCallScreenLayoutMode() {
            const root = document.getElementById('callScreenRoot');
            const appEl = document.getElementById('app');
            if (!roomId) {
                if (root) root.style.display = 'none';
                if (appEl) {
                    appEl.style.display = '';
                    appEl.style.pointerEvents = '';
                }
                const isl = document.getElementById('callIsland');
                if (isl) isl.remove();
                return;
            }
            if (root) {
                root.classList.remove('call-screen--pip');
                root.style.cursor = '';
                root.onclick = null;
                root.style.display = callMinimized ? 'none' : '';
            }
            if (appEl) {
                if (callMinimized) {
                    appEl.style.display = '';
                    appEl.style.pointerEvents = '';
                } else {
                    appEl.style.display = 'none';
                    appEl.style.pointerEvents = 'none';
                }
            }
            renderCallIslandWidget();
        }

        function renderCallScreen() {
            let root = document.getElementById('callScreenRoot');
            if (!root) {
                root = document.createElement('div');
                root.id = 'callScreenRoot';
                document.body.appendChild(root);
            }
            if (!root.dataset.callBuilt) {
                root.className = 'call-screen';
                root.innerHTML = `
                <button type="button" class="ctrl-btn call-minimize-fab" onclick="minimizeCallToIsland()" title="Свернуть">
                    <i class="fas fa-comment-dots"></i>
                    <span id="callUnreadBadge" class="call-unread-badge" style="display:none;">0</span>
                </button>
                    <div class="call-topbar" id="callTopbar">
                        <i class="fas fa-phone-alt"></i>
                        <span class="call-timer" id="callTimer">00:00</span>
                        <span id="roomPrivacyBadge" class="room-status ${roomIsPrivate ? 'private' : 'public'}" title="${roomIsPrivate ? 'Закрытая' : 'Публичная'}"><i class="fas ${roomIsPrivate ? 'fa-lock' : 'fa-globe'}"></i></span>
                    </div>
                    <div class="videos" id="videosContainer">
                        <div class="waiting" id="waitingMsg">
                            <h3><i class="fas fa-clock"></i> Ожидание подключения...</h3>
                            <p>${isCreator ? 'Отправьте ID другу' : 'Ожидаем создателя комнаты'}</p>
                        </div>
                    </div>
                    <div class="participants-panel">
                        <div class="participants-header">
                            <span style="display:flex;align-items:center;gap:8px;">
                                <button id="participantsCloseBtn" class="close-participants" style="position:static;display:${isMobileLayout() ? 'inline-flex' : 'none'};" onclick="closeParticipantsPanel()"><i class="fas fa-times"></i></button>
                                <i class="fas fa-users"></i> Участники
                            </span>
                            <div style="display:flex;align-items:center;gap:8px;">
                                <button id="roomSettingsBtn" class="close-participants" style="position:static;width:32px;height:32px;display:${canManageRoom() ? 'inline-flex' : 'none'};" onclick="showRoomSettingsMenu(event)"><i class="fas fa-ellipsis-v"></i></button>
                            </div>
                        </div>
                        <div class="participants-list" id="participantsList"></div>
                    </div>
                    <div class="toggle-participants" onclick="toggleParticipantsPanel()">
                        <i class="fas fa-users"></i>
                    </div>
                    <div class="copy-id" onclick="copyRoomId()" title="${shouldCopyTelegramInvite() ? 'Скопировать ссылку' : 'Скопировать ID'}"><i id="copyInviteIcon" class="fas ${shouldCopyTelegramInvite() ? 'fa-link' : 'fa-id-card'}"></i></div>
                    <button type="button" class="call-settings-btn" onclick="showCallSettingsModal()" title="Настройки звонка"><i class="fas fa-ellipsis-v"></i></button>
                    <div class="call-bottom-bar">
                        <button type="button" class="ctrl-btn" id="durakBtn" title="Дурак" onclick="onDurakToolbarClick()"><i class="fas fa-gamepad"></i></button>
                        <div class="controls">
                        <button class="ctrl-btn active" id="videoBtn" onclick="toggleVideo()"><i class="fas fa-video"></i></button>
                        <button class="ctrl-btn flip" id="flipCameraBtn" onclick="switchCameraFacingMode()" style="display:none"><i class="fas fa-sync-alt"></i></button>
                        <button class="ctrl-btn active" id="audioBtn" onclick="toggleAudio()"><i class="fas fa-microphone"></i></button>
                        <button class="ctrl-btn screen" id="screenBtn" onclick="startScreenShare()"><i class="fas fa-desktop"></i></button>
                        <button class="ctrl-btn watch" id="watchBtn" onclick="showWatchPartyModal()"><i class="fas fa-users-viewfinder"></i></button>
                        <button class="ctrl-btn watch-stop" id="stopWatchBtn" onclick="stopWatchParty()" style="display:none"><i class="fas fa-stop"></i></button>
                        <button class="ctrl-btn end" onclick="endCall()"><i class="fas fa-phone-slash"></i></button>
                        </div>
                        <button type="button" id="durakCallPanelToggle" class="durak-call-panel-toggle" onclick="toggleDurakCallPanel()" title="Панель звонка"><i class="fas fa-chevron-up"></i></button>
                    </div>`;
                root.dataset.callBuilt = '1';
            }
            updateCallMinimizeUnreadBadge();
            syncCallScreenLayoutMode();
            applyWatchFocusMode(false);
            applyCallScreenPerformanceMode();
            updateParticipantsResponsiveUI();
            primeCallAudioSession();
            updateUI();
            updateEmptyState();
            setTimeout(() => ensureDurakControlButton(), 0);
        }

        function showJoinModal() {
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.innerHTML = `
                <div class="modal-content">
                    <h2><i class="fas fa-link"></i> Подключиться</h2>
                    <input type="text" id="roomInput" class="modal-input" placeholder="Вставьте ссылку или ID комнаты">
                    <div class="modal-buttons">
                        <button class="modal-btn cancel" id="modalCancel">Отмена</button>
                        <button class="modal-btn confirm" id="modalConfirm">Подключиться</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            
            document.getElementById('modalConfirm').onclick = () => {
                const parsedRoomId = parseRoomInput(document.getElementById('roomInput').value);
                if (parsedRoomId) { modal.remove(); joinRoom(parsedRoomId); }
                else showNotification('Ошибка', 'Укажите корректную ссылку или ID вида id123', 'error');
            };
            document.getElementById('modalCancel').onclick = () => {
                modal.remove();
            };
        }

        function setFriendsTab(tab) {
            friendsActiveTab = tab === 'requests' ? 'requests' : 'friends';
            renderMainScreen();
        }

        function toggleFriendsHomePanel() {
            friendsPanelOpenMobile = !friendsPanelOpenMobile;
            renderMainScreen();
        }

        function closeFriendsHomePanel() {
            friendsPanelOpenMobile = false;
            renderMainScreen();
        }

        function onFriendsSearchInput(event) {
            friendsSearchValue = String(event?.target?.value || '');
            clearTimeout(friendsSearchDebounceTimer);
            friendsSearchDebounceTimer = setTimeout(() => {
                friendsSearchDebounceTimer = null;
                searchFriendsUsers();
            }, 400);
        }

        function copyAppUserId() {
            if (!authProfile?.appUserId) return;
            navigator.clipboard.writeText(authProfile.appUserId);
            showNotification('Друзья', 'ID аккаунта скопирован', 'success');
        }

        function showFriendsSettingsMenu(event) {
            event.preventDefault();
            event.stopPropagation();
            const menu = document.createElement('div');
            menu.className = 'context-menu';
            menu.innerHTML = `
                <div class="context-item" onclick="persistFriendsNotifyValue(${!friendsNotificationsEnabled})">
                    <i class="fas ${friendsNotificationsEnabled ? 'fa-toggle-on' : 'fa-toggle-off'}"></i>
                    Push уведомления: ${friendsNotificationsEnabled ? 'Вкл' : 'Выкл'}
                </div>
            `;
            document.body.appendChild(menu);
            const rect = event.currentTarget.getBoundingClientRect();
            placeContextMenu(menu, rect.right - menu.offsetWidth, rect.bottom + 8, rect.top - 8);
            setTimeout(() => document.addEventListener('click', () => menu.remove(), { once: true }), 0);
        }

        function buildFriendItemRow(user, actionsHtml) {
            const label = user?.displayName || user?.name || user?.id || '';
            const name = escapeHtml(label);
            const id = escapeHtml(user?.id || '');
            const rawId = String(user?.id || '').replace(/'/g, "\\'");
            const avatar = avatarMarkup(label, user?.avatar || '', user?.initials);
            return `
                <div class="contact-item" onclick="openUserProfile('${rawId}')" style="cursor:pointer;">
                    <div class="participant-info">
                        <div class="participant-avatar" style="width:36px;height:36px;min-width:36px">${avatar}</div>
                        <div>
                            <div class="contact-name">${name}</div>
                            <div class="contact-chat">ID: ${id}</div>
                        </div>
                    </div>
                    <div class="contact-actions" onclick="event.stopPropagation()">${actionsHtml}</div>
                </div>
            `;
        }

        function renderFriendsTabContent() {
            const query = friendsSearchValue.trim();
            const friends = Array.isArray(friendsState.friends) ? friendsState.friends : [];
            const incomingRequests = Array.isArray(friendsState.incomingRequests) ? friendsState.incomingRequests : [];
            let html = '<div class="contacts-list">';
            if (query) {
                const results = Array.isArray(friendsSearchResults) ? friendsSearchResults : [];
                if (!results.length) {
                    html += `<div class="friends-empty">По вашему запросу никто не найден</div>`;
                } else {
                    results.forEach((result) => {
                        let actionsHtml = `<button class="contact-btn" title="Профиль" onclick="openUserProfile('${result.id}')"><i class="fas fa-user"></i></button>`;
                        if (result.isFriend) {
                            actionsHtml += `<button class="contact-btn" title="Добавить в чат" onclick="openAddUserToGroupModal('${result.id}')"><i class="fas fa-comments"></i></button>`;
                        } else if (result.outgoingPending) {
                            actionsHtml += '<button class="contact-btn secondary" title="Запрос отправлен"><i class="fas fa-clock"></i></button>';
                        } else if (result.incomingPending) {
                            const req = incomingRequests.find((item) => item.fromId === result.id);
                            if (req?.requestId) {
                                actionsHtml += `<button class="contact-btn" title="Принять" onclick="handleFriendRequest('${req.requestId}','accept')"><i class="fas fa-check"></i></button>`;
                            } else {
                                actionsHtml += '<button class="contact-btn secondary" title="Входящий запрос"><i class="fas fa-inbox"></i></button>';
                            }
                        } else {
                            actionsHtml += `<button class="contact-btn" title="Написать" onclick="openMessengerChat('${result.id}')"><i class="fas fa-paper-plane"></i></button>`;
                            actionsHtml += `<button class="contact-btn" title="Добавить" onclick="sendFriendRequest('${result.id}')"><i class="fas fa-user-plus"></i></button>`;
                        }
                        html += buildFriendItemRow(result, actionsHtml);
                    });
                }
                html += '</div>';
                return html;
            }
            if (!friends.length) {
                html += `<div class="friends-empty">Пока нет друзей. Используйте поиск по ID аккаунта.</div>`;
            } else {
                friends.forEach((friend) => {
                    const actions = `
                        <button class="contact-btn" title="Профиль" onclick="openUserProfile('${friend.id}')"><i class="fas fa-user"></i></button>
                        <button class="contact-btn delete" title="Удалить" onclick="deleteFriend('${friend.id}')"><i class="fas fa-user-times"></i></button>
                    `;
                    html += buildFriendItemRow(friend, actions);
                });
            }
            html += '</div>';
            return html;
        }

        function renderRequestsTabContent() {
            const incoming = Array.isArray(friendsState.incomingRequests) ? friendsState.incomingRequests : [];
            const outgoing = Array.isArray(friendsState.outgoingRequests) ? friendsState.outgoingRequests : [];
            const incomingCalls = Array.isArray(friendsState.incomingCalls) ? friendsState.incomingCalls : [];
            let html = '<div class="contacts-list">';
            if (!incoming.length && !outgoing.length && !incomingCalls.length) {
                html += `<div class="friends-empty">Нет активных запросов</div>`;
            }
            incomingCalls.forEach((invite) => {
                const actions = `
                    <button class="contact-btn" onclick="replyIncomingCall('${invite.inviteId}','answer')">Ответить</button>
                    <button class="contact-btn delete" onclick="replyIncomingCall('${invite.inviteId}','decline')">Сбросить</button>
                `;
                html += buildFriendItemRow({ id: invite.fromId, name: invite.fromName, avatar: invite.fromAvatar }, actions);
            });
            incoming.forEach((request) => {
                const actions = `
                    <button class="contact-btn" onclick="handleFriendRequest('${request.requestId}','accept')">Принять</button>
                    <button class="contact-btn delete" onclick="handleFriendRequest('${request.requestId}','decline')">Отклонить</button>
                `;
                html += buildFriendItemRow({ id: request.fromId, name: request.name, avatar: request.avatar }, actions);
            });
            outgoing.forEach((request) => {
                const actions = '<button class="contact-btn secondary">Ожидает ответа</button>';
                html += buildFriendItemRow({ id: request.toId, name: request.name, avatar: request.avatar }, actions);
            });
            html += '</div>';
            return html;
        }

        function getMessengerSocketReady() {
            return !!ws && ws.readyState === WebSocket.OPEN;
        }

        function getMessengerConnectionState() {
            if (typeof navigator !== 'undefined' && navigator.onLine === false) return 'offline';
            if (wsReconnectInProgress) return 'connecting';
            if (ws && ws.readyState === WebSocket.CONNECTING) return 'connecting';
            if (getMessengerSocketReady()) return 'online';
            if (!authProfile) return 'offline';
            return 'connecting';
        }

        function getMessengerSidebarBrandLabel() {
            const state = getMessengerConnectionState();
            if (state === 'online') return 'Seych';
            if (state === 'offline') return 'Нет сети';
            return 'Соединение...';
        }

        function updateMessengerSidebarStatus() {
            const brand = document.querySelector('.sidebar-brand');
            if (brand) {
                const state = getMessengerConnectionState();
                brand.classList.toggle('online', state === 'online');
                brand.classList.toggle('connecting', state === 'connecting');
                brand.classList.toggle('offline', state === 'offline');
                brand.textContent = getMessengerSidebarBrandLabel();
            }
        }

        function sendMessengerEvent(payload) {
            if (!payload) return false;
            if (getMessengerSocketReady()) {
                try {
                    ws.send(JSON.stringify(payload));
                    return true;
                } catch (_) {
                    return false;
                }
            }
            pendingMessengerEvents.push(payload);
            return true;
        }

        function flushPendingMessengerEvents() {
            if (!getMessengerSocketReady() || !pendingMessengerEvents.length) return;
            const queue = pendingMessengerEvents.slice();
            pendingMessengerEvents = [];
            queue.forEach((payload) => {
                try { ws.send(JSON.stringify(payload)); } catch (_) {}
            });
        }

        function syncMessengerIdentity() {
            if (!authProfile?.appUserId) return;
            sendMessengerEvent({
                type: 'messenger-register',
                appUserId: authProfile.appUserId,
                deviceSessionId: getDeviceSessionId(),
                userName: authProfile.name || userName || '',
                userAvatar: authProfile.avatar || '',
                username: ensureGeneratedMessengerUsername(messengerProfile.username || authProfile.vkUsername || '', authProfile.appUserId),
                statusText: messengerProfile.statusText || '',
                privacy: messengerProfile.privacy,
                blacklist: messengerProfile.blacklist
            });
            sendMessengerEvent({ type: 'messenger-sync' });
            if (messengerActiveChatId) {
                sendMessengerEvent({ type: 'messenger-open-chat', chatId: messengerActiveChatId });
            }
            // Load stories after registration
            setTimeout(() => loadStories(), 1000);
        }

        function setMessengerView(view) {
            messengerView = view;
            mobileNavDrawerOpen = false;
            if (view === 'chats') {
                isChatOpen = false;
            }
            if (view === 'notifications') {
                markMessengerNotificationsRead();
            }
            if (view === 'profile') {
                messengerViewedProfile = null;
            }
            if (view === 'calls' && roomId) {
                restoreCallFromIsland();
                return;
            }
            renderMainScreen();
        }

        function getStoredMessengerProfile() {
            try {
                const raw = localStorage.getItem('seych-messenger-profile');
                const parsed = raw ? JSON.parse(raw) : {};
                return {
                    username: ensureGeneratedMessengerUsername(String(parsed?.username || authProfile?.vkUsername || '').replace(/^@+/, '').trim(), authProfile?.appUserId || appUserId),
                    statusText: String(parsed?.statusText || '').trim(),
                    privacy: {
                        canWrite: ['all', 'friends', 'nobody'].includes(parsed?.privacy?.canWrite) ? parsed.privacy.canWrite : 'all',
                        canCall: ['all', 'friends', 'nobody'].includes(parsed?.privacy?.canCall) ? parsed.privacy.canCall : 'all',
                        canViewProfile: ['all', 'friends', 'nobody'].includes(parsed?.privacy?.canViewProfile) ? parsed.privacy.canViewProfile : 'all',
                        canSeeStories: ['all', 'friends', 'nobody'].includes(parsed?.privacy?.canSeeStories) ? parsed.privacy.canSeeStories : 'friends',
                        canJoinGroups: ['all', 'friends', 'nobody'].includes(parsed?.privacy?.canJoinGroups) ? parsed.privacy.canJoinGroups : 'friends'
                    },
                    blacklist: Array.isArray(parsed?.blacklist) ? parsed.blacklist.map((v) => String(v || '').trim()).filter(Boolean) : []
                };
            } catch (_) {
                return { username: '', statusText: '', privacy: { canWrite: 'all', canCall: 'all', canViewProfile: 'all', canSeeStories: 'friends', canJoinGroups: 'friends' }, blacklist: [] };
            }
        }

        function persistMessengerProfileLocal() {
            localStorage.setItem('seych-messenger-profile', JSON.stringify(messengerProfile));
        }

        function compressImageToJpegDataUrl(file, maxDim, quality) {
            return new Promise((resolve, reject) => {
                const r = new FileReader();
                r.onload = () => {
                    const img = new Image();
                    img.onload = () => {
                        let w = img.naturalWidth || img.width;
                        let h = img.naturalHeight || img.height;
                        const scale = w && h ? Math.min(1, maxDim / Math.max(w, h)) : 1;
                        w = Math.max(1, Math.round(w * scale));
                        h = Math.max(1, Math.round(h * scale));
                        const c = document.createElement('canvas');
                        c.width = w;
                        c.height = h;
                        const ctx = c.getContext('2d');
                        if (!ctx) {
                            reject(new Error('canvas'));
                            return;
                        }
                        ctx.drawImage(img, 0, 0, w, h);
                        resolve(c.toDataURL('image/jpeg', quality));
                    };
                    img.onerror = () => reject(new Error('image'));
                    img.src = r.result;
                };
                r.onerror = () => reject(new Error('read'));
                r.readAsDataURL(file);
            });
        }

        let profileUsernameLastChecked = '';
        let profileUsernameLastAvailable = true;

        function scheduleProfileUsernameCheck(value) {
            const username = normalizeMessengerUsernameValue(value);
            const statusEl = document.getElementById('profileUsernameStatus');
            if (profileUsernameCheckTimer) {
                clearTimeout(profileUsernameCheckTimer);
                profileUsernameCheckTimer = 0;
            }
            if (!statusEl) return;
            if (!username) {
                profileUsernameLastChecked = '';
                profileUsernameLastAvailable = true;
                statusEl.dataset.state = 'idle';
                statusEl.textContent = 'Введите username';
                return;
            }
            statusEl.dataset.state = 'idle';
            statusEl.textContent = 'Проверяем username...';
            profileUsernameCheckTimer = setTimeout(() => {
                profileUsernameCheckTimer = 0;
                profileUsernameLastChecked = username.toLowerCase();
                sendMessengerEvent({ type: 'messenger-check-username', username });
            }, 260);
        }

        function openProfileEditModal() {
            let pendingAvatar = null;
            let pendingCover = null;
            const initialAvatar = String(authProfile?.avatar || '').trim();
            const initialCover = String(authProfile?.coverUrl || '').trim();
            const initialName = String(authProfile?.name || '').trim();
            const initialUsername = ensureGeneratedMessengerUsername(messengerProfile.username || authProfile?.vkUsername || '', authProfile?.appUserId || appUserId);
            const initialStatus = String(messengerProfile.statusText || '').trim();
            const initials = (String(authProfile?.initials || '').trim() || String(initialName || authProfile?.appUserId || 'U')
                .split(/\s+/)
                .filter(Boolean)
                .map((part) => part.charAt(0))
                .join('')
                .slice(0, 2)
                .toUpperCase()) || 'U';
            profileUsernameLastChecked = initialUsername.toLowerCase();
            profileUsernameLastAvailable = true;

            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.innerHTML = `
                <div class="modal-content modal-sheet modal-sheet--pc-dialog profile-edit-modal">
                    <div class="modal-sheet-header">
                        <div class="modal-sheet-title"><i class="fas fa-user-edit"></i><span>Редактировать профиль</span></div>
                        <button type="button" class="modal-sheet-close" id="profileModalCancel" aria-label="Закрыть"><i class="fas fa-times"></i></button>
                    </div>
                    <div class="modal-sheet-body">
                        <div class="profile-edit-cover" style="height:220px;margin-bottom:86px;">
                            <div id="profileCoverPreview" class="profile-edit-cover-media"></div>
                            <div class="profile-edit-cover-overlay"></div>
                            <div class="profile-edit-avatar-dock">
                                <div class="profile-edit-avatar-wrap">
                                    <div id="profileAvatarPreview" class="profile-edit-avatar-core"></div>
                                </div>
                                <div class="profile-edit-avatar-actions" style="flex-wrap:wrap;">
                                    <button type="button" class="profile-edit-icon-btn" id="profileAvatarPick" title="Сменить аватар">
                                        <i class="fas fa-camera"></i><span>Сменить аватар</span>
                                    </button>
                                    <button type="button" class="profile-edit-icon-btn delete" id="profileAvatarRemove" title="Удалить аватар">
                                        <i class="fas fa-trash"></i><span>Удалить</span>
                                    </button>
                                </div>
                            </div>
                            <div class="profile-edit-cover-actions">
                                <button type="button" class="profile-edit-icon-btn" id="profileCoverPick" title="Сменить обложку">
                                    <i class="fas fa-image"></i><span>Сменить обложку</span>
                                </button>
                                <button type="button" class="profile-edit-icon-btn delete" id="profileCoverRemove" title="Удалить обложку">
                                    <i class="fas fa-times"></i><span>Удалить</span>
                                </button>
                            </div>
                        </div>
                        <input type="file" id="profileAvatarInput" accept="image/*" style="display:none">
                        <input type="file" id="profileCoverInput" accept="image/*" style="display:none">
                        <div class="profile-edit-fields">
                            <label class="profile-field-label" for="profileNameInput">Имя</label>
                            <input id="profileNameInput" class="modal-input" placeholder="Имя" maxlength="120" value="${escapeHtml(initialName)}" style="text-align:left;">
                            <label class="profile-field-label" for="profileUsernameInput">Username</label>
                            <input id="profileUsernameInput" class="modal-input" placeholder="username" maxlength="50" value="${escapeHtml(initialUsername)}" style="text-align:left;">
                            <div id="profileUsernameStatus" class="profile-username-status" data-state="idle">Введите username</div>
                            <label class="profile-field-label" for="profileStatusInput">Описание</label>
                            <textarea id="profileStatusInput" class="modal-input" placeholder="Описание" maxlength="160" style="min-height:120px;resize:vertical;text-align:left;">${escapeHtml(initialStatus)}</textarea>
                        </div>
                    </div>
                    <div class="modal-buttons" style="flex-shrink:0;">
                        <button class="modal-btn cancel" id="profileModalBack">Отмена</button>
                        <button class="modal-btn confirm" id="profileModalSave">Сохранить</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);

            const avatarPreviewEl = document.getElementById('profileAvatarPreview');
            const coverPreviewEl = document.getElementById('profileCoverPreview');
            const avatarInputEl = document.getElementById('profileAvatarInput');
            const coverInputEl = document.getElementById('profileCoverInput');
            const usernameInputEl = document.getElementById('profileUsernameInput');
            const usernameStatusEl = document.getElementById('profileUsernameStatus');

            const avatarPickBtn = document.getElementById('profileAvatarPick');
            const avatarRemoveBtn = document.getElementById('profileAvatarRemove');
            const coverPickBtn = document.getElementById('profileCoverPick');
            const coverRemoveBtn = document.getElementById('profileCoverRemove');

            const syncMediaActionButtons = () => {
                const hasAvatar = !!String(pendingAvatar != null ? pendingAvatar : initialAvatar).trim();
                const hasCover = !!String(pendingCover != null ? pendingCover : initialCover).trim();
                if (avatarPickBtn) avatarPickBtn.querySelector('span').textContent = hasAvatar ? 'Сменить аватар' : 'Загрузить аватар';
                if (coverPickBtn) coverPickBtn.querySelector('span').textContent = hasCover ? 'Сменить обложку' : 'Загрузить обложку';
                if (avatarRemoveBtn) avatarRemoveBtn.style.display = hasAvatar ? '' : 'none';
                if (coverRemoveBtn) coverRemoveBtn.style.display = hasCover ? '' : 'none';
            };

            const renderAvatarPreview = (url) => {
                if (!avatarPreviewEl) return;
                if (url) {
                    avatarPreviewEl.innerHTML = `<img src="${escapeHtml(url)}" alt="" referrerpolicy="no-referrer">`;
                } else {
                    avatarPreviewEl.textContent = initials;
                }
            };

            const renderCoverPreview = (coverUrl, avatarUrl) => {
                if (!coverPreviewEl) return;
                const style = profileCoverBackgroundStyle(coverUrl, avatarUrl);
                coverPreviewEl.className = `profile-edit-cover-media ${String(coverUrl || '').trim() ? '' : 'is-fallback'}`.trim();
                coverPreviewEl.setAttribute('style', style || '');
            };

            renderAvatarPreview(initialAvatar);
            renderCoverPreview(initialCover, initialAvatar);
            syncMediaActionButtons();
            if (usernameStatusEl) {
                usernameStatusEl.dataset.state = initialUsername ? 'ok' : 'idle';
                usernameStatusEl.textContent = initialUsername ? 'Username свободен' : 'Введите username';
            }

            document.getElementById('profileAvatarPick').onclick = () => avatarInputEl && avatarInputEl.click();
            document.getElementById('profileCoverPick').onclick = () => coverInputEl && coverInputEl.click();
            document.getElementById('profileAvatarRemove').onclick = () => {
                pendingAvatar = '';
                renderAvatarPreview('');
                renderCoverPreview(pendingCover != null ? pendingCover : initialCover, '');
                syncMediaActionButtons();
            };
            document.getElementById('profileCoverRemove').onclick = () => {
                pendingCover = '';
                renderCoverPreview('', pendingAvatar != null ? pendingAvatar : initialAvatar);
                syncMediaActionButtons();
            };

            avatarInputEl.onchange = async () => {
                const file = avatarInputEl.files && avatarInputEl.files[0];
                if (!file || !/^image\//i.test(file.type || '')) return;
                try {
                    pendingAvatar = await compressImageToJpegDataUrl(file, 512, 0.85);
                    renderAvatarPreview(pendingAvatar);
                    renderCoverPreview(pendingCover != null ? pendingCover : initialCover, pendingAvatar);
                    syncMediaActionButtons();
                } catch (_) {
                    showNotification('Аватар', 'Не удалось обработать изображение', 'warning');
                }
                avatarInputEl.value = '';
            };

            coverInputEl.onchange = async () => {
                const file = coverInputEl.files && coverInputEl.files[0];
                if (!file || !/^image\//i.test(file.type || '')) return;
                try {
                    pendingCover = await compressImageToJpegDataUrl(file, 1600, 0.82);
                    renderCoverPreview(pendingCover, pendingAvatar != null ? pendingAvatar : initialAvatar);
                    syncMediaActionButtons();
                } catch (_) {
                    showNotification('Обложка', 'Не удалось обработать изображение', 'warning');
                }
                coverInputEl.value = '';
            };

            usernameInputEl.addEventListener('input', () => {
                const clean = normalizeMessengerUsernameValue(usernameInputEl.value || '');
                if (usernameInputEl.value !== clean) usernameInputEl.value = clean;
                scheduleProfileUsernameCheck(clean);
            });

            document.getElementById('profileModalCancel').onclick = () => {
                if (profileUsernameCheckTimer) clearTimeout(profileUsernameCheckTimer);
                modal.remove();
            };

            document.getElementById('profileModalBack').onclick = () => {
                if (profileUsernameCheckTimer) clearTimeout(profileUsernameCheckTimer);
                modal.remove();
            };

            document.getElementById('profileModalSave').onclick = () => {
                const name = String(document.getElementById('profileNameInput')?.value || '').trim() || authProfile?.name || 'Пользователь';
                const username = normalizeMessengerUsernameValue(usernameInputEl?.value || '');
                const statusText = String(document.getElementById('profileStatusInput')?.value || '').trim();
                const normalizedChecked = String(profileUsernameLastChecked || '').trim();
                if (username && normalizedChecked === username.toLowerCase() && !profileUsernameLastAvailable) {
                    showNotification('Username', 'Этот username уже занят', 'warning');
                    usernameInputEl?.focus();
                    return;
                }
                const avatarOut = pendingAvatar != null ? pendingAvatar : initialAvatar;
                const coverOut = pendingCover != null ? pendingCover : initialCover;
                saveProfile({ ...authProfile, name, vkUsername: username, avatar: avatarOut, coverUrl: coverOut });
                messengerProfile.username = username;
                messengerProfile.statusText = statusText;
                persistMessengerProfileLocal();
                sendMessengerEvent({ type: 'messenger-update-profile', name, username, statusText, avatar: avatarOut, coverUrl: coverOut });
                if (profileUsernameCheckTimer) clearTimeout(profileUsernameCheckTimer);
                modal.remove();
                renderMainScreen();
            };
        }

        function setPrivacyRule(kind, value) {
            const safe = ['all', 'friends', 'nobody'].includes(value) ? value : 'all';
            if (!messengerProfile.privacy) messengerProfile.privacy = { canWrite: 'all', canCall: 'all', canViewProfile: 'all', canSeeStories: 'friends', canJoinGroups: 'friends' };
            messengerProfile.privacy[kind] = safe;
            persistMessengerProfileLocal();
            // Обновить UI всех открытых privacy dropdowns
            document.querySelectorAll('.privacy-dd-panel').forEach(panel => {
                const trigger = panel.previousElementSibling;
                if (trigger && trigger.classList.contains('privacy-dd-trigger')) {
                    // Найдём, относится ли эта панель к данному kind
                    const opts = panel.querySelectorAll('.privacy-dd-opt');
                    let kindMatches = false;
                    opts.forEach(opt => {
                        const optKind = opt.getAttribute('data-kind');
                        const optValue = opt.getAttribute('data-value');
                        if (optKind === kind) {
                            kindMatches = true;
                            if (optValue === safe) opt.classList.add('active');
                            else opt.classList.remove('active');
                        }
                    });
                    if (kindMatches) {
                        const labels = { all: 'Все', friends: 'Друзья', nobody: 'Никто' };
                        trigger.innerHTML = `${labels[safe] || safe} <i class="fas fa-chevron-down"></i>`;
                    }
                }
            });
            sendMessengerEvent({
                type: 'messenger-update-privacy',
                canWrite: messengerProfile.privacy.canWrite,
                canCall: messengerProfile.privacy.canCall,
                canViewProfile: messengerProfile.privacy.canViewProfile,
                canSeeStories: messengerProfile.privacy.canSeeStories,
                canJoinGroups: messengerProfile.privacy.canJoinGroups
            });
            renderMainScreen();
        }

        function togglePrivacyDropdown(triggerBtn, event) {
            if (event) event.stopPropagation();
            const panel = triggerBtn && triggerBtn.nextElementSibling;
            if (!panel) return;
            const willOpen = !panel.classList.contains('open');
            document.querySelectorAll('.privacy-dd-panel').forEach((p) => p.classList.remove('open'));
            if (willOpen) panel.classList.add('open');
        }

        function renderPrivacyDropdown(kindKey, currentVal) {
            const safe = ['all', 'friends', 'nobody'].includes(currentVal) ? currentVal : 'all';
            const labels = { all: 'Все', friends: 'Друзья', nobody: 'Никто' };
            const opts = ['all', 'friends', 'nobody']
                .map(
                    (v) =>
                        `<button type="button" class="privacy-dd-opt ${v === safe ? 'active' : ''}" data-kind="${escapeHtml(kindKey)}" data-value="${escapeHtml(v)}" onclick="setPrivacyRule('${escapeHtml(kindKey)}','${escapeHtml(v)}')">${labels[v]}</button>`
                )
                .join('');
            return `<div class="privacy-dd"><button type="button" class="privacy-dd-trigger" onclick="togglePrivacyDropdown(this, event)">${labels[safe]} <i class="fas fa-chevron-down"></i></button><div class="privacy-dd-panel">${opts}</div></div>`;
        }

        function removeUserFromBlacklist(userId) {
            const id = String(userId || '').trim();
            messengerProfile.blacklist = (messengerProfile.blacklist || []).filter((item) => item !== id);
            persistMessengerProfileLocal();
            sendMessengerEvent({ type: 'messenger-block-user', targetUserId: id, blocked: false });
            renderMainScreen();
        }

        function toggleBlockActivePeer() {
            const peerId = String(messengerActivePeerId || '').trim();
            if (!peerId) return;
            const current = new Set(messengerProfile.blacklist || []);
            const blocked = !current.has(peerId);
            if (blocked) current.add(peerId);
            else current.delete(peerId);
            messengerProfile.blacklist = Array.from(current);
            persistMessengerProfileLocal();
            sendMessengerEvent({ type: 'messenger-block-user', targetUserId: peerId, blocked, comment: '' });
            // На мобиле всегда открываем рабочее окно чата,
            // иначе из-за классов верстки workspace может скрыться.
            if (isMobileLayout()) isChatOpen = true;
            openMessengerChat(peerId);
        }

        function clearComposerReplyEdit() {
            composerReplyMessage = null;
            composerEditMessageId = '';
            renderMainScreen();
        }

        function openForwardModal(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            if (!row) return;
            const modal = document.createElement('div');
            modal.className = 'modal';
            const list = messengerChats
                .filter((chat) => String(chat?.id || '') !== String(messengerActiveChatId || ''))
                .map((chat) => `<button class="contact-btn" style="width:100%;margin-bottom:8px;" onclick="forwardMessageToChat('${escapeHtml(row.id)}','${escapeHtml(chat.id)}')">${escapeHtml(chat.peer?.name || chat.peer?.displayName || chat.id)}</button>`)
                .join('') || '<div class="friends-empty">Нет доступных чатов</div>';
            modal.innerHTML = `<div class="modal-content"><h2><i class="fas fa-share"></i> Переслать</h2>${list}<div class="modal-buttons"><button class="modal-btn cancel" onclick="this.closest('.modal').remove()">Закрыть</button></div></div>`;
            document.body.appendChild(modal);
        }

        function forwardMessageToChat(messageId, targetChatId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            const targetChat = findMessengerChatById(targetChatId);
            if (!row || !targetChat) return;
            const payload = {
                type: 'messenger-send',
                chatId: targetChat.id,
                text: '',
                forwardedFromMessageId: row.id || ''
            };
            if (isDirectMessengerChat(targetChat)) payload.toUserId = targetChat.peer?.id || '';
            sendMessengerEvent(payload);
            document.querySelectorAll('.modal').forEach((m) => m.remove());
            showNotification('Мессенджер', 'Сообщение переслано', 'success');
        }

        function toggleMessageReaction(messageId, emoji) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const mid = String(messageId || '').trim();
            const e = String(emoji || '').trim();
            if (!mid || !e) return;
            sendMessengerEvent({ type: 'messenger-react', chatId: activeChat.id, messageId: mid, emoji: e });
        }

        function quickReactToMessage(event, messageId, emoji) {
            try {
                const tgt = event?.target;
                if (tgt && tgt.closest && tgt.closest('button,a,input,textarea,.chat-msg-reaction')) return;
            } catch (_) {}
            toggleMessageReaction(messageId, emoji);
        }

        function reactFromContextMenu(messageId, emoji) {
            toggleMessageReaction(messageId, emoji);
            try {
                document.querySelectorAll('.context-menu').forEach((m) => m.remove());
            } catch (_) {}
        }

        function getMessageCopyableText(row) {
            if (!row) return '';
            const kind = row.messageKind || '';
            if (kind === 'voice') {
                const t = String(row.text || '').trim();
                if (t && t !== 'Голосовое сообщение') return t;
                return 'Голосовое сообщение';
            }
            if (kind === 'image') {
                const t = String(row.text || '').trim();
                return t || '[Фото]';
            }
            if (kind === 'video') {
                const t = String(row.text || '').trim();
                return t || '[Видео]';
            }
            return String(row.text || '').trim();
        }

        async function copyMessengerMessage(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            const text = getMessageCopyableText(row);
            try {
                document.querySelectorAll('.context-menu').forEach((m) => m.remove());
            } catch (_) {}
            if (!text) {
                showNotification('Мессенджер', 'Нечего копировать', 'info');
                return;
            }
            try {
                if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
                    await navigator.clipboard.writeText(text);
                } else {
                    const ta = document.createElement('textarea');
                    ta.value = text;
                    ta.setAttribute('readonly', '');
                    ta.style.position = 'fixed';
                    ta.style.left = '-9999px';
                    document.body.appendChild(ta);
                    ta.select();
                    document.execCommand('copy');
                    ta.remove();
                }
                showNotification('Мессенджер', 'Скопировано в буфер', 'success');
            } catch (_) {
                showNotification('Мессенджер', 'Не удалось скопировать', 'error');
            }
        }

        function openMessageReactionsModal(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            const r = row && row.reactions && typeof row.reactions === 'object' ? row.reactions : {};
            const entries = Object.entries(r)
                .map(([emoji, users]) => [String(emoji || ''), Array.isArray(users) ? users : []])
                .filter(([emoji, users]) => emoji && users.length);
            if (!entries.length) {
                showNotification('Реакции', 'Реакций нет', 'info');
                return;
            }
            const reactionOrder = ['❤️', '👍', '👎', '😂', '😮', '😢', '😡', '🔥', '🎉', '👏', '😍', '🤔', '🙏', '💯', '😎'];
            entries.sort((a, b) => reactionOrder.indexOf(a[0]) - reactionOrder.indexOf(b[0]));
            const blocks = entries.map(([emoji, users]) => {
                const uniq = Array.from(new Set(users.map((u) => String(u)).filter(Boolean)));
                const items = uniq.map((uid) => {
                    const peer = resolvePeerDisplay(uid);
                    const name = String(peer?.displayName || peer?.name || uid || '').trim() || uid;
                    const avatar = String(peer?.avatar || '');
                    const initials = String(peer?.initials || '');
                    const uname = String(peer?.username || '').trim();
                    const sub = uname ? `@${uname}` : (peer?.statusText ? String(peer.statusText) : '');
                    return `<div class="contact-item" style="justify-content:flex-start;gap:12px;cursor:pointer;" onclick="openUserProfile('${escapeHtml(uid)}')">
                        <div style="width:44px;height:44px;flex-shrink:0;">${avatarMarkup(name, avatar, initials)}</div>
                        <div style="min-width:0;">
                            <div class="contact-name">${escapeHtml(name)}</div>
                            ${sub ? `<div class="contact-chat">${escapeHtml(sub)}</div>` : ''}
                        </div>
                    </div>`;
                }).join('') || '<div class="friends-empty">Пусто</div>';
                return `<div style="margin-bottom:12px;">
                    <div style="font-weight:900;margin:6px 0 8px;">${escapeHtml(emoji)}</div>
                    <div style="display:grid;gap:8px;">${items}</div>
                </div>`;
            }).join('');
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.innerHTML = `<div class="modal-content" style="max-width:560px;text-align:left;">
                <h2><i class="fas fa-face-smile"></i> Реакции</h2>
                <div style="max-height:60vh;overflow:auto;display:block;">${blocks}</div>
                <div class="modal-buttons">
                    <button type="button" class="modal-btn cancel" onclick="this.closest('.modal').remove()">Закрыть</button>
                </div>
            </div>`;
            document.body.appendChild(modal);
        }

        function openMessageViewsModal(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            const list = Array.isArray(row?.readBy) ? row.readBy.map((u) => String(u)).filter(Boolean) : [];
            const uniq = Array.from(new Set(list));
            if (!uniq.length) {
                showNotification('Просмотры', 'Пока никто не прочитал', 'info');
                return;
            }
            const items = uniq.map((uid) => {
                const peer = resolvePeerDisplay(uid);
                const name = String(peer?.displayName || peer?.name || uid || '').trim() || uid;
                const avatar = String(peer?.avatar || '');
                const initials = String(peer?.initials || '');
                const uname = String(peer?.username || '').trim();
                const sub = uname ? `@${uname}` : (peer?.statusText ? String(peer.statusText) : '');
                return `<div class="contact-item" style="justify-content:flex-start;gap:12px;cursor:pointer;" onclick="openUserProfile('${escapeHtml(uid)}')">
                    <div style="width:44px;height:44px;flex-shrink:0;">${avatarMarkup(name, avatar, initials)}</div>
                    <div style="min-width:0;">
                        <div class="contact-name">${escapeHtml(name)}</div>
                        ${sub ? `<div class="contact-chat">${escapeHtml(sub)}</div>` : ''}
                    </div>
                </div>`;
            }).join('');
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.innerHTML = `<div class="modal-content" style="max-width:520px;text-align:left;">
                <h2><i class="fas fa-eye"></i> Просмотры</h2>
                <div style="max-height:60vh;overflow:auto;display:grid;gap:8px;">${items}</div>
                <div class="modal-buttons">
                    <button type="button" class="modal-btn cancel" onclick="this.closest('.modal').remove()">Закрыть</button>
                </div>
            </div>`;
            document.body.appendChild(modal);
        }

        function openMessageMenu(event, messageId, mine) {
            if (event) event.preventDefault();
            // Если long-press сработал дважды, удаляем предыдущее меню,
            // чтобы не появлялось два одинаковых контекст-меню.
            const now = Date.now();
            try {
                if (
                    window.__lastMsgMenuAt &&
                    window.__lastMsgMenuFor === messageId &&
                    now - window.__lastMsgMenuAt < 650
                ) return;
                window.__lastMsgMenuAt = now;
                window.__lastMsgMenuFor = messageId;
            } catch (_) {}
            try {
                document.querySelectorAll('.context-menu').forEach((m) => m.remove());
            } catch (_) {}
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            if (!row) return;
            const menu = document.createElement('div');
            menu.className = 'context-menu';
            const reactions = ['❤️', '👍', '👎', '😂', '😮', '😢', '😡', '🔥', '🎉', '👏', '😍', '🤔', '🙏', '💯', '😎'];
            let html = `<div class="context-reactions">${reactions
                .map((e) => `<button type="button" onclick="reactFromContextMenu('${escapeHtml(row.id)}','${escapeHtml(e)}')" aria-label="${escapeHtml(e)}">${escapeHtml(e)}</button>`)
                .join('')}</div>`;
            html += `<div class="context-item" onclick="setReplyToMessage('${escapeHtml(row.id)}')"><i class="fas fa-reply"></i> Ответить</div>`;
            const hasReactions = row?.reactions && typeof row.reactions === 'object'
                ? Object.values(row.reactions).some((v) => Array.isArray(v) && v.length)
                : false;
            if (hasReactions) {
                html += `<div class="context-item" onclick="openMessageReactionsModal('${escapeHtml(row.id)}')"><i class="fas fa-face-smile"></i> Реакции</div>`;
            }
            const hasViews = mine && Array.isArray(row?.readBy) && row.readBy.length > 0;
            if (hasViews) {
                html += `<div class="context-item" onclick="openMessageViewsModal('${escapeHtml(row.id)}')"><i class="fas fa-eye"></i> Просмотры</div>`;
            }
            if (mine && !row.deletedAt) {
                html += `<div class="context-item" onclick="startEditMessage('${escapeHtml(row.id)}')"><i class="fas fa-pen"></i> Редактировать</div>`;
                html += `<div class="context-item" onclick="deleteMessageById('${escapeHtml(row.id)}')"><i class="fas fa-trash"></i> Удалить</div>`;
            }
            html += `<div class="context-item" onclick="copyMessageText('${escapeHtml(row.id)}')"><i class="fas fa-copy"></i> Копировать</div>`;
            if ((row.messageKind === 'image' && row.imageBase64) || (row.messageKind === 'video' && row.videoBase64)) {
                html += `<div class="context-item" onclick="downloadMessageMedia('${escapeHtml(row.id)}')"><i class="fas fa-download"></i> Скачать ${row.messageKind === 'image' ? 'фото' : 'видео'}</div>`;
            }
            html += `<div class="context-item" onclick="openForwardModal('${escapeHtml(row.id)}')"><i class="fas fa-share"></i> Переслать</div>`;
            menu.innerHTML = html;
            document.body.appendChild(menu);
            const x = event?.pageX || (window.innerWidth / 2);
            const y = event?.pageY || (window.innerHeight / 2);
            placeContextMenu(menu, x, y);
            setTimeout(() => document.addEventListener('click', () => menu.remove(), { once: true }), 0);
        }

        function copyMessageText(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            if (!row) return;
            const isImage = row.messageKind === 'image';
            const isVideo = row.messageKind === 'video';
            const isVoice = row.messageKind === 'voice';
            const text = String(row.text || (isImage ? 'Фото' : isVideo ? 'Видео' : isVoice ? 'Голосовое сообщение' : '')).trim();
            if (!text) {
                showNotification('Копирование', 'Нет текста для копирования', 'warning');
                return;
            }
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(text)
                    .then(() => showNotification('Копирование', 'Текст сообщения скопирован', 'success'))
                    .catch(() => showNotification('Копирование', 'Не удалось скопировать текст', 'error'));
            } else {
                const textarea = document.createElement('textarea');
                textarea.value = text;
                textarea.style.position = 'fixed';
                textarea.style.left = '-9999px';
                document.body.appendChild(textarea);
                textarea.select();
                try {
                    document.execCommand('copy');
                    showNotification('Копирование', 'Текст сообщения скопирован', 'success');
                } catch (_) {
                    showNotification('Копирование', 'Не удалось скопировать текст', 'error');
                }
                textarea.remove();
            }
        }

        function downloadMessageMedia(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            if (!row) return;
            const isImage = row.messageKind === 'image' && row.imageBase64;
            const isVideo = row.messageKind === 'video' && row.videoBase64;
            if (!isImage && !isVideo) {
                showNotification('Загрузка', 'Нет медиафайла для скачивания', 'warning');
                return;
            }
            const mime = isImage
                ? (/^image\/(jpeg|png|gif|webp)$/i.test(String(row.mimeType || '')) ? String(row.mimeType || '') : 'image/jpeg')
                : (/^video\/(mp4|webm|ogg|quicktime)$/i.test(String(row.videoMime || '')) ? String(row.videoMime || '') : 'video/mp4');
            const raw = isImage ? String(row.imageBase64 || '') : String(row.videoBase64 || '');
            const b64 = raw.replace(/[^a-zA-Z0-9+/=]/g, '');
            const ext = isImage
                ? (mime.split('/')[1] || 'jpg')
                : (mime.split('/')[1] || 'mp4');
            const filename = `message-${messageId || 'media'}.${ext}`;
            const url = `data:${mime};base64,${b64}`;
            const anchor = document.createElement('a');
            anchor.href = url;
            anchor.download = filename;
            anchor.style.display = 'none';
            document.body.appendChild(anchor);
            anchor.click();
            anchor.remove();
            showNotification('Загрузка', 'Файл будет сохранён', 'success');
        }

        function startMessageHold(event, messageId, mine) {
            // Не открываем long-press/контекст-меню для кнопок (play/stop и т.п.).
            const tgt = event?.target;
            if (tgt && tgt.closest && tgt.closest('button')) return;
            cancelMessageHold();
            messageTouchHoldTimer = setTimeout(() => {
                openMessageMenu(event, messageId, mine);
            }, 420);
            // Если пользователь двигает палец — это уже скролл/жест, отменяем long-press.
            try {
                const onMove = () => {
                    cancelMessageHold();
                    try { document.removeEventListener('touchmove', onMove); } catch (_) {}
                };
                document.addEventListener('touchmove', onMove, { passive: true, once: true });
            } catch (_) {}
        }

        function cancelMessageHold() {
            if (!messageTouchHoldTimer) return;
            clearTimeout(messageTouchHoldTimer);
            messageTouchHoldTimer = null;
        }

        let messageSwipeStart = null;
        let messengerIsUserScrolling = false;
        let messengerUserScrollTimer = null;
        let messengerWorkspaceIsUserScrolling = false;
        let messengerWorkspaceUserScrollTimer = null;
        function bindMessengerWorkspaceScrollGuard() {
            if (messengerView !== 'notifications') return;
            const sc = document.querySelector('.messenger-workspace .workspace-scroll');
            if (!sc || sc.dataset.workspaceScrollGuardBound === '1') return;
            sc.dataset.workspaceScrollGuardBound = '1';
            sc.addEventListener('scroll', () => {
                messengerWorkspaceIsUserScrolling = true;
                if (messengerWorkspaceUserScrollTimer) clearTimeout(messengerWorkspaceUserScrollTimer);
                messengerWorkspaceUserScrollTimer = setTimeout(() => {
                    messengerWorkspaceIsUserScrolling = false;
                    if (messengerRenderPendingAfterScroll && shouldRenderMessengerUi()) {
                        messengerRenderPendingAfterScroll = false;
                        renderMainScreen();
                    }
                }, 650);
            }, { passive: true });
        }
        function bindMessengerHistoryScrollGuard() {
            const hist = document.querySelector('.chat-history');
            if (!hist || hist.dataset.scrollGuardBound === '1') return;
            hist.dataset.scrollGuardBound = '1';
            hist.addEventListener('scroll', () => {
                messengerIsUserScrolling = true;
                // Пока пользователь скроллит — запретим автопрокрутку.
                messengerShouldAutoScroll = false;
                // Если пользователь дошёл до низа — скрываем кнопку.
                try {
                    const dist = hist.scrollHeight - hist.scrollTop - hist.clientHeight;
                    if (dist < 80) {
                        messengerNewWhileScrolledCount = 0;
                        updateMessengerNewWhileScrolledFabUI();
                    }
                } catch (_) {}
                updateMessengerNewWhileScrolledFabUI();
                if (messengerUserScrollTimer) clearTimeout(messengerUserScrollTimer);
                messengerUserScrollTimer = setTimeout(() => {
                    messengerIsUserScrolling = false;
                    if (messengerRenderPendingAfterScroll && shouldRenderMessengerUi()) {
                        messengerRenderPendingAfterScroll = false;
                        renderMainScreen();
                    }
                }, 650);
            }, { passive: true });
        }
        function startMessageSwipeStart(event) {
            try {
                const tgt = event?.target;
                if (tgt && tgt.closest && tgt.closest('button')) {
                    messageSwipeStart = null;
                    return;
                }
                const t = event?.touches?.[0];
                const x = t ? t.clientX : event?.clientX;
                const y = t ? t.clientY : event?.clientY;
                if (typeof x !== 'number' || typeof y !== 'number') return;
                const msgEl = event?.target?.closest ? event.target.closest('.chat-msg') : null;
                messageSwipeStart = { x, y, ts: Date.now(), el: msgEl, handler: null };
                // Для анимации во время свайпа: двигаем bubble по X и подсвечиваем.
                const onMove = (ev) => {
                    try {
                        if (!messageSwipeStart) return;
                        const tt = ev?.touches?.[0];
                        if (!tt) return;
                        const curX = tt.clientX;
                        const curY = tt.clientY;
                        const dx = curX - messageSwipeStart.x;
                        const dy = curY - messageSwipeStart.y;
                        // Если это горизонтальный жест — отменяем long-press, чтобы меню не вылезало.
                        if (Math.abs(dx) > Math.abs(dy) && Math.abs(dx) > 25) {
                            cancelMessageHold();
                        }
                        if (messageSwipeStart.el) {
                            const limited = Math.max(-140, Math.min(0, dx));
                            messageSwipeStart.el.style.transform = `translateX(${limited}px)`;
                            if (dx < -28) messageSwipeStart.el.classList.add('chat-msg--swipe-reply');
                            else messageSwipeStart.el.classList.remove('chat-msg--swipe-reply');
                        }
                    } catch (_) {}
                };
                messageSwipeStart.handler = onMove;
                document.addEventListener('touchmove', onMove, { passive: true });
            } catch (_) {}
        }

        function handleMessageSwipeEnd(event, messageId) {
            // При свайпе отменяем long-press меню.
            cancelMessageHold();
            // Убираем touchmove-анимацию.
            try {
                if (messageSwipeStart?.handler) {
                    document.removeEventListener('touchmove', messageSwipeStart.handler);
                }
            } catch (_) {}
            try {
                const tgt = event?.target;
                if (tgt && tgt.closest && tgt.closest('button')) {
                    messageSwipeStart = null;
                    return;
                }
            } catch (_) {}
            if (!messageSwipeStart) return;
            const start = messageSwipeStart;
            messageSwipeStart = null;
            try {
                const t = event?.changedTouches?.[0];
                const x = t ? t.clientX : event?.clientX;
                const y = t ? t.clientY : event?.clientY;
                if (typeof x !== 'number' || typeof y !== 'number') return;
                const dx = x - start.x;
                const dy = y - start.y;
                const dt = Date.now() - start.ts;
                // Свайп справа налево => ответить
                if (dx < -70 && Math.abs(dy) < 60 && dt < 800) {
                    // Убираем анимацию
                    if (start.el && start.el.style) {
                        start.el.style.transform = '';
                        start.el.classList.remove('chat-msg--swipe-reply');
                    }
                    setReplyToMessage(messageId);
                } else {
                    if (start.el && start.el.style) {
                        start.el.style.transform = '';
                        start.el.classList.remove('chat-msg--swipe-reply');
                    }
                }
            } catch (_) {}
        }

        function messengerSafeId(v) {
            return String(v || '').replace(/[^a-zA-Z0-9_-]/g, '');
        }

        function scrollAndHighlightMessengerMessage(messageId) {
            if (!messageId) return;
            try {
                messengerIsUserScrolling = true;
                const safe = messengerSafeId(messageId);
                const el = document.getElementById(`chatMsg-${safe}`);
                if (!el) return;
                el.classList.add('chat-msg--reply-highlight');
                // Подсветка обычно короткая, чтобы не мешала.
                setTimeout(() => {
                    try { el.classList.remove('chat-msg--reply-highlight'); } catch (_) {}
                }, 1200);
                el.scrollIntoView({ behavior: 'smooth', block: 'center' });
            } catch (_) {}
            setTimeout(() => {
                messengerIsUserScrolling = false;
            }, 700);
        }

        let chatListHoldTimer = null;
        function startChatListHold(event, peerId, chatId) {
            cancelChatListHold();
            chatListHoldTimer = setTimeout(() => {
                openChatListContextMenu(event, peerId, chatId);
            }, 480);
        }
        function cancelChatListHold() {
            if (!chatListHoldTimer) return;
            clearTimeout(chatListHoldTimer);
            chatListHoldTimer = null;
        }
        function openChatListContextMenu(event, peerId, chatId) {
            if (event && event.preventDefault) event.preventDefault();
            if (event && event.stopPropagation) event.stopPropagation();
            cancelChatListHold();
            const pid = String(peerId || '').trim();
            const cid = String(chatId || '').trim();
            if (!cid) return;
            const menu = document.createElement('div');
            menu.className = 'context-menu';
            menu.innerHTML = `
                <div class="context-item" onclick="clearChatHistoryForMe('${escapeHtml(cid)}')"><i class="fas fa-eraser"></i> Очистить историю (у себя)</div>
                <div class="context-item" onclick="openDeleteChatModal('${escapeHtml(cid)}','${escapeHtml(pid)}')"><i class="fas fa-trash"></i> Удалить чат…</div>
                ${!pid ? `<div class="context-item" onclick="leaveGroupChat('${escapeHtml(cid)}')"><i class="fas fa-sign-out-alt"></i> Выйти из чата</div>` : ''}
            `;
            document.body.appendChild(menu);
            const x = event?.pageX || event?.clientX || (event?.touches && event.touches[0]?.pageX) || 80;
            const y = event?.pageY || event?.clientY || (event?.touches && event.touches[0]?.pageY) || 80;
            placeContextMenu(menu, x, y);
            setTimeout(() => document.addEventListener('click', () => menu.remove(), { once: true }), 0);
        }
        function clearChatHistoryForMe(chatId) {
            const id = String(chatId || '').trim();
            if (!id) return;
            sendMessengerEvent({ type: 'messenger-clear-chat', chatId: id });
            messengerMessages.set(id, []);
            if (shouldRenderMessengerUi()) renderMainScreen();
        }
        function openDeleteChatModal(chatId, peerId) {
            const cid = String(chatId || '').trim();
            if (!cid) return;
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.innerHTML = `
                <div class="modal-content">
                    <h2><i class="fas fa-trash"></i> Удалить чат</h2>
                    <p style="opacity:.85;">${escapeHtml(peerId || '')}</p>
                    <div class="modal-buttons" style="flex-direction:column;gap:10px;">
                        <button type="button" class="modal-btn confirm" style="width:100%" onclick="confirmDeleteChat('${escapeHtml(cid)}',false);this.closest('.modal').remove();">Удалить у себя</button>
                        <button type="button" class="modal-btn confirm" style="width:100%;background:linear-gradient(135deg,#c0392b,#922b21)" onclick="confirmDeleteChat('${escapeHtml(cid)}',true);this.closest('.modal').remove();">Удалить для всех</button>
                        <button type="button" class="modal-btn cancel" style="width:100%" onclick="this.closest('.modal').remove()">Отмена</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
        }
        function confirmDeleteChat(chatId, forEveryone) {
            sendMessengerEvent({ type: 'messenger-delete-chat', chatId, forEveryone: !!forEveryone });
        }

        function setReplyToMessage(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            if (!row) return;
            composerReplyMessage = row;
            composerEditMessageId = '';
            renderMainScreen();
            requestAnimationFrame(() => {
                const input = document.getElementById('chatComposerInput');
                if (input && typeof input.focus === 'function') input.focus();
            });
        }

        function startEditMessage(messageId) {
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            const row = (resolveChatMessages(activeChat.id) || []).find((item) => item.id === messageId);
            if (!row) return;
            composerReplyMessage = null;
            composerEditMessageId = row.id;
            renderMainScreen();
            const input = document.getElementById('chatComposerInput');
            if (input) {
                input.value = row.text || '';
                input.focus();
                onComposerInput();
            }
        }

        function deleteMessageById(messageId) {
            sendMessengerEvent({ type: 'messenger-delete', messageId });
        }

        function minimizeCallToIsland() {
            if (!roomId) return;
            callMinimized = true;
            renderMainScreen();
            syncCallScreenLayoutMode();
        }

        function restoreCallFromIsland() {
            if (!roomId) return;
            callMinimized = false;
            messengerView = 'calls';
            const island = document.getElementById('callIsland');
            if (island) island.remove();
            renderMainScreen();
            syncCallScreenLayoutMode();
            updateUI();
            updateEmptyState();
        }

        function updateCallMinimizeUnreadBadge() {
            const el = document.getElementById('callUnreadBadge');
            if (!el) return;
            const total = getMessengerUnreadTotal();
            if (!total) {
                el.style.display = 'none';
                el.textContent = '0';
                return;
            }
            el.style.display = 'flex';
            el.textContent = total > 99 ? '99+' : String(total);
        }

        function renderCallIslandWidget() {
            const prev = document.getElementById('callIsland');
            if (prev) prev.remove();
            if (!roomId || !callMinimized) return;
            if (currentGroupCallChatId) return;
            const island = document.createElement('div');
            island.id = 'callIsland';
            island.className = 'call-island';
            const islandTitle = currentGroupCallChatId ? (currentGroupCallTitle || 'Групповой звонок') : 'Идёт звонок';
            island.innerHTML =
                `<div class="call-island-inner"><div class="call-island-title"><i class="fas fa-phone-volume"></i> ${escapeHtml(islandTitle)}</div><div class="call-island-timer" id="callIslandTimer">00:00</div></div><i class="fas fa-chevron-up call-island-chevron" aria-hidden="true"></i>`;
            island.onclick = (e) => {
                e.preventDefault();
                restoreCallFromIsland();
            };
            document.body.appendChild(island);
            updateCallTimerDisplay();
        }

        function openMessengerChat(peerId) {
            const peer = String(peerId || '').trim();
            if (!peer || !authProfile?.appUserId) return;
            messengerShouldAutoScroll = true;
            messengerNewWhileScrolledCount = 0;
            updateMessengerNewWhileScrolledFabUI();
            discardVoicePreview();
            if (voiceRecordingActive && voiceMediaRecorder) {
                const mr = voiceMediaRecorder;
                mr.onstop = () => {
                    clearVoiceRecTimerUi();
                    try {
                        if (voiceMediaStream) voiceMediaStream.getTracks().forEach((t) => t.stop());
                    } catch (_) {}
                    voiceMediaStream = null;
                    voiceMediaRecorder = null;
                    voiceRecordChunks = [];
                    voiceRecordingActive = false;
                    voiceRecordStartedAt = 0;
                };
                try {
                    if (typeof mr.requestData === 'function') mr.requestData();
                    mr.stop();
                } catch (_) {
                    stopVoiceStreams();
                }
            } else {
                stopVoiceStreams();
            }
            messengerView = 'chats';
            if (isMobileLayout()) {
                isChatOpen = true;
            }
            messengerActivePeerId = peer;
            messengerActiveChatId = createDirectChatIdClient(authProfile.appUserId, peer);
            lastActiveChatId = messengerActiveChatId;
            lastActivePeerId = messengerActivePeerId;
            // Открываем чат => сбрасываем счётчик непрочитанных.
            setMessengerUnreadForChat(messengerActiveChatId, 0);
            updateCallMinimizeUnreadBadge();
            messengerComposeBlocked = false;
            messengerComposeHint = '';
            composerReplyMessage = null;
            composerEditMessageId = '';
            persistMessengerSessionChat(messengerActiveChatId);
            persistMessengerSessionPeer(peer);
            sendMessengerEvent({ type: 'messenger-open-chat', chatId: messengerActiveChatId, withUserId: peer });
            renderMainScreen();
        }

        function openMessengerChatById(chatId) {
            const chat = findMessengerChatById(chatId);
            if (!chat) return;
            if (isDirectMessengerChat(chat)) {
                openMessengerChat(chat.peer?.id || '');
                return;
            }
            messengerShouldAutoScroll = true;
            messengerNewWhileScrolledCount = 0;
            updateMessengerNewWhileScrolledFabUI();
            messengerMentionWhileScrolledCount = 0;
            updateMessengerMentionFabUI();
            messengerPendingMentionIdsByChat.delete(String(chat.id || ''));
            stopVoiceStreams();
            discardVoicePreview();
            messengerView = 'chats';
            if (isMobileLayout()) isChatOpen = true;
            messengerActiveChatId = chat.id || '';
            messengerActivePeerId = '';
            lastActiveChatId = messengerActiveChatId;
            lastActivePeerId = messengerActivePeerId;
            setMessengerUnreadForChat(messengerActiveChatId, 0);
            setMessengerMentionUnreadForChat(messengerActiveChatId, 0);
            updateCallMinimizeUnreadBadge();
            messengerComposeBlocked = false;
            messengerComposeHint = '';
            composerReplyMessage = null;
            composerEditMessageId = '';
            persistMessengerSessionChat(messengerActiveChatId);
            persistMessengerSessionPeer('');
            sendMessengerEvent({ type: 'messenger-open-chat', chatId: messengerActiveChatId });
            renderMainScreen();
        }

        function renderNotificationsWorkspace() {
            const list = Array.isArray(messengerNotifications) ? messengerNotifications : [];
            if (!list.length) {
                return `<div class="workspace-scroll sm-workspace sm-workspace--notifications">
                    <div class="sm-empty-state">
                        <i class="fas fa-bell-slash"></i>
                        <div class="sm-empty-title">Уведомлений нет</div>
                        <div class="sm-empty-desc">Когда что-то произойдёт — увидите здесь</div>
                    </div>
                </div>`;
            }
            const items = list.map((it) => {
                const chatTitle = String(it.chatTitle || it.chatId || 'Чат').trim() || 'Чат';
                const ts = new Date(Number(it.createdAt || Date.now())).toLocaleString([], { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
                const unread = messengerNotificationUnreadIds.has(String(it.id || ''));
                const actorName = String(it.actorName || '').trim() || 'Пользователь';
                const actorInitials = String(it.actorInitials || '').trim() || actorName.split(/\s+/).filter(Boolean).map((p) => p.charAt(0)).join('').slice(0, 2).toUpperCase();
                
                let icon = 'fa-bell';
                let typeLabel = 'Уведомление';
                if (it.type === 'mention') {
                    icon = 'fa-at';
                    typeLabel = 'Упоминание';
                } else if (it.type === 'reaction') {
                    icon = 'fa-smile';
                    typeLabel = 'Реакция';
                } else if (it.type === 'system') {
                    icon = 'fa-info-circle';
                    typeLabel = 'Событие';
                }
                
                let metaHtml = '';
                if (it.duration || it.reason) {
                    metaHtml = `<div class="messenger-notification-meta">
                        ${it.duration ? `<div class="messenger-notification-meta-item"><b>Срок:</b> ${escapeHtml(it.duration)}</div>` : ''}
                        ${it.reason ? `<div class="messenger-notification-meta-item"><b>Причина:</b> ${escapeHtml(it.reason)}</div>` : ''}
                    </div>`;
                }
                
                return `<div class="sm-notif-card messenger-notification-card sm-notif-v4" onclick="openMessengerNotification('${escapeHtml(it.id || '')}')">
                    <div class="sm-notif-v4-top">
                        <div class="sm-notif-v4-avatar">
                            ${avatarMarkup(chatTitle, it.chatAvatar || '', it.chatInitials || '')}
                            ${unread ? '<span class="sm-notif-v4-dot"></span>' : ''}
                        </div>
                        <div class="sm-notif-v4-main">
                            <div class="sm-notif-v4-title">${escapeHtml(chatTitle)}</div>
                            <div class="sm-notif-v4-type"><i class="fas ${icon}"></i><span>${escapeHtml(typeLabel)}</span></div>
                        </div>
                        <div class="sm-notif-v4-time">${escapeHtml(ts)}</div>
                    </div>
                    ${it.actorId ? `<div class="sm-notif-v4-actor-row">
                        <div class="sm-notif-v4-actor-avatar">${avatarMarkup(actorName, it.actorAvatar || '', actorInitials)}</div>
                        <div class="sm-notif-v4-actor-text">
                            <span class="sm-notif-actor-name" onclick="event.stopPropagation(); openUserProfile('${escapeHtml(it.actorId || '')}')">${escapeHtml(actorName)}</span>
                            <span>${escapeHtml(String(it.text || it.title || '').replace(actorName, '').trim())}</span>
                        </div>
                    </div>` : ''}
                    ${metaHtml}
                </div>`;
            }).join('');
            return `<div class="workspace-scroll sm-workspace sm-workspace--notifications"><div class="sm-notif-list">${items}</div></div>`;
        }

        function openUserProfile(targetUserId) {
            const id = String(targetUserId || '').trim();
            if (!id) return;
            if (String(id) === String(authProfile?.appUserId || '')) {
                messengerViewedProfile = null;
                messengerView = 'profile';
                requestStoriesForUser(id);
                renderMainScreen();
                return;
            }
            sendMessengerEvent({ type: 'messenger-get-profile', targetUserId: id });
            requestStoriesForUser(id);
            messengerView = 'profile';
            renderMainScreen();
        }

        function canCurrentUserAddProfileToChats(profileView, isFriend) {
            const view = profileView && typeof profileView === 'object' ? profileView : {};
            const profile = view.profile && typeof view.profile === 'object' ? view.profile : {};
            const rule = String(view.canJoinGroups || view.privacy?.canJoinGroups || profile.canJoinGroups || profile.privacy?.canJoinGroups || '').trim();
            if (rule === 'nobody') return false;
            if (rule === 'friends') return !!isFriend;
            if (rule === 'all') return true;
            return true;
        }

        function copyGroupInviteLink(inviteUrl) {
            copyTextToClipboard(inviteUrl, 'Ссылка скопирована');
        }

        function formatGroupParticipantMeta(member) {
            const role = getGroupRoleLabel(member?.role);
            const presence = getParticipantPresenceState(member);
            return `${role} • ${formatPresenceLabel(presence.online, presence.lastSeenAt)}`;
        }

        function openPrivacySettingsModal() {
            closeTransientModal('messengerPrivacySettingsModal');
            const privacy = messengerProfile.privacy || { canWrite: 'all', canCall: 'all', canViewProfile: 'all', canSeeStories: 'friends', canJoinGroups: 'friends' };
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerPrivacySettingsModal';
            modal.innerHTML = `
                <div class="modal-content" style="max-width:620px;text-align:left;">
                    <h2><i class="fas fa-user-shield"></i> Приватность</h2>
                    <div class="privacy-grid">
                        <div class="privacy-card"><span>Кто может писать</span>${renderPrivacyDropdown('canWrite', privacy.canWrite)}</div>
                        <div class="privacy-card"><span>Кто может звонить</span>${renderPrivacyDropdown('canCall', privacy.canCall)}</div>
                        <div class="privacy-card"><span>Кто видит профиль</span>${renderPrivacyDropdown('canViewProfile', privacy.canViewProfile)}</div>
                        <div class="privacy-card"><span>Кто видит истории</span>${renderPrivacyDropdown('canSeeStories', privacy.canSeeStories)}</div>
                        <div class="privacy-card"><span>Кто может добавлять меня в чаты</span>${renderPrivacyDropdown('canJoinGroups', privacy.canJoinGroups)}</div>
                    </div>
                    <div class="modal-buttons">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerPrivacySettingsModal')">Закрыть</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
        }

        function getGroupRestrictionHintClient(restriction) {
            const state = restriction && typeof restriction === 'object' ? restriction : null;
            if (!state || !state.type) return '';
            const duration = state.forever
                ? 'навсегда'
                : (state.until ? new Date(Number(state.until)).toLocaleString([], { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }) : '');
            if (state.type === 'muted') {
                return `У вас мут${duration ? ` до ${duration}` : ''}`;
            }
            if (state.type === 'banned') {
                return `Чат заблокирован для вас${duration ? ` до ${duration}` : ''}`;
            }
            return '';
        }

        function formatGroupRestrictionUntilClient(restriction) {
            const state = restriction && typeof restriction === 'object' ? restriction : null;
            if (!state || !state.type) return '';
            if (state.forever) return 'Навсегда';
            const until = Number(state.until || 0);
            if (!until) return 'Не указан';
            return new Date(until).toLocaleString([], { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
        }

        function getGroupRestrictionStatusText(restriction) {
            const state = restriction && typeof restriction === 'object' ? restriction : null;
            if (!state || !state.type) return '';
            const untilLabel = formatGroupRestrictionUntilClient(state);
            if (state.type === 'muted') {
                return state.forever ? 'Мут навсегда' : `Мут до ${untilLabel}`;
            }
            if (state.type === 'banned') {
                return state.forever ? 'Блокировка навсегда' : `Блокировка до ${untilLabel}`;
            }
            return '';
        }

        function renderGroupRestrictionSummaryCard(restriction) {
            const state = restriction && typeof restriction === 'object' ? restriction : null;
            if (!state || !state.type) {
                return `<div class="contact-item" style="justify-content:flex-start;margin-bottom:14px;"><div><div class="contact-chat">Текущие санкции</div><div class="contact-name">Активных ограничений нет</div></div></div>`;
            }
            const actorName = String(state.actorName || 'Администратор').trim() || 'Администратор';
            const title = state.type === 'banned' ? 'Активная блокировка' : 'Активный мут';
            const duration = formatGroupRestrictionUntilClient(state);
            const reason = String(state.reason || '').trim() || 'Не указана';
            return `<div class="contact-item" style="justify-content:flex-start;margin-bottom:14px;">
                <div>
                    <div class="contact-chat">${escapeHtml(title)}</div>
                    <div class="contact-name">${escapeHtml(getGroupRestrictionStatusText(state))}</div>
                    <div class="contact-chat" style="margin-top:6px;">Выдал(а): ${escapeHtml(actorName)}</div>
                    <div class="contact-chat">Срок: ${escapeHtml(duration)}</div>
                    <div class="contact-chat">Причина: ${escapeHtml(reason)}</div>
                </div>
            </div>`;
        }

        function formatGroupCallDurationSec(totalSec) {
            const safe = Math.max(0, Number(totalSec) || 0);
            const hours = Math.floor(safe / 3600);
            const minutes = Math.floor((safe % 3600) / 60);
            const seconds = safe % 60;
            if (hours > 0) {
                return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
            }
            return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
        }

        function parseGroupEventPayload(text) {
            const raw = String(text || '').trim();
            const match = raw.match(/^\[\[group-event:(.+)\]\]$/s);
            if (!match) return null;
            try {
                const parsed = JSON.parse(match[1]);
                return parsed && typeof parsed === 'object' ? parsed : null;
            } catch (_) {
                return null;
            }
        }

        function renderGroupEventBlock(payload) {
            const event = payload && typeof payload === 'object' ? payload : null;
            if (!event) return '';
            const title = escapeHtml(String(event.title || 'Событие').trim() || 'Событие');
            const previews = Array.isArray(event.participants) ? event.participants.slice(0, 4) : [];
            const actorId = String(event.actorUserId || '').trim();
            const actorName = String(event.actorName || '').trim();
            const actorHtml = actorName
                ? `<div style="margin-top:8px;font-size:12px;opacity:.84;">${escapeHtml(actorName)}</div>`
                : (actorId ? `<div style="margin-top:8px;font-size:12px;opacity:.84;">${escapeHtml(actorId)}</div>` : '');
            const actorsHtml = previews.length
                ? `<div style="margin-top:8px;font-size:12px;opacity:.78;line-height:1.45;">${escapeHtml(previews.map((item) => String(item.displayName || item.userId || 'Пользователь')).filter(Boolean).join(', '))}</div>`
                : '';
            const durationHtml = Number(event.durationSec || 0) > 0
                ? `<div style="margin-top:8px;font-size:12px;opacity:.78;">${escapeHtml(formatGroupCallDurationSec(event.durationSec))}</div>`
                : '';
            const icon = event.type === 'group-call-ended' ? 'fa-phone-slash' : 'fa-phone-volume';
            return `<div class="chat-system-msg" style="max-width:280px;margin:8px auto;padding:14px 16px;">
                <div style="font-size:20px;margin-bottom:8px;"><i class="fas ${icon}"></i></div>
                <div style="font-weight:800;">${title}</div>
                ${actorHtml}
                ${actorsHtml}
                ${durationHtml}
            </div>`;
        }

        function renderGroupBlockedScreen(chat) {
            const restriction = chat?.group?.restriction || null;
            const actorName = String(restriction?.actorName || 'Администратор').trim() || 'Администратор';
            const actorAvatar = String(restriction?.actorAvatar || '').trim() || '';
            const duration = restriction?.forever
                ? 'Навсегда'
                : (restriction?.until ? new Date(Number(restriction.until)).toLocaleString([], { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }) : 'Не указан');
            const reason = String(restriction?.reason || '').trim() || 'Не указана';
            return `<div class="group-blocked-card">
                <div class="group-blocked-card__icon">
                    <i class="fas fa-ban"></i>
                </div>
                <div class="group-blocked-card__title">Заблокирован(а)</div>
                <div class="group-blocked-card__subtitle">Вы сможете вернуться в чат позже</div>
                <div class="group-blocked-card__section">
                    <div class="group-blocked-card__label">Администратор</div>
                    <div class="group-blocked-card__admin">
                        <div style="width:42px;height:42px;flex-shrink:0;border-radius:50%;overflow:hidden;display:flex;align-items:center;justify-content:center;border:1px solid rgba(255,255,255,0.14);">
                            ${avatarMarkup(actorName, actorAvatar, String(actorName || '').slice(0, 2))}
                        </div>
                        <div class="group-blocked-card__admin-name">${escapeHtml(actorName)}</div>
                    </div>
                </div>
                <div class="group-blocked-card__section">
                    <div class="group-blocked-card__label">Срок</div>
                    <div class="group-blocked-card__value">${escapeHtml(duration)}</div>
                </div>
                <div class="group-blocked-card__section">
                    <div class="group-blocked-card__label">Причина</div>
                    <div class="group-blocked-card__value">${escapeHtml(reason)}</div>
                </div>
            </div>`;
        }

        function renderActiveGroupCallBanner(chat) {
            const activeCall = chat?.group?.activeCall;
            if (!activeCall?.roomId) return '';
            const inThisRoom = !!roomId && String(roomId) === String(activeCall.roomId);
            const title = inThisRoom ? 'Идёт звонок' : 'Групповой звонок';
            const subtitle = activeCall.participantCount > 0 ? `${activeCall.participantCount} участников в звонке` : 'Звонок уже создан';
            const actionText = inThisRoom ? 'Вернуться' : 'Войти';
            return `<div class="contact-item" style="justify-content:space-between;gap:12px;margin:0;">
                <div style="min-width:0;">
                    <div class="contact-name"><i class="fas fa-phone-volume" style="color:#5be37a;"></i> ${escapeHtml(title)}</div>
                    <div class="contact-chat">${escapeHtml(subtitle)}</div>
                </div>
                <button type="button" class="contact-btn" onclick="joinActiveGroupCall('${escapeHtml(String(chat.id || ''))}')">${actionText}</button>
            </div>`;
        }

        function getActiveGroupCallChats(limit = 3) {
            const activeChatId = String(messengerActiveChatId || '').trim();
            const currentCallChatId = String(currentGroupCallChatId || '').trim();
            const list = (Array.isArray(messengerChats) ? messengerChats : []).filter((chat) => {
                return isGroupMessengerChat(chat) && !!String(chat?.group?.activeCall?.roomId || '').trim();
            });
            list.sort((a, b) => {
                const aRoomMatch = !!roomId && String(a?.group?.activeCall?.roomId || '') === String(roomId || '');
                const bRoomMatch = !!roomId && String(b?.group?.activeCall?.roomId || '') === String(roomId || '');
                if (aRoomMatch !== bRoomMatch) return bRoomMatch ? 1 : -1;
                const aCallMatch = currentCallChatId && String(a?.id || '') === currentCallChatId;
                const bCallMatch = currentCallChatId && String(b?.id || '') === currentCallChatId;
                if (aCallMatch !== bCallMatch) return bCallMatch ? 1 : -1;
                const aActiveMatch = activeChatId && String(a?.id || '') === activeChatId;
                const bActiveMatch = activeChatId && String(b?.id || '') === activeChatId;
                if (aActiveMatch !== bActiveMatch) return bActiveMatch ? 1 : -1;
                return Number(b?.updatedAt || 0) - Number(a?.updatedAt || 0);
            });
            return list.slice(0, Math.max(1, Number(limit) || 3));
        }

        function renderGlobalActiveGroupCallWidgets() {
            const chats = getActiveGroupCallChats(3);
            if (!chats.length) return '';
            const totalActive = (Array.isArray(messengerChats) ? messengerChats : []).filter((chat) => {
                return isGroupMessengerChat(chat) && !!String(chat?.group?.activeCall?.roomId || '').trim();
            }).length;
            const itemsHtml = chats.map((chat) => {
                const activeCall = chat.group?.activeCall || null;
                const inThisRoom = !!roomId && String(roomId) === String(activeCall?.roomId || '');
                const actionText = inThisRoom ? 'Вернуться' : 'Войти';
                const stateTitle = inThisRoom ? 'Идёт звонок' : 'Групповой звонок';
                const groupTitle = chat.peer?.displayName || chat.peer?.name || 'Групповой чат';
                const participantLine = Number(activeCall?.participantCount || 0) > 0
                    ? `${Number(activeCall.participantCount || 0)} участников`
                    : 'Звонок уже создан';
                return `<div class="contact-item" style="justify-content:space-between;gap:12px;margin-bottom:10px;cursor:pointer;" onclick="openMessengerChatById('${escapeHtml(chat.id || '')}')">
                    <div style="display:flex;align-items:center;gap:12px;min-width:0;">
                        <div style="width:42px;height:42px;flex-shrink:0;">${avatarMarkup(groupTitle, chat.peer?.avatar || '', chat.peer?.initials || '')}</div>
                        <div style="min-width:0;">
                            <div class="contact-name" style="display:flex;align-items:center;gap:8px;"><i class="fas fa-phone-volume" style="color:#5be37a;"></i><span style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escapeHtml(stateTitle)}</span></div>
                            <div class="contact-chat" style="white-space:normal;">${escapeHtml(groupTitle)} • ${escapeHtml(participantLine)}</div>
                        </div>
                    </div>
                    <button type="button" class="contact-btn" onclick="event.stopPropagation();joinActiveGroupCall('${escapeHtml(chat.id || '')}')">${actionText}</button>
                </div>`;
            }).join('');
            const moreHtml = totalActive > chats.length
                ? `<div class="messenger-connection" style="margin-top:0;">Ещё активных звонков: ${totalActive - chats.length}</div>`
                : '';
            return `<div style="margin:8px 0 12px;">
                <div class="messenger-connection" style="margin-top:0;margin-bottom:8px;"><i class="fas fa-phone-volume" style="margin-right:6px;color:#5be37a;"></i>Активные групповые звонки</div>
                ${itemsHtml}
                ${moreHtml}
            </div>`;
        }

        function getAvailableGroupsForUserInvite(targetUserId) {
            const targetId = String(targetUserId || '').trim();
            if (!targetId || !authProfile?.appUserId || targetId === String(authProfile.appUserId || '')) return [];
            return (messengerChats || []).filter((chat) => {
                if (!isGroupMessengerChat(chat)) return false;
                if (!hasGroupPermissionClient(chat, 'addMembers')) return false;
                const members = Array.isArray(chat.group?.members) ? chat.group.members.map((item) => String(item || '')) : [];
                return !members.includes(targetId);
            });
        }

        function openAddUserToGroupModal(targetUserId) {
            const targetId = String(targetUserId || '').trim();
            const userInfo = getUserInfo(targetId);
            const groups = getAvailableGroupsForUserInvite(targetId);
            closeTransientModal('messengerAddUserToGroupModal');
            const list = groups.length
                ? groups.map((chat) => `<button type="button" class="contact-btn" style="width:100%;display:flex;align-items:center;justify-content:flex-start;gap:12px;margin-bottom:8px;" onclick="addUserToGroupChat('${escapeHtml(chat.id || '')}','${escapeHtml(targetId)}')"><span style="width:40px;height:40px;display:inline-flex;">${avatarMarkup(chat.peer?.displayName || chat.peer?.name || chat.id || '', chat.peer?.avatar || '', chat.peer?.initials || '')}</span><span style="text-align:left;">${escapeHtml(chat.peer?.displayName || chat.peer?.name || chat.id || '')}</span></button>`).join('')
                : '<div class="friends-empty">Нет групп, куда вы можете добавить этого друга</div>';
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerAddUserToGroupModal';
            modal.innerHTML = `<div class="modal-content" style="max-width:520px;text-align:left;"><h2><i class="fas fa-comments"></i> Выбор чата</h2><div class="contact-item" style="justify-content:flex-start;gap:12px;margin-bottom:14px;"><div style="width:44px;height:44px;">${avatarMarkup(userInfo.displayName || userInfo.name || targetId, userInfo.avatar || '', userInfo.initials || '')}</div><div><div class="contact-name">${escapeHtml(userInfo.displayName || userInfo.name || targetId)}</div><div class="contact-chat">${escapeHtml(userInfo.username ? '@' + userInfo.username : targetId)}</div></div></div><div class="contacts-list" style="max-height:320px;">${list}</div><div class="modal-buttons"><button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerAddUserToGroupModal')">Отмена</button></div></div>`;
            document.body.appendChild(modal);
        }

        function addUserToGroupChat(chatId, targetUserId) {
            sendMessengerEvent({ type: 'messenger-add-group-members', chatId, memberIds: [targetUserId] });
            closeTransientModal('messengerAddUserToGroupModal');
            showNotification('Группа', 'Запрос на добавление отправлен', 'info');
        }

        function startGroupCallForChat(chatId) {
            const chat = findMessengerChatById(chatId);
            if (!chat || !isGroupMessengerChat(chat)) return;
            const activeCall = chat.group?.activeCall || null;
            const callRoomId = String(activeCall?.roomId || '').trim();
            if (callRoomId) {
                if (roomId && roomId === callRoomId) {
                    if (callMinimized) restoreCallFromIsland();
                    else {
                        messengerView = 'calls';
                        renderMainScreen();
                    }
                    return;
                }
                joinRoom(callRoomId, {
                    groupChatId: chat.id,
                    groupTitle: chat.peer?.displayName || chat.peer?.name || 'Групповой звонок'
                });
                closeTransientModal('messengerGroupProfileModal');
                return;
            }
            sendMessengerEvent({ type: 'messenger-create-group-call', chatId });
            closeTransientModal('messengerGroupProfileModal');
        }

        function joinActiveGroupCall(chatId) {
            startGroupCallForChat(chatId);
        }

        function closeTransientModal(id) {
            const el = id ? document.getElementById(id) : null;
            if (el) el.remove();
        }

        function onGroupAvatarSelected(event) {
            const input = event?.target;
            const file = input?.files?.[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onloadend = () => {
                const raw = String(reader.result || '');
                const preview = document.getElementById('groupAvatarPreview');
                if (preview) {
                    preview.innerHTML = raw ? `<img src="${escapeHtml(raw)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:18px;">` : '<i class="fas fa-users"></i>';
                }
                if (input) input.dataset.avatar = raw;
            };
            reader.readAsDataURL(file);
        }

        function openCreateGroupModal() {
            closeTransientModal('messengerCreateGroupModal');
            const friends = Array.isArray(friendsState.friends) ? friendsState.friends : [];
            const membersHtml = friends.length
                ? friends.map((friend) => `
                    <label class="contact-item" style="cursor:pointer;justify-content:flex-start;gap:12px;">
                        <input type="checkbox" value="${escapeHtml(friend.id || '')}" style="accent-color:#7c5cff;">
                        <div style="width:42px;height:42px;flex-shrink:0;">${avatarMarkup(friend.displayName || friend.name || friend.id || '', friend.avatar || '', friend.initials || '')}</div>
                        <div style="min-width:0;">
                            <div class="contact-name">${escapeHtml(friend.displayName || friend.name || friend.id || '')}</div>
                            <div class="contact-chat">${escapeHtml(friend.username ? '@' + friend.username : friend.id || '')}</div>
                        </div>
                    </label>`).join('')
                : '<div class="friends-empty">Добавлять можно только друзей. Сначала добавьте друзей.</div>';
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerCreateGroupModal';
            modal.innerHTML = `
                <div class="modal-content" style="max-width:560px;text-align:left;">
                    <h2 style="text-align:center;"><i class="fas fa-pen"></i> Создание чата</h2>
                    <div style="display:flex;gap:14px;align-items:center;margin-bottom:16px;">
                        <label id="groupAvatarPreview" for="groupAvatarInput" style="width:76px;height:76px;border-radius:18px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);display:flex;align-items:center;justify-content:center;cursor:pointer;overflow:hidden;"><i class="fas fa-users"></i></label>
                        <div style="flex:1;">
                            <input id="groupTitleInput" class="modal-input" maxlength="220" placeholder="Название" style="margin-bottom:10px;text-align:left;">
                            <input id="groupInviteInput" class="modal-input" maxlength="120" placeholder="Своя ссылка (необязательно)" style="margin-bottom:0;text-align:left;">
                        </div>
                    </div>
                    <input id="groupAvatarInput" type="file" accept="image/*" style="display:none" onchange="onGroupAvatarSelected(event)">
                    <textarea id="groupDescriptionInput" class="modal-input" maxlength="4000" placeholder="Описание" style="min-height:96px;resize:vertical;text-align:left;"></textarea>
                    <div style="font-size:13px;font-weight:700;margin-bottom:8px;">Добавить участников</div>
                    <div class="contacts-list" style="max-height:220px;">${membersHtml}</div>
                    <div class="modal-buttons" style="margin-top:16px;">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerCreateGroupModal')">Отмена</button>
                        <button type="button" class="modal-btn confirm" onclick="submitCreateGroup()">Создать</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
        }

        function submitCreateGroup() {
            const modal = document.getElementById('messengerCreateGroupModal');
            if (!modal) return;
            const title = String(modal.querySelector('#groupTitleInput')?.value || '').trim();
            if (!title) {
                showNotification('Мессенджер', 'Укажите название чата', 'warning');
                return;
            }
            const inviteCode = String(modal.querySelector('#groupInviteInput')?.value || '').trim();
            const description = String(modal.querySelector('#groupDescriptionInput')?.value || '').trim();
            const avatar = String(modal.querySelector('#groupAvatarInput')?.dataset?.avatar || '');
            const memberIds = Array.from(modal.querySelectorAll('.contacts-list input[type="checkbox"]:checked')).map((el) => String(el.value || '').trim()).filter(Boolean);
            sendMessengerEvent({
                type: 'messenger-create-group',
                title,
                description,
                inviteCode,
                avatar,
                memberIds
            });
            modal.remove();
        }

        function getGroupRoleLabel(role) {
            if (role === 'owner') return 'Владелец';
            if (role === 'admin') return 'Администратор';
            return 'Участник';
        }

        function getGroupPermissionLabel(value) {
            return { owner: 'Только владелец', owner_admins: 'Владелец и администраторы', all: 'Все' }[String(value || '').trim()] || 'Владелец и администраторы';
        }

        function hasGroupPermissionClient(chat, key) {
            const role = String(chat?.group?.myRole || '').trim();
            if (!role) return false;
            const rule = String(chat?.group?.permissions?.[key] || 'owner_admins').trim();
            if (rule === 'all') return true;
            if (rule === 'owner_admins') return role === 'owner' || role === 'admin';
            return role === 'owner';
        }

        function canManageGroupMemberClient(chat, member) {
            const myRole = String(chat?.group?.myRole || '').trim();
            const targetRole = String(member?.role || '').trim();
            const rank = { owner: 3, admin: 2, member: 1 };
            const myRank = rank[myRole] || 0;
            const targetRank = rank[targetRole] || 0;
            return !!member?.userId && String(member.userId) !== String(authProfile?.appUserId || '') && myRank > targetRank;
        }

        function getGroupInviteUrl(chat) {
            const code = String(chat?.group?.inviteCode || '').trim();
            if (!code) return '';
            return `${location.origin}${location.pathname}?groupInvite=${encodeURIComponent(code)}`;
        }

        function openGroupEditModal(chatId) {
            const chat = findMessengerChatById(chatId);
            if (!chat || !isGroupMessengerChat(chat)) return;
            closeTransientModal('messengerGroupEditModal');
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerGroupEditModal';
            modal.innerHTML = `
                <div class="modal-content" style="max-width:560px;text-align:left;">
                    <h2><i class="fas fa-pen"></i> Изменение информации</h2>
                    <div style="display:flex;gap:14px;align-items:center;margin-bottom:16px;">
                        <label id="groupEditAvatarPreview" for="groupEditAvatarInput" style="width:76px;height:76px;border-radius:18px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);display:flex;align-items:center;justify-content:center;cursor:pointer;overflow:hidden;">${chat.peer?.avatar ? `<img src="${escapeHtml(chat.peer.avatar)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:18px;">` : '<i class="fas fa-users"></i>'}</label>
                        <div style="flex:1;">
                            <input id="groupEditTitleInput" class="modal-input" maxlength="220" placeholder="Название" value="${escapeHtml(chat.peer?.displayName || chat.peer?.name || '')}" style="margin-bottom:10px;text-align:left;">
                            <input id="groupEditInviteInput" class="modal-input" maxlength="120" placeholder="Своя ссылка" value="${escapeHtml(chat.group?.inviteCode || '')}" style="margin-bottom:0;text-align:left;">
                        </div>
                    </div>
                    <input id="groupEditAvatarInput" type="file" accept="image/*" style="display:none" onchange="onGroupEditAvatarSelected(event)">
                    <textarea id="groupEditDescriptionInput" class="modal-input" maxlength="4000" placeholder="Описание" style="min-height:96px;resize:vertical;text-align:left;">${escapeHtml(chat.group?.description || '')}</textarea>
                    <div class="modal-buttons">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerGroupEditModal')">Отмена</button>
                        <button type="button" class="modal-btn confirm" onclick="submitGroupEdit('${escapeHtml(chatId)}')">Сохранить</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
        }

        function onGroupEditAvatarSelected(event) {
            const input = event?.target;
            const file = input?.files?.[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onloadend = () => {
                const raw = String(reader.result || '');
                const preview = document.getElementById('groupEditAvatarPreview');
                if (preview) {
                    preview.innerHTML = raw ? `<img src="${escapeHtml(raw)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:18px;">` : '<i class="fas fa-users"></i>';
                }
                if (input) input.dataset.avatar = raw;
            };
            reader.readAsDataURL(file);
        }

        function submitGroupEdit(chatId) {
            const modal = document.getElementById('messengerGroupEditModal');
            if (!modal) return;
            const payload = {
                type: 'messenger-update-group',
                chatId,
                title: String(modal.querySelector('#groupEditTitleInput')?.value || '').trim(),
                description: String(modal.querySelector('#groupEditDescriptionInput')?.value || '').trim(),
                inviteCode: String(modal.querySelector('#groupEditInviteInput')?.value || '').trim()
            };
            const avatarData = modal.querySelector('#groupEditAvatarInput')?.dataset?.avatar;
            if (avatarData !== undefined) payload.avatar = String(avatarData || '');
            sendMessengerEvent(payload);
            modal.remove();
        }

        function openAppearanceSettingsModal() {
            closeTransientModal('messengerAppearanceSettingsModal');
            const draft = {
                theme: messengerAppearance.theme === 'dark' ? 'dark' : 'classic',
                chatWallpaper: String(messengerAppearance.chatWallpaper || '').trim(),
                chatWallpaperBlur: messengerAppearance.chatWallpaperBlur !== false
            };
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerAppearanceSettingsModal';
            modal.innerHTML = `
                <div class="modal-content modal-sheet modal-sheet--pc-dialog">
                    <div class="modal-sheet-header">
                        <div class="modal-sheet-title"><i class="fas fa-palette"></i><span>Внешний вид</span></div>
                        <button type="button" class="modal-sheet-close" onclick="closeTransientModal('messengerAppearanceSettingsModal')" aria-label="Закрыть"><i class="fas fa-times"></i></button>
                    </div>
                    <div class="modal-sheet-body">
                        <div class="ui-row" style="margin-bottom:10px;">
                            <div>
                                <div class="ui-row-title">Темы</div>
                                <div class="ui-row-subtitle">Дарк и классик — выбирай что нравится.</div>
                            </div>
                        </div>

                        <div class="ui-cards" style="margin-bottom:14px;">
                            <div class="ui-card ${draft.theme === 'classic' ? 'active' : ''}" id="appearanceThemeClassic" role="button" tabindex="0">
                                <div class="ui-card-title"><span><i class="fas fa-sun" style="margin-right:8px;"></i>Классический</span>${draft.theme === 'classic' ? '<span style="opacity:.9;">Выбран</span>' : ''}</div>
                                <div class="ui-card-subtitle">Светлее и мягче акценты.</div>
                                <div class="ui-preview ui-preview--classic">
                                    <div class="ui-preview-msgs">
                                        <div class="ui-preview-bubble">Классик</div>
                                        <div class="ui-preview-bubble out">Готово</div>
                                    </div>
                                </div>
                            </div>
                            <div class="ui-card ${draft.theme === 'dark' ? 'active' : ''}" id="appearanceThemeDark" role="button" tabindex="0">
                                <div class="ui-card-title"><span><i class="fas fa-moon" style="margin-right:8px;"></i>Дарк</span>${draft.theme === 'dark' ? '<span style="opacity:.9;">Выбран</span>' : ''}</div>
                                <div class="ui-card-subtitle">Глубокий фон и яркие градиенты.</div>
                                <div class="ui-preview ui-preview--dark">
                                    <div class="ui-preview-msgs">
                                        <div class="ui-preview-bubble">Дарк</div>
                                        <div class="ui-preview-bubble out">Готово</div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div class="ui-row" style="margin-bottom:10px;">
                            <div>
                                <div class="ui-row-title">Обои чата</div>
                                <div class="ui-row-subtitle">Загрузи свою картинку. Размытие можно включить/выключить.</div>
                            </div>
                            <div class="ui-row-actions">
                                <button type="button" class="ui-mini-btn" id="appearanceWallpaperPick"><i class="fas fa-image"></i><span>${draft.chatWallpaper ? 'Сменить' : 'Загрузить'}</span></button>
                                <button type="button" class="ui-mini-btn delete" id="appearanceWallpaperRemove" style="display:${draft.chatWallpaper ? 'inline-flex' : 'none'};"><i class="fas fa-trash"></i><span>Удалить</span></button>
                            </div>
                        </div>

                        <label class="ui-row" style="cursor:pointer;">
                            <div>
                                <div class="ui-row-title">Размытие обоев</div>
                                <div class="ui-row-subtitle">Если выключить — обои будут четкими.</div>
                            </div>
                            <div class="ui-row-actions">
                                <input id="appearanceWallpaperBlurToggle" type="checkbox" ${draft.chatWallpaperBlur ? 'checked' : ''} style="accent-color:#7c5cff;width:18px;height:18px;">
                            </div>
                        </label>

                        <input type="file" id="appearanceWallpaperInput" accept="image/*" style="display:none">
                    </div>
                    <div class="modal-buttons" style="flex-shrink:0;">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerAppearanceSettingsModal')">Отмена</button>
                        <button type="button" class="modal-btn confirm" id="appearanceApplyBtn">Применить</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);

            const themeClassicEl = document.getElementById('appearanceThemeClassic');
            const themeDarkEl = document.getElementById('appearanceThemeDark');
            const pickBtn = document.getElementById('appearanceWallpaperPick');
            const removeBtn = document.getElementById('appearanceWallpaperRemove');
            const blurToggle = document.getElementById('appearanceWallpaperBlurToggle');
            const fileInput = document.getElementById('appearanceWallpaperInput');
            const applyBtn = document.getElementById('appearanceApplyBtn');

            const syncPreview = () => {
                if (removeBtn) removeBtn.style.display = draft.chatWallpaper ? 'inline-flex' : 'none';
                if (pickBtn) {
                    const span = pickBtn.querySelector('span');
                    if (span) span.textContent = draft.chatWallpaper ? 'Сменить' : 'Загрузить';
                }
                if (themeClassicEl) themeClassicEl.classList.toggle('active', draft.theme === 'classic');
                if (themeDarkEl) themeDarkEl.classList.toggle('active', draft.theme === 'dark');
                if (blurToggle) blurToggle.checked = !!draft.chatWallpaperBlur;
            };

            const bindCard = (el, theme) => {
                if (!el) return;
                const activate = () => {
                    draft.theme = theme;
                    syncPreview();
                };
                el.onclick = activate;
                el.onkeydown = (e) => {
                    if (e && (e.key === 'Enter' || e.key === ' ')) {
                        e.preventDefault();
                        activate();
                    }
                };
            };
            bindCard(themeClassicEl, 'classic');
            bindCard(themeDarkEl, 'dark');

            if (pickBtn && fileInput) pickBtn.onclick = () => fileInput.click();
            if (removeBtn) removeBtn.onclick = () => {
                draft.chatWallpaper = '';
                syncPreview();
            };
            if (blurToggle) blurToggle.onchange = () => {
                draft.chatWallpaperBlur = !!blurToggle.checked;
                syncPreview();
            };
            if (fileInput) {
                fileInput.onchange = async () => {
                    const file = fileInput.files && fileInput.files[0];
                    if (!file || !/^image\//i.test(file.type || '')) return;
                    try {
                        draft.chatWallpaper = await compressImageToJpegDataUrl(file, 1920, 0.82);
                        syncPreview();
                    } catch (_) {
                        showNotification('Обои', 'Не удалось обработать изображение', 'warning');
                    }
                    fileInput.value = '';
                };
            }
            if (applyBtn) {
                applyBtn.onclick = () => {
                    messengerAppearance = {
                        ...messengerAppearance,
                        theme: draft.theme === 'dark' ? 'dark' : 'classic',
                        chatWallpaper: String(draft.chatWallpaper || '').trim(),
                        chatWallpaperBlur: !!draft.chatWallpaperBlur
                    };
                    applyMessengerTheme();
                    sendMessengerEvent({
                        type: 'messenger-update-appearance',
                        theme: messengerAppearance.theme,
                        chatWallpaper: messengerAppearance.chatWallpaper,
                        chatWallpaperBlur: !!messengerAppearance.chatWallpaperBlur
                    });
                    closeTransientModal('messengerAppearanceSettingsModal');
                    if (shouldRenderMessengerUi()) renderMainScreen();
                };
            }
            syncPreview();
        }

        function renderGroupPermissionSelect(id, current) {
            const safe = ['owner', 'owner_admins', 'all'].includes(String(current || '').trim()) ? String(current).trim() : 'owner_admins';
            return `<select id="${id}" class="modal-input" style="margin:0;text-align:left;">
                <option value="owner" ${safe === 'owner' ? 'selected' : ''}>Только владелец</option>
                <option value="owner_admins" ${safe === 'owner_admins' ? 'selected' : ''}>Владелец и администраторы</option>
                <option value="all" ${safe === 'all' ? 'selected' : ''}>Все</option>
            </select>`;
        }

        function openGroupSettingsModal(chatId) {
            const chat = findMessengerChatById(chatId);
            if (!chat || !isGroupMessengerChat(chat)) return;
            closeTransientModal('messengerGroupSettingsModal');
            const perms = chat.group?.permissions || {};
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerGroupSettingsModal';
            modal.innerHTML = `
                <div class="modal-content modal-sheet">
                    <div class="modal-sheet-header">
                        <div class="modal-sheet-title"><i class="fas fa-cog"></i><span>Управление чатом</span></div>
                        <button type="button" class="modal-sheet-close" onclick="closeTransientModal('messengerGroupSettingsModal')" aria-label="Закрыть"><i class="fas fa-times"></i></button>
                    </div>
                    <div class="modal-sheet-body">
                        <div class="ui-row" style="margin-bottom:12px;">
                            <div>
                                <div class="ui-row-title">Права доступа</div>
                                <div class="ui-row-subtitle">Кто и что может делать в этом чате.</div>
                            </div>
                        </div>

                        <div class="ui-row" style="margin-bottom:10px;">
                            <div>
                                <div class="ui-row-title">Добавлять участников</div>
                                <div class="ui-row-subtitle">Кому разрешено приглашать людей.</div>
                            </div>
                            <div class="ui-row-actions">${renderGroupPermissionSelect('permAddMembers', perms.addMembers)}</div>
                        </div>

                        <div class="ui-row" style="margin-bottom:10px;">
                            <div>
                                <div class="ui-row-title">Изменять информацию</div>
                                <div class="ui-row-subtitle">Название, описание и аватар чата.</div>
                            </div>
                            <div class="ui-row-actions">${renderGroupPermissionSelect('permEditInfo', perms.editInfo)}</div>
                        </div>

                        <div class="ui-row" style="margin-bottom:10px;">
                            <div>
                                <div class="ui-row-title">Модерация</div>
                                <div class="ui-row-subtitle">Мьют/бан/кик и управление участниками.</div>
                            </div>
                            <div class="ui-row-actions">${renderGroupPermissionSelect('permModerate', perms.moderate)}</div>
                        </div>

                        <div class="ui-row" style="margin-bottom:10px;">
                            <div>
                                <div class="ui-row-title">Ссылка чата</div>
                                <div class="ui-row-subtitle">Кто видит и может копировать инвайт.</div>
                            </div>
                            <div class="ui-row-actions">${renderGroupPermissionSelect('permLinkAccess', perms.linkAccess)}</div>
                        </div>

                        <div class="ui-row" style="margin-bottom:12px;">
                            <div>
                                <div class="ui-row-title">Звонки</div>
                                <div class="ui-row-subtitle">Кто может создавать групповой звонок.</div>
                            </div>
                            <div class="ui-row-actions">${renderGroupPermissionSelect('permCreateCalls', perms.createCalls)}</div>
                        </div>

                        <label class="ui-row" style="cursor:pointer;">
                            <div>
                                <div class="ui-row-title">Вступление по ссылке</div>
                                <div class="ui-row-subtitle">Если выключить — по ссылке никто не зайдёт.</div>
                            </div>
                            <div class="ui-row-actions">
                                <input id="groupJoinByLinkToggle" type="checkbox" ${chat.group?.joinByLink !== false ? 'checked' : ''} style="accent-color:#7c5cff;width:18px;height:18px;">
                            </div>
                        </label>
                    </div>
                    <div class="modal-buttons" style="flex-shrink:0;">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerGroupSettingsModal')">Отмена</button>
                        <button type="button" class="modal-btn confirm" onclick="submitGroupSettings('${escapeHtml(chatId)}')">Сохранить</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
        }

        function submitGroupSettings(chatId) {
            const modal = document.getElementById('messengerGroupSettingsModal');
            if (!modal) return;
            sendMessengerEvent({
                type: 'messenger-update-group',
                chatId,
                joinByLink: !!modal.querySelector('#groupJoinByLinkToggle')?.checked,
                permissions: {
                    addMembers: String(modal.querySelector('#permAddMembers')?.value || 'owner_admins'),
                    editInfo: String(modal.querySelector('#permEditInfo')?.value || 'owner_admins'),
                    moderate: String(modal.querySelector('#permModerate')?.value || 'owner_admins'),
                    linkAccess: String(modal.querySelector('#permLinkAccess')?.value || 'owner_admins'),
                    createCalls: String(modal.querySelector('#permCreateCalls')?.value || 'owner_admins')
                }
            });
            modal.remove();
        }

        function openAddMembersToGroupModal(chatId) {
            const chat = findMessengerChatById(chatId);
            if (!chat || !isGroupMessengerChat(chat)) return;
            const currentMembers = new Set((chat.group?.members || []).map((v) => String(v || '')));
            const friends = (friendsState.friends || []).filter((friend) => !currentMembers.has(String(friend.id || '')));
            const list = friends.length
                ? friends.map((friend) => `
                    <label class="contact-item" style="cursor:pointer;justify-content:flex-start;gap:12px;">
                        <input type="checkbox" value="${escapeHtml(friend.id || '')}" style="accent-color:#7c5cff;">
                        <div style="width:42px;height:42px;flex-shrink:0;">${avatarMarkup(friend.displayName || friend.name || friend.id || '', friend.avatar || '', friend.initials || '')}</div>
                        <div style="min-width:0;">
                            <div class="contact-name">${escapeHtml(friend.displayName || friend.name || friend.id || '')}</div>
                            <div class="contact-chat">${escapeHtml(friend.username ? '@' + friend.username : friend.id || '')}</div>
                        </div>
                    </label>`).join('')
                : '<div class="friends-empty">Свободных друзей для добавления нет</div>';
            closeTransientModal('messengerGroupAddMembersModal');
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerGroupAddMembersModal';
            modal.innerHTML = `<div class="modal-content" style="max-width:560px;text-align:left;"><h2><i class="fas fa-user-plus"></i> Добавить участников</h2><div class="contacts-list" style="max-height:320px;">${list}</div><div class="modal-buttons"><button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerGroupAddMembersModal')">Отмена</button><button type="button" class="modal-btn confirm" onclick="submitAddMembersToGroup('${escapeHtml(chatId)}')">Добавить</button></div></div>`;
            document.body.appendChild(modal);
        }

        function submitAddMembersToGroup(chatId) {
            const modal = document.getElementById('messengerGroupAddMembersModal');
            if (!modal) return;
            const memberIds = Array.from(modal.querySelectorAll('input[type="checkbox"]:checked')).map((el) => String(el.value || '').trim()).filter(Boolean);
            if (!memberIds.length) {
                showNotification('Группа', 'Выберите хотя бы одного друга', 'warning');
                return;
            }
            sendMessengerEvent({ type: 'messenger-add-group-members', chatId, memberIds });
            modal.remove();
        }

        function openGroupMemberActionModal(chatId, targetUserId) {
            const chat = findMessengerChatById(chatId);
            const member = (chat?.group?.participants || []).find((item) => String(item?.userId || '') === String(targetUserId || ''));
            if (!chat || !member) return;
            closeTransientModal('messengerGroupMemberActionModal');
            const restriction = member?.restriction || null;
            const canToggleAdmin = String(chat?.group?.myRole || '') === 'owner';
            const muteActionLabel = restriction?.type === 'muted' ? 'Снять мут' : 'Мьют';
            const muteActionValue = restriction?.type === 'muted' ? 'unmute' : 'mute';
            const banActionLabel = restriction?.type === 'banned' ? 'Снять блокировку' : 'Блокировка';
            const banActionValue = restriction?.type === 'banned' ? 'unban' : 'ban';
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerGroupMemberActionModal';
            modal.innerHTML = `
                <div class="modal-content modal-sheet modal-sheet--pc-dialog">
                    <div class="modal-sheet-header">
                        <div class="modal-sheet-title"><i class="fas fa-user-shield"></i><span>Управление участником</span></div>
                        <button type="button" class="modal-sheet-close" onclick="closeTransientModal('messengerGroupMemberActionModal')" aria-label="Закрыть"><i class="fas fa-times"></i></button>
                    </div>
                    <div class="modal-sheet-body">
                        <div class="ui-row" style="margin-bottom:12px;">
                            <div style="display:flex;align-items:center;gap:12px;min-width:0;">
                                <div style="width:52px;height:52px;flex-shrink:0;">${avatarMarkup(member.displayName || member.name || member.userId || '', member.avatar || '', member.initials || '')}</div>
                                <div style="min-width:0;">
                                    <div class="ui-row-title" style="font-size:14px;">${escapeHtml(member.displayName || member.name || member.userId || '')}</div>
                                    <div class="ui-row-subtitle">${escapeHtml(getGroupRoleLabel(member.role))}</div>
                                </div>
                            </div>
                        </div>

                        ${renderGroupRestrictionSummaryCard(restriction)}

                        <label class="ui-row" style="cursor:${canToggleAdmin ? 'pointer' : 'default'};opacity:${canToggleAdmin ? '1' : '0.75'};">
                            <div>
                                <div class="ui-row-title">${member.role === 'admin' ? 'Администратор' : 'Сделать администратором'}</div>
                                <div class="ui-row-subtitle">${canToggleAdmin ? 'Владелец может назначать/снимать администратора.' : 'Доступно только владельцу чата.'}</div>
                            </div>
                            <div class="ui-row-actions">
                                <input id="groupAdminToggle" type="checkbox" ${member.role === 'admin' ? 'checked' : ''} ${canToggleAdmin ? '' : 'disabled'} style="accent-color:#7c5cff;width:18px;height:18px;">
                            </div>
                        </label>

                        <div class="ui-row" style="margin-top:12px;margin-bottom:10px;">
                            <div>
                                <div class="ui-row-title">Действие</div>
                                <div class="ui-row-subtitle">Выбери режим: без наказания, мьют, блокировка или исключение.</div>
                            </div>
                        </div>

                        <div class="ui-radio-list" style="margin-bottom:12px;">
                            <label class="ui-radio-item" onclick="onGroupMemberActionPick(event,'none')">
                                <div class="ui-radio-left">
                                    <div class="ui-radio-title"><i class="fas fa-shield-alt" style="opacity:.9;"></i> Без наказания</div>
                                    <div class="ui-radio-subtitle">Ничего не применять, только сохранить роль.</div>
                                </div>
                                <div class="ui-radio-right">
                                    <input type="radio" name="groupActionMode" value="none" checked>
                                </div>
                            </label>
                            <label class="ui-radio-item" onclick="onGroupMemberActionPick(event,'${escapeHtml(muteActionValue)}')">
                                <div class="ui-radio-left">
                                    <div class="ui-radio-title"><i class="fas fa-volume-mute" style="opacity:.9;"></i> ${escapeHtml(muteActionLabel)}</div>
                                    <div class="ui-radio-subtitle">${muteActionValue === 'mute' ? 'Ограничить отправку сообщений на время.' : 'Снять ограничение на сообщения.'}</div>
                                </div>
                                <div class="ui-radio-right">
                                    <input type="radio" name="groupActionMode" value="${escapeHtml(muteActionValue)}">
                                </div>
                            </label>
                            <label class="ui-radio-item" onclick="onGroupMemberActionPick(event,'${escapeHtml(banActionValue)}')">
                                <div class="ui-radio-left">
                                    <div class="ui-radio-title"><i class="fas fa-ban" style="opacity:.9;"></i> ${escapeHtml(banActionLabel)}</div>
                                    <div class="ui-radio-subtitle">${banActionValue === 'ban' ? 'Запретить доступ к чату на время.' : 'Снять блокировку доступа к чату.'}</div>
                                </div>
                                <div class="ui-radio-right">
                                    <input type="radio" name="groupActionMode" value="${escapeHtml(banActionValue)}">
                                </div>
                            </label>
                            <label class="ui-radio-item" onclick="onGroupMemberActionPick(event,'kick')">
                                <div class="ui-radio-left">
                                    <div class="ui-radio-title"><i class="fas fa-user-slash" style="opacity:.9;"></i> Исключение</div>
                                    <div class="ui-radio-subtitle">Удалить участника из чата.</div>
                                </div>
                                <div class="ui-radio-right">
                                    <input type="radio" name="groupActionMode" value="kick">
                                </div>
                            </label>
                        </div>

                        <div id="groupActionDurationWrap" style="display:none;">
                            <div class="ui-row" style="margin-bottom:10px;">
                                <div>
                                    <div class="ui-row-title">Срок</div>
                                    <div class="ui-row-subtitle">Для мьюта/блокировки можно выбрать длительность.</div>
                                </div>
                            </div>
                            <div class="ui-input-grid" style="margin-bottom:10px;">
                                <input id="groupActionDurationValue" class="modal-input" type="number" min="1" value="1" placeholder="Срок" style="margin:0;text-align:left;">
                                <select id="groupActionDurationUnit" class="modal-input" style="margin:0;text-align:left;">
                                    <option value="minutes">Минуты</option>
                                    <option value="hours">Часы</option>
                                    <option value="days">Дни</option>
                                    <option value="forever">Навсегда</option>
                                </select>
                            </div>
                        </div>

                        <input id="groupActionReasonInput" class="modal-input" placeholder="Причина" style="margin:0;text-align:left;">
                    </div>
                    <div class="modal-buttons" style="flex-shrink:0;">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerGroupMemberActionModal')">Отмена</button>
                        <button type="button" class="modal-btn confirm" onclick="submitGroupMemberAction('${escapeHtml(chatId)}','${escapeHtml(targetUserId)}')">Применить</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
            toggleGroupMemberActionFields();
        }

        function onGroupMemberActionPick(event, nextValue) {
            try {
                event?.preventDefault?.();
                event?.stopPropagation?.();
            } catch (_) {}
            const modal = document.getElementById('messengerGroupMemberActionModal');
            if (!modal) return false;
            const current = String(modal.querySelector('input[name="groupActionMode"]:checked')?.value || 'none');
            const picked = String(nextValue || 'none');
            const next = current === picked && picked !== 'none' ? 'none' : picked;
            modal.querySelectorAll('input[name="groupActionMode"]').forEach((el) => {
                el.checked = String(el.value || '') === next;
            });
            toggleGroupMemberActionFields();
            return false;
        }

        function toggleGroupMemberActionFields() {
            const modal = document.getElementById('messengerGroupMemberActionModal');
            if (!modal) return;
            const action = String(modal.querySelector('input[name="groupActionMode"]:checked')?.value || 'none');
            const durationWrap = modal.querySelector('#groupActionDurationWrap');
            const reasonInput = modal.querySelector('#groupActionReasonInput');
            if (durationWrap) durationWrap.style.display = action === 'mute' || action === 'ban' ? '' : 'none';
            const list = modal.querySelector('.ui-radio-list');
            if (list) {
                const items = Array.from(list.querySelectorAll('.ui-radio-item'));
                if (action && action !== 'none') {
                    items.forEach((label) => {
                        const v = String(label.querySelector('input[name="groupActionMode"]')?.value || '');
                        label.style.display = v === action ? '' : 'none';
                    });
                } else {
                    items.forEach((label) => {
                        label.style.display = '';
                    });
                }
            }
            if (reasonInput) {
                reasonInput.placeholder = action === 'kick'
                    ? 'Причина исключения'
                    : action === 'ban'
                        ? 'Причина блокировки'
                        : action === 'mute'
                            ? 'Причина мьюта'
                            : action === 'unban'
                                ? 'Причина снятия блокировки'
                                : action === 'unmute'
                                    ? 'Причина снятия мута'
                                    : 'Причина';
            }
        }

        function submitGroupMemberAction(chatId, targetUserId) {
            const modal = document.getElementById('messengerGroupMemberActionModal');
            if (!modal) return;
            const chat = findMessengerChatById(chatId);
            const member = (chat?.group?.participants || []).find((item) => String(item?.userId || '') === String(targetUserId || ''));
            const adminToggle = !!modal.querySelector('#groupAdminToggle')?.checked;
            const selectedAction = String(modal.querySelector('input[name="groupActionMode"]:checked')?.value || 'none');
            const reason = String(modal.querySelector('#groupActionReasonInput')?.value || '').trim();
            const durationValue = Number(modal.querySelector('#groupActionDurationValue')?.value || 1);
            const durationUnit = String(modal.querySelector('#groupActionDurationUnit')?.value || 'minutes');
            const roleWasAdmin = String(member?.role || '') === 'admin';
            const canToggleAdmin = String(chat?.group?.myRole || '') === 'owner';
            if (canToggleAdmin && roleWasAdmin !== adminToggle) {
                sendMessengerEvent({ type: 'messenger-group-member-action', chatId, targetUserId, action: 'toggle-admin', enabled: adminToggle });
            }
            if (selectedAction !== 'none') {
                sendMessengerEvent({
                    type: 'messenger-group-member-action',
                    chatId,
                    targetUserId,
                    action: selectedAction,
                    durationValue,
                    durationUnit,
                    reason
                });
            }
            if ((roleWasAdmin === adminToggle || !canToggleAdmin) && selectedAction === 'none') {
                showNotification('Группа', 'Выберите действие для участника', 'warning');
                return;
            }
            modal.remove();
        }

        function openGroupInvitePreviewModal(group, inviteCode, canJoin) {
            closeTransientModal('messengerGroupInvitePreviewModal');
            const model = buildGroupChatClientModel(group);
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerGroupInvitePreviewModal';
            modal.innerHTML = `
                <div class="modal-content" style="max-width:520px;text-align:left;">
                    <div style="display:flex;justify-content:center;margin-bottom:14px;"><div style="width:88px;height:88px;">${avatarMarkup(model?.peer?.displayName || group.title || 'Групповой чат', group.avatar || '', model?.peer?.initials || '')}</div></div>
                    <div style="text-align:center;font-size:22px;font-weight:800;margin-bottom:8px;">${escapeHtml(group.title || 'Групповой чат')}</div>
                    <div style="text-align:center;opacity:.78;margin-bottom:14px;">${escapeHtml(`${Array.isArray(group.members) ? group.members.length : 0} участников`)}</div>
                    <div class="contact-item" style="justify-content:flex-start;margin-bottom:14px;"><div><div class="contact-chat">Описание</div><div class="contact-name">${escapeHtml(group.description || 'Без описания')}</div></div></div>
                    <div class="modal-buttons">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerGroupInvitePreviewModal')">Отмена</button>
                        <button type="button" class="modal-btn confirm" ${canJoin ? '' : 'disabled'} onclick="joinGroupByInvite('${escapeHtml(inviteCode || '')}')">${canJoin ? 'Присоединиться' : 'Вы уже в чате'}</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
        }

        function joinGroupByInvite(inviteCode) {
            sendMessengerEvent({ type: 'messenger-join-group-by-invite', inviteCode });
            closeTransientModal('messengerGroupInvitePreviewModal');
        }

        function openGroupProfileModal(chatId) {
            const chat = findMessengerChatById(chatId);
            if (!chat || !isGroupMessengerChat(chat)) return;
            closeTransientModal('messengerGroupProfileModal');
            const participants = Array.isArray(chat.group?.participants) ? chat.group.participants : [];
            const canEditInfo = hasGroupPermissionClient(chat, 'editInfo');
            const canAddMembers = hasGroupPermissionClient(chat, 'addMembers');
            const canCreateCalls = hasGroupPermissionClient(chat, 'createCalls');
            const isOwner = String(chat.group?.myRole || '') === 'owner';
            const inviteUrl = getGroupInviteUrl(chat);
            const canSeeLink = hasGroupPermissionClient(chat, 'linkAccess');
            const membersHtml = participants.length
                ? participants.map((member) => {
                    const memberId = String(member.userId || '').trim();
                    const canOpenProfile = !!memberId;
                    const memberClick = canOpenProfile
                        ? `onclick="closeTransientModal('messengerGroupProfileModal'); openUserProfile('${escapeHtml(memberId)}')"`
                        : '';
                    const roleLabel = getGroupRoleLabel(member?.role);
                    const presence = getParticipantPresenceState(member);
                    const onlineLabel = presence.online ? 'В сети' : 'Не в сети';
                    const lastSeenLabel = !presence.online && Number(presence.lastSeenAt || 0) > 0
                        ? `Был(а): ${new Date(Number(presence.lastSeenAt)).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })}`
                        : '';
                    return `
                    <div class="contact-item" style="justify-content:flex-start;gap:12px;">
                        <div style="display:flex;align-items:center;gap:12px;min-width:0;flex:1;cursor:${canOpenProfile ? 'pointer' : 'default'};" ${memberClick}>
                            <div style="width:44px;height:44px;flex-shrink:0;">${avatarMarkup(member.displayName || member.name || member.userId || '', member.avatar || '', member.initials || '')}</div>
                            <div style="min-width:0;flex:1;">
                            <div class="contact-name">${escapeHtml(member.displayName || member.name || member.userId || '')}</div>
                            <div class="contact-chat">${escapeHtml(`Роль: ${roleLabel}`)}</div>
                            <div class="contact-chat">${escapeHtml(`Статус сети: ${onlineLabel}`)}</div>
                            ${lastSeenLabel ? `<div class="contact-chat">${escapeHtml(lastSeenLabel)}</div>` : ''}
                            ${getGroupRestrictionStatusText(member.restriction) ? `<div class="contact-chat">${escapeHtml(getGroupRestrictionStatusText(member.restriction))}</div>` : ''}
                            </div>
                        </div>
                        ${canManageGroupMemberClient(chat, member) ? `<button type="button" class="messenger-nav-btn" onclick="event.stopPropagation(); openGroupMemberActionModal('${escapeHtml(chatId)}','${escapeHtml(member.userId || '')}')"><i class="fas fa-ellipsis-v"></i></button>` : ''}
                    </div>`;
                }).join('')
                : '<div class="friends-empty">Участники не найдены</div>';
            const modal = document.createElement('div');
            modal.className = 'modal';
            modal.id = 'messengerGroupProfileModal';
            modal.innerHTML = `
                <div class="modal-content modal-sheet modal-sheet--pc-dialog">
                    <div class="modal-sheet-header">
                        <div class="modal-sheet-title"><i class="fas fa-users"></i><span>Информация о чате</span></div>
                        <button type="button" class="modal-sheet-close" onclick="closeTransientModal('messengerGroupProfileModal')" aria-label="Закрыть"><i class="fas fa-times"></i></button>
                    </div>
                    <div class="modal-sheet-body">
                        <div class="contact-item" style="display:flex;align-items:center;gap:14px;padding:14px;">
                            <div style="width:68px;height:68px;flex-shrink:0;border-radius:18px;overflow:hidden;border:1px solid rgba(255,255,255,0.12);">${avatarMarkup(chat.peer?.displayName || chat.peer?.name || 'Групповой чат', chat.peer?.avatar || '', chat.peer?.initials || '')}</div>
                            <div style="min-width:0;flex:1;">
                                <div style="font-size:16px;font-weight:900;letter-spacing:.2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${escapeHtml(chat.peer?.displayName || chat.peer?.name || 'Групповой чат')}</div>
                                <div style="font-size:12px;opacity:.78;margin-top:2px;">${escapeHtml(getGroupChatStatusText(chat))}</div>
                            </div>
                        </div>

                        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:12px;">
                            ${isOwner ? `<button type="button" class="contact-btn" onclick="openGroupSettingsModal('${escapeHtml(chatId)}')"><i class="fas fa-cog"></i> Настройки</button>` : ''}
                            ${canEditInfo ? `<button type="button" class="contact-btn" onclick="openGroupEditModal('${escapeHtml(chatId)}')"><i class="fas fa-pen"></i> Изменить</button>` : ''}
                            ${canAddMembers ? `<button type="button" class="contact-btn" onclick="openAddMembersToGroupModal('${escapeHtml(chatId)}')"><i class="fas fa-user-plus"></i> Добавить</button>` : ''}
                            ${canCreateCalls ? `<button type="button" class="contact-btn" onclick="startGroupCallForChat('${escapeHtml(chatId)}')"><i class="fas fa-phone"></i> Звонок</button>` : ''}
                            <button type="button" class="contact-btn delete" onclick="leaveGroupChat('${escapeHtml(chatId)}')"><i class="fas fa-sign-out-alt"></i> Выйти</button>
                        </div>

                        <div style="display:grid;gap:10px;margin-top:12px;">
                            <div class="contact-item" style="justify-content:flex-start;padding:14px;"><div><div class="contact-chat">Описание</div><div class="contact-name">${escapeHtml(chat.group?.description || 'Без описания')}</div></div></div>
                            <div class="contact-item" style="justify-content:flex-start;padding:14px;cursor:${canSeeLink && inviteUrl ? 'pointer' : 'default'};" ${canSeeLink && inviteUrl ? `onclick="copyGroupInviteLink('${escapeHtml(inviteUrl)}')"` : ''}><div><div class="contact-chat">Ссылка</div><div class="contact-name">${escapeHtml(canSeeLink ? (inviteUrl || 'Будет сгенерирована автоматически') : 'Недоступно по настройкам')}</div></div></div>
                        </div>

                        <div style="font-size:13px;font-weight:900;margin:14px 4px 8px;">Участники</div>
                        <div class="contacts-list" style="max-height:320px;">${membersHtml}</div>
                    </div>
                    <div class="modal-buttons" style="flex-shrink:0;">
                        <button type="button" class="modal-btn cancel" onclick="closeTransientModal('messengerGroupProfileModal')">Закрыть</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
        }

        function leaveGroupChat(chatId) {
            showCustomConfirm('Выйти из чата', 'Вы действительно хотите выйти из группы?', () => {
                sendMessengerEvent({ type: 'messenger-update-group', chatId, action: 'removeSelf' });
                closeTransientModal('messengerGroupProfileModal');
                closeMobileChatView();
            });
        }

        function rejoinGroupChat(chatId) {
            sendMessengerEvent({ type: 'messenger-update-group', chatId, action: 'rejoin' });
        }

        function closeMobileChatView() {
            lastActiveChatId = messengerActiveChatId;
            lastActivePeerId = messengerActivePeerId;
            isChatOpen = false;
            messengerActiveChatId = '';
            messengerActivePeerId = '';
            persistMessengerSessionChat('');
            persistMessengerSessionPeer('');
            renderMainScreen();
        }

        function openReturnToChatButton() {
            if (!lastActiveChatId) return;
            isChatOpen = true;
            messengerActiveChatId = lastActiveChatId;
            messengerActivePeerId = lastActivePeerId;
            renderMainScreen();
        }

        function resolveChatMessages(chatId) {
            const list = messengerMessages.get(chatId);
            return Array.isArray(list) ? list : [];
        }

        // Проверяет, есть ли в чате хотя бы одно несистемное сообщение (не system-блок, не "История очищена")
        function hasRealMessages(chatId) {
            const cid = String(chatId || '');
            const cached = _hasRealMsgCache.get(cid);
            if (typeof cached === 'boolean') return cached;
            const msgs = resolveChatMessages(cid);
            const result = msgs.some(m => {
                const kind = String(m?.messageKind || m?.kind || '');
                if (kind === 'system') return false;
                const t = String(m?.text || '').trim();
                if (t === 'История очищена' || t === 'История чата очищена') return false;
                return true;
            });
            _hasRealMsgCache.set(cid, result);
            return result;
        }

        function makeClientMessageId() {
            try {
                const id = (window.crypto && typeof window.crypto.randomUUID === 'function') ? window.crypto.randomUUID() : `${Date.now()}_${Math.random()}`;
                return `tmp_${String(id).replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 80)}`;
            } catch (_) {
                return `tmp_${Date.now()}`;
            }
        }

        function upsertMessengerMessage(chatId, msg) {
            if (!chatId || !msg || !msg.id) return;
            const prev = messengerMessages.get(chatId) || [];
            const next = [...prev];
            const idx = next.findIndex((x) => x && x.id === msg.id);
            if (idx >= 0) next[idx] = msg;
            else next.push(msg);
            messengerMessages.set(chatId, next.slice(-300));
        }

        function sendMessageFromComposer() {
            if (messengerComposeBlocked) {
                showNotification('Мессенджер', messengerComposeHint || 'Отправка недоступна', 'warning');
                return;
            }
            // При отправке своего сообщения — всегда прокручиваем вниз.
            messengerNewWhileScrolledCount = 0;
            updateMessengerNewWhileScrolledFabUI();
            messengerShouldAutoScroll = true;
            const input = document.getElementById('chatComposerInput');
            const shouldKeepComposerFocus = !!(input && (document.activeElement === input || isComposerFocusLockActive()));
            const text = String(input?.value || '').trim();
            const activeChat = resolveActiveMessengerChat();
            if (!activeChat) return;
            if (!text && !composerMediaDraft && !composerEditMessageId) return;
            if (shouldKeepComposerFocus && !isMobileLayout()) {
                messengerSuppressBlurUntil = Date.now() + 500;
                armComposerFocusLock(isMobileLayout() ? 1600 : 1400);
            }
            const paintAfterSend = () => {
                if (!shouldRenderMessengerUi()) return;
                if (isMobileLayout() && shouldKeepComposerFocus) {
                    refreshMessengerChatHistoryOnly();
                    return;
                }
                if (shouldDeferTransientMessengerRender()) {
                    messengerRenderPendingAfterScroll = true;
                    return;
                }
                requestAnimationFrame(() => renderMainScreen());
            };
            let sent = false;
            if (composerEditMessageId) {
                if (!text) return;
                sent = sendMessengerEvent({ type: 'messenger-edit', messageId: composerEditMessageId, text });
            } else if (composerMediaDraft && composerMediaDraft.b64) {
                const clientMessageId = makeClientMessageId();
                const chatId = messengerActiveChatId;
                const meId = authProfile?.appUserId || '';
                const kind = String(composerMediaDraft.kind || '').trim();
                const replyTo = composerReplyMessage?.id || '';
                if (!chatId || !meId || !['image', 'video', 'voice'].includes(kind)) return;

                const pending = {
                    id: clientMessageId,
                    chatId,
                    fromId: meId,
                    toId: isGroupMessengerChat(activeChat) ? chatId : messengerActivePeerId,
                    text,
                    messageKind: kind,
                    createdAt: Date.now(),
                    editedAt: 0,
                    deletedAt: 0,
                    replyTo,
                    forwardedFromMessageId: '',
                    uploading: true,
                    uploadProgress: 100,
                    audioBase64: '',
                    audioMime: '',
                    imageBase64: '',
                    mimeType: '',
                    videoBase64: '',
                    videoMime: ''
                };

                if (kind === 'image') {
                    pending.imageBase64 = composerMediaDraft.b64;
                    pending.mimeType = composerMediaDraft.mime || 'image/jpeg';
                } else if (kind === 'video') {
                    pending.videoBase64 = composerMediaDraft.b64;
                    pending.videoMime = composerMediaDraft.mime || 'video/mp4';
                } else {
                    pending.audioBase64 = composerMediaDraft.b64;
                    pending.audioMime = composerMediaDraft.mime || 'audio/webm';
                }

                upsertMessengerMessage(chatId, pending);
                paintAfterSend();

                const payload = {
                    type: 'messenger-send',
                    clientMessageId,
                    chatId: activeChat.id,
                    text,
                    messageKind: kind,
                    replyTo
                };
                if (isDirectMessengerChat(activeChat)) payload.to = messengerActivePeerId;
                if (kind === 'image') {
                    payload.imageBase64 = composerMediaDraft.b64;
                    payload.mimeType = pending.mimeType;
                } else if (kind === 'video') {
                    payload.videoBase64 = composerMediaDraft.b64;
                    payload.videoMime = pending.videoMime;
                } else {
                    payload.audioBase64 = composerMediaDraft.b64;
                    payload.mimeType = pending.audioMime;
                }
                sent = sendMessengerEvent(payload);
            } else {
                const clientMessageId = makeClientMessageId();
                const chatId = messengerActiveChatId;
                const meId = authProfile?.appUserId || '';
                if (chatId && meId && text) {
                    upsertMessengerMessage(chatId, {
                        id: clientMessageId,
                        chatId,
                        fromId: meId,
                        toId: isGroupMessengerChat(activeChat) ? chatId : messengerActivePeerId,
                        text,
                        messageKind: 'text',
                        createdAt: Date.now(),
                        editedAt: 0,
                        deletedAt: 0,
                        replyTo: composerReplyMessage?.id || '',
                        forwardedFromMessageId: '',
                        uploading: true,
                        uploadProgress: 0
                    });
                    paintAfterSend();
                }
                const payload = {
                    type: 'sendMessage',
                    chatId,
                    text,
                    replyTo: composerReplyMessage?.id || '',
                    clientMessageId
                };
                if (isDirectMessengerChat(activeChat)) payload.to = messengerActivePeerId;
                sent = sendMessengerEvent(payload);
            }
            if (!sent) return;
            if (input) input.value = '';
            composerDraftByPeerId.set(messengerActiveChatId || messengerActivePeerId, '');
            lastComposerTypingEmit = 0;
            sendMessengerEvent(
                isGroupMessengerChat(activeChat)
                    ? { type: 'messenger-typing', chatId: activeChat.id, isTyping: false }
                    : { type: 'messenger-typing', toUserId: messengerActivePeerId, isTyping: false }
            );
            composerReplyMessage = null;
            composerEditMessageId = '';
            composerMediaDraft = null;
            const refocusComposer = () => {
                const ta = document.getElementById('chatComposerInput');
                if (!ta || messengerView !== 'chats' || !messengerActiveChatId) return;
                onComposerInput();
                if (!shouldKeepComposerFocus) return;
                try {
                    ta.focus({ preventScroll: true });
                } catch (_) {
                    ta.focus();
                }
            };
            if (shouldKeepComposerFocus && !isMobileLayout()) {
                requestAnimationFrame(refocusComposer);
            }
        }

        function onComposerKeydown(event) {
            if (!event) return;
            if (composerMentionState?.open) {
                if (event.key === 'ArrowDown') {
                    event.preventDefault();
                    const len = Array.isArray(composerMentionState.candidates) ? composerMentionState.candidates.length : 0;
                    if (len) {
                        composerMentionState.activeIndex = (Number(composerMentionState.activeIndex || 0) + 1) % len;
                        syncComposerMentionMenuDom(resolveActiveMessengerChat());
                    }
                    return;
                }
                if (event.key === 'ArrowUp') {
                    event.preventDefault();
                    const len = Array.isArray(composerMentionState.candidates) ? composerMentionState.candidates.length : 0;
                    if (len) {
                        composerMentionState.activeIndex = (Number(composerMentionState.activeIndex || 0) - 1 + len) % len;
                        syncComposerMentionMenuDom(resolveActiveMessengerChat());
                    }
                    return;
                }
                if (event.key === 'Escape') {
                    composerMentionState.open = false;
                    syncComposerMentionMenuDom(resolveActiveMessengerChat());
                    return;
                }
                if (event.key === 'Enter' && !event.shiftKey) {
                    event.preventDefault();
                    const idx = Math.max(0, Number(composerMentionState.activeIndex || 0));
                    const cand = Array.isArray(composerMentionState.candidates) ? composerMentionState.candidates[idx] : null;
                    if (cand?.username) {
                        selectComposerMention(cand.username);
                    } else {
                        composerMentionState.open = false;
                        syncComposerMentionMenuDom(resolveActiveMessengerChat());
                    }
                    return;
                }
            }
            if (event.key !== 'Enter') return;
            // На телефоне отправляем только кнопкой, чтобы `Enter` не конфликтовал с переносом/UX.
            if (isMobileLayout()) return;
            if (event.shiftKey) return; // Shift+Enter => новая строка (по ТЗ).
            event.preventDefault();
            sendMessageFromComposer();
        }

        function renderComposerMentionMenu(chat) {
            if (!composerMentionState?.open) return '';
            if (!chat || !isGroupMessengerChat(chat)) return '';
            const candidates = Array.isArray(composerMentionState.candidates) ? composerMentionState.candidates : [];
            if (!candidates.length) return '';
            const activeIdx = Math.max(0, Number(composerMentionState.activeIndex || 0));
            const items = candidates.map((c, idx) => {
                const name = String(c.displayName || c.name || c.userId || '').trim() || String(c.userId || '');
                const uname = String(c.username || '').trim();
                return `<div class="composer-mention-item ${idx === activeIdx ? 'active' : ''}" onclick="selectComposerMention('${escapeHtml(uname)}')">
                    <div style="width:34px;height:34px;flex-shrink:0;">${avatarMarkup(name, c.avatar || '', c.initials || '')}</div>
                    <div class="composer-mention-meta">
                        <div class="composer-mention-name">${escapeHtml(name)}</div>
                        <div class="composer-mention-username">@${escapeHtml(uname)}</div>
                    </div>
                </div>`;
            }).join('');
            return `<div id="composerMentionMenu" class="composer-mention-menu" style="display:block;">${items}</div>`;
        }

        function closeComposerMentionMenu() {
            if (!composerMentionState) return;
            composerMentionState.open = false;
            composerMentionState.candidates = [];
            composerMentionState.activeIndex = 0;
            composerMentionState.query = '';
            composerMentionState.atIndex = -1;
            composerMentionState.endIndex = -1;
        }

        function updateComposerMentionMenu() {
            const prevOpen = !!composerMentionState?.open;
            const prevQuery = String(composerMentionState?.query || '');
            const prevKey = Array.isArray(composerMentionState?.candidates)
                ? composerMentionState.candidates.map((c) => String(c?.username || '')).join('|')
                : '';
            const prevAt = Number(composerMentionState?.atIndex || -1);
            const prevEnd = Number(composerMentionState?.endIndex || -1);
            const input = document.getElementById('chatComposerInput');
            const activeChat = resolveActiveMessengerChat();
            if (!input || !activeChat || !isGroupMessengerChat(activeChat)) {
                closeComposerMentionMenu();
                return prevOpen;
            }
            const value = String(input.value || '');
            const cursor = typeof input.selectionStart === 'number' ? input.selectionStart : value.length;
            const atIndex = value.lastIndexOf('@', Math.max(0, cursor - 1));
            if (atIndex < 0) {
                closeComposerMentionMenu();
                return prevOpen;
            }
            const before = atIndex > 0 ? value.charAt(atIndex - 1) : '';
            if (before && !/\s/.test(before)) {
                closeComposerMentionMenu();
                return prevOpen;
            }
            const query = value.slice(atIndex + 1, cursor);
            if (/\s/.test(query) || /[^a-zA-Z0-9]/.test(query)) {
                closeComposerMentionMenu();
                return prevOpen;
            }
            const q = String(query || '').toLowerCase();
            const members = getGroupChatParticipants(activeChat);
            const candidates = members
                .map((m) => {
                    const uid = String(m?.userId || m?.id || '').trim();
                    const peer = uid ? resolvePeerDisplay(uid) : null;
                    const username = String(m?.username || peer?.username || '').replace(/^@+/, '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
                    if (!username) return null;
                    const displayName = String(m?.displayName || peer?.displayName || peer?.name || uid || '').trim();
                    return {
                        userId: uid,
                        displayName,
                        name: String(peer?.name || displayName || uid || ''),
                        username,
                        avatar: String(m?.avatar || peer?.avatar || ''),
                        initials: String(m?.initials || peer?.initials || '')
                    };
                })
                .filter(Boolean)
                .filter((c) => !q || String(c.username || '').toLowerCase().includes(q) || String(c.displayName || '').toLowerCase().includes(q));
            candidates.sort((a, b) => {
                const au = String(a.username || '').toLowerCase();
                const bu = String(b.username || '').toLowerCase();
                const aStarts = q && au.startsWith(q);
                const bStarts = q && bu.startsWith(q);
                if (aStarts !== bStarts) return aStarts ? -1 : 1;
                return au.localeCompare(bu, 'ru-RU');
            });
            const limited = candidates.slice(0, 12);
            if (!limited.length) {
                closeComposerMentionMenu();
                return prevOpen;
            }
            composerMentionState.open = true;
            composerMentionState.query = q;
            composerMentionState.candidates = limited;
            composerMentionState.activeIndex = Math.min(Math.max(0, Number(composerMentionState.activeIndex || 0)), limited.length - 1);
            composerMentionState.atIndex = atIndex;
            composerMentionState.endIndex = cursor;
            const nextKey = limited.map((c) => String(c?.username || '')).join('|');
            return !prevOpen || prevQuery !== q || prevKey !== nextKey || prevAt !== atIndex || prevEnd !== cursor;
        }

        function selectComposerMention(username) {
            const uname = String(username || '').replace(/^@+/, '').trim();
            if (!uname) return;
            const input = document.getElementById('chatComposerInput');
            if (!input) return;
            const value = String(input.value || '');
            const atIndex = Number(composerMentionState?.atIndex || -1);
            const endIndex = Number(composerMentionState?.endIndex || -1);
            const start = atIndex >= 0 ? atIndex : value.lastIndexOf('@');
            const end = endIndex >= 0 ? endIndex : (typeof input.selectionStart === 'number' ? input.selectionStart : value.length);
            if (start < 0 || end < start) return;
            const insert = `@${uname} `;
            const next = value.slice(0, start) + insert + value.slice(end);
            input.value = next;
            const pos = start + insert.length;
            try {
                input.focus();
                input.setSelectionRange(pos, pos);
            } catch (_) {}
            const draftKey = messengerActiveChatId || messengerActivePeerId;
            if (draftKey) composerDraftByPeerId.set(draftKey, input.value);
            closeComposerMentionMenu();
            onComposerInput();
            syncComposerMentionMenuDom(resolveActiveMessengerChat());
        }

        function preserveComposerFocusOnSend() {
            if (!isMobileLayout()) return;
            const input = document.getElementById('chatComposerInput');
            if (!input) return;
            // На мобиле не форсим refocus, чтобы не было эффекта "клавиатура закрылась и сразу открылась".
            messengerSuppressBlurUntil = 0;
            messengerComposerFocusLockUntil = 0;
        }

        function onComposerInput() {
            const input = document.getElementById('chatComposerInput');
            const hasText = !!String(input?.value || '').trim();
            const actionBtn = document.getElementById('chatComposerActionBtn');
            if (actionBtn) {
                if (voiceRecordingActive) {
                    actionBtn.innerHTML = '<i class="fas fa-stop"></i>';
                } else if (voiceRecordPreview) {
                    actionBtn.innerHTML = '<i class="fas fa-paper-plane"></i>';
                } else {
                    actionBtn.innerHTML = hasText ? '<i class="fas fa-paper-plane"></i>' : '<i class="fas fa-microphone"></i>';
                }
            }
            const activeChat = resolveActiveMessengerChat();
            const draftKey = messengerActiveChatId || messengerActivePeerId;
            if (draftKey && input) {
                composerDraftByPeerId.set(draftKey, input.value);
            }
            if (activeChat) {
                const now = Date.now();
                if (!hasText) {
                    lastComposerTypingEmit = 0;
                    sendMessengerEvent(
                        isGroupMessengerChat(activeChat)
                            ? { type: 'messenger-typing', chatId: activeChat.id, isTyping: false }
                            : { type: 'messenger-typing', toUserId: messengerActivePeerId, isTyping: false }
                    );
                } else if (now - lastComposerTypingEmit > 850) {
                    lastComposerTypingEmit = now;
                    sendMessengerEvent(
                        isGroupMessengerChat(activeChat)
                            ? { type: 'messenger-typing', chatId: activeChat.id, isTyping: true }
                            : { type: 'messenger-typing', toUserId: messengerActivePeerId, isTyping: true }
                    );
                }
            }
            const mentionChanged = updateComposerMentionMenu();
            if (mentionChanged && messengerView === 'chats') syncComposerMentionMenuDom(resolveActiveMessengerChat());
        }

        function clearVoiceRecTimerUi() {
            if (voiceRecTimerInterval) {
                clearInterval(voiceRecTimerInterval);
                voiceRecTimerInterval = null;
            }
        }

        function stopVoiceStreams() {
            clearVoiceRecTimerUi();
            try {
                if (voiceMediaStream) {
                    voiceMediaStream.getTracks().forEach((t) => t.stop());
                }
            } catch (_) {}
            voiceMediaStream = null;
            voiceMediaRecorder = null;
            voiceRecordChunks = [];
            voiceRecordingActive = false;
            voiceRecordStartedAt = 0;
        }

        function discardVoicePreview() {
            if (voicePreviewAudioEl) {
                try {
                    voicePreviewAudioEl.pause();
                } catch (_) {}
            }
            if (voiceRecordPreview?.url) {
                try {
                    URL.revokeObjectURL(voiceRecordPreview.url);
                } catch (_) {}
            }
            voiceRecordPreview = null;
        }

        function toggleVoicePreviewPlay(btn) {
            if (!voiceRecordPreview?.url) return;
            if (!voicePreviewAudioEl) voicePreviewAudioEl = new Audio();
            const icon = btn && btn.querySelector ? btn.querySelector('i') : null;
            voicePreviewAudioEl.src = voiceRecordPreview.url;
            if (voicePreviewAudioEl.paused) {
                voicePreviewAudioEl.play().catch(() => {});
                if (icon) icon.className = 'fas fa-pause';
            } else {
                voicePreviewAudioEl.pause();
                voicePreviewAudioEl.currentTime = 0;
                if (icon) icon.className = 'fas fa-play';
            }
            voicePreviewAudioEl.onended = () => {
                if (icon) icon.className = 'fas fa-play';
            };
        }

        async function startVoiceRecording() {
            const blockedPeer = (messengerProfile.blacklist || []).includes(String(messengerActivePeerId || ''));
            const activeChat = resolveActiveMessengerChat();
            if (messengerComposeBlocked || (isDirectMessengerChat(activeChat) && blockedPeer) || !messengerActiveChatId) {
                showNotification('Мессенджер', messengerComposeHint || 'Запись недоступна', 'warning');
                return;
            }
            if (voiceRecordingActive) return;
            discardVoicePreview();
            try {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                voiceMediaStream = stream;
                voiceRecordChunks = [];
                const mr = new MediaRecorder(stream, { mimeType: MediaRecorder.isTypeSupported('audio/webm') ? 'audio/webm' : 'audio/mp4' });
                voiceMediaRecorder = mr;
                mr.ondataavailable = (e) => {
                    if (e.data && e.data.size) voiceRecordChunks.push(e.data);
                };
                mr.start(120);
                voiceRecordingActive = true;
                voiceRecordStartedAt = Date.now();
                sendMessengerEvent(
                    isGroupMessengerChat(activeChat)
                        ? { type: 'messenger-typing', chatId: activeChat.id, isTyping: true, activity: 'voice' }
                        : { type: 'messenger-typing', toUserId: messengerActivePeerId, isTyping: true, activity: 'voice' }
                );
                renderMainScreen();
                clearVoiceRecTimerUi();
                voiceRecTimerInterval = setInterval(() => {
                    const el = document.getElementById('voiceRecTimerUi');
                    if (el && voiceRecordStartedAt) {
                        el.textContent = formatVoiceDurationMs(Date.now() - voiceRecordStartedAt);
                    }
                }, 400);
            } catch (err) {
                stopVoiceStreams();
                showNotification('Микрофон', 'Нет доступа к микрофону', 'error');
            }
        }

        function stopVoiceRecordingCapture() {
            const mr = voiceMediaRecorder;
            const activeChat = resolveActiveMessengerChat();
            if (!mr || !voiceRecordingActive) {
                if (activeChat) {
                    sendMessengerEvent(
                        isGroupMessengerChat(activeChat)
                            ? { type: 'messenger-typing', chatId: activeChat.id, isTyping: false, activity: 'voice' }
                            : { type: 'messenger-typing', toUserId: messengerActivePeerId, isTyping: false, activity: 'voice' }
                    );
                }
                stopVoiceStreams();
                renderMainScreen();
                return;
            }
            if (activeChat) {
                sendMessengerEvent(
                    isGroupMessengerChat(activeChat)
                        ? { type: 'messenger-typing', chatId: activeChat.id, isTyping: false, activity: 'voice' }
                        : { type: 'messenger-typing', toUserId: messengerActivePeerId, isTyping: false, activity: 'voice' }
                );
            }
            mr.onstop = () => {
                const blob = new Blob(voiceRecordChunks, { type: mr.mimeType || 'audio/webm' });
                const durationMs = Math.max(0, Date.now() - (voiceRecordStartedAt || Date.now()));
                clearVoiceRecTimerUi();
                stopVoiceStreams();
                if (blob.size < 48) {
                    showNotification('Мессенджер', 'Запись слишком короткая', 'warning');
                    renderMainScreen();
                    return;
                }
                const url = URL.createObjectURL(blob);
                voiceRecordPreview = {
                    blob,
                    mime: blob.type || mr.mimeType || 'audio/webm',
                    durationMs,
                    url
                };
                renderMainScreen();
            };
            try {
                if (typeof mr.requestData === 'function') mr.requestData();
                mr.stop();
            } catch (_) {
                stopVoiceStreams();
                renderMainScreen();
            }
        }

        function sendVoiceFromPreview() {
            const activeChat = resolveActiveMessengerChat();
            if (!voiceRecordPreview || !activeChat) return;
            const blockedPeer = (messengerProfile.blacklist || []).includes(String(messengerActivePeerId || ''));
            if (messengerComposeBlocked || (isDirectMessengerChat(activeChat) && blockedPeer)) {
                showNotification('Мессенджер', messengerComposeHint || 'Отправка недоступна', 'warning');
                return;
            }
            // При отправке своего сообщения — прокручиваем вниз.
            messengerNewWhileScrolledCount = 0;
            updateMessengerNewWhileScrolledFabUI();
            messengerShouldAutoScroll = true;
            const { blob, mime, durationMs } = voiceRecordPreview;
            const r = new FileReader();
            r.onloadend = () => {
                const raw = String(r.result || '');
                const base64 = raw.includes(',') ? raw.split(',')[1] : '';
                if (voicePreviewAudioEl) {
                    try {
                        voicePreviewAudioEl.pause();
                    } catch (_) {}
                }
                if (voiceRecordPreview?.url) {
                    try {
                        URL.revokeObjectURL(voiceRecordPreview.url);
                    } catch (_) {}
                }
                voiceRecordPreview = null;
                if (!base64) {
                    showNotification('Мессенджер', 'Не удалось подготовить аудио', 'warning');
                    renderMainScreen();
                    return;
                }
                const payload = {
                    type: 'messenger-send',
                    chatId: activeChat.id,
                    text: '',
                    messageKind: 'voice',
                    audioBase64: base64,
                    mimeType: mime || 'audio/webm',
                    durationMs
                };
                if (isDirectMessengerChat(activeChat)) payload.to = messengerActivePeerId;
                sendMessengerEvent(payload);
                composerDraftByPeerId.set(messengerActiveChatId || messengerActivePeerId, '');
                renderMainScreen();
            };
            r.readAsDataURL(blob);
        }

        function composerPrimaryAction(event) {
            const activeChat = resolveActiveMessengerChat();
            const blockedPeer = (messengerProfile.blacklist || []).includes(String(messengerActivePeerId || ''));
            if (messengerComposeBlocked || (isDirectMessengerChat(activeChat) && blockedPeer)) {
                showNotification('Мессенджер', messengerComposeHint || 'Отправка недоступна', 'warning');
                return;
            }
            const input = document.getElementById('chatComposerInput');
            const hasText = !!String(input?.value || '').trim();
            if (hasText || !!composerMediaDraft) {
                if (isMobileLayout()) preserveComposerFocusOnSend();
                sendMessageFromComposer();
                return;
            }
            if (voiceRecordPreview) {
                sendVoiceFromPreview();
                return;
            }
            if (voiceRecordingActive) {
                stopVoiceRecordingCapture();
                return;
            }
            startVoiceRecording();
        }

        function clearComposerMediaDraft() {
            composerMediaDraft = null;
            try {
                const inp = document.getElementById('chatMediaInput');
                if (inp) inp.value = '';
            } catch (_) {}
            if (shouldRenderMessengerUi()) renderMainScreen();
        }

        function openImageLightbox(dataUrl) {
            if (!dataUrl) return;
            const lb = document.createElement('div');
            lb.className = 'glass-media-lightbox';
            const close = document.createElement('button');
            close.type = 'button';
            close.className = 'lb-close';
            close.setAttribute('aria-label', 'Закрыть');
            close.innerHTML = '<i class="fas fa-times"></i>';
            const img = document.createElement('img');
            img.alt = '';
            img.src = dataUrl;
            img.draggable = false;
            img.oncontextmenu = (e) => {
                try { e.preventDefault(); } catch (_) {}
                return false;
            };
            lb.appendChild(close);
            lb.appendChild(img);
            lb.addEventListener('click', (e) => {
                if (e.target === lb || e.target.closest('.lb-close')) lb.remove();
            });
            document.body.appendChild(lb);
        }

        function openImageLightboxFromImg(imgEl) {
            const src = imgEl && imgEl.src ? imgEl.src : '';
            if (!src) return;
            openImageLightbox(src);
        }

        function openVideoLightboxFromMsg(chatId, msgId) {
            const cid = String(chatId || '');
            const mid = String(msgId || '');
            if (!cid || !mid) return;
            const list = messengerMessages.get(cid) || [];
            const msg = list.find((m) => m && String(m.id || '') === mid);
            if (!msg || !msg.videoBase64) return;
            const mimeRaw = String(msg.videoMime || '');
            const mime = /^video\/(webm|mp4|quicktime|ogg)$/i.test(mimeRaw) ? mimeRaw : 'video/mp4';
            const b64 = String(msg.videoBase64 || '').replace(/[^a-zA-Z0-9+/=]/g, '');
            const src = `data:${mime};base64,${b64}`;
            openVideoLightbox(src);
        }

        function openVideoLightbox(src) {
            if (!src) return;
            // Удаляем предыдущий открытый видеобокс.
            try {
                document.querySelectorAll('.glass-media-lightbox.glass-video-lightbox').forEach((x) => x.remove());
            } catch (_) {}
            const lb = document.createElement('div');
            lb.className = 'glass-media-lightbox glass-video-lightbox';

            const close = document.createElement('button');
            close.type = 'button';
            close.className = 'lb-close';
            close.setAttribute('aria-label', 'Закрыть');
            close.innerHTML = '<i class="fas fa-times"></i>';

            const video = document.createElement('video');
            video.playsInline = true;
            video.preload = 'metadata';
            video.src = src;
            video.controls = false;
            video.setAttribute('controlsList', 'nodownload noplaybackrate noremoteplayback');
            video.disablePictureInPicture = true;
            video.oncontextmenu = (e) => {
                try { e.preventDefault(); } catch (_) {}
                return false;
            };

            const controls = document.createElement('div');
            controls.className = 'video-lightbox-controls';

            const playBtn = document.createElement('button');
            playBtn.type = 'button';
            playBtn.className = 'video-lightbox-playbtn';
            playBtn.innerHTML = '<i class="fas fa-play"></i>';

            const progress = document.createElement('div');
            progress.className = 'video-lightbox-progress';
            const fill = document.createElement('div');
            fill.className = 'video-lightbox-progress-fill';
            progress.appendChild(fill);

            const timeEl = document.createElement('div');
            timeEl.className = 'video-lightbox-time';
            timeEl.textContent = '0:00 / 0:00';

            controls.appendChild(playBtn);
            controls.appendChild(progress);
            controls.appendChild(timeEl);

            lb.appendChild(close);
            lb.appendChild(video);
            lb.appendChild(controls);

            const setPlayIcon = (playing) => {
                const icon = playBtn.querySelector('i');
                if (!icon) return;
                icon.className = playing ? 'fas fa-pause' : 'fas fa-play';
            };

            const renderProgress = () => {
                const dur = video.duration;
                const cur = video.currentTime;
                if (!Number.isFinite(dur) || !dur || Number.isNaN(dur)) return;
                const pct = Math.max(0, Math.min(100, (cur / dur) * 100));
                fill.style.width = `${pct}%`;
                const curStr = formatVoiceDurationMs(Math.max(0, cur * 1000));
                const durStr = formatVoiceDurationMs(Math.max(0, dur * 1000));
                timeEl.textContent = `${curStr} / ${durStr}`;
            };

            close.addEventListener('click', (e) => {
                e.preventDefault();
                try { video.pause(); } catch (_) {}
                lb.remove();
            });

            lb.addEventListener('click', (e) => {
                if (e.target === lb) {
                    try { video.pause(); } catch (_) {}
                    lb.remove();
                }
            });

            playBtn.addEventListener('click', (e) => {
                e.preventDefault();
                if (video.paused) {
                    video.play().catch(() => {});
                } else {
                    video.pause();
                }
            });

            video.addEventListener('play', () => setPlayIcon(true));
            video.addEventListener('pause', () => setPlayIcon(false));
            video.addEventListener('timeupdate', () => renderProgress());
            video.addEventListener('loadedmetadata', () => {
                renderProgress();
            });

            progress.addEventListener('click', (e) => {
                const rect = progress.getBoundingClientRect();
                const pct = rect.width ? Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width)) : 0;
                if (video.duration) {
                    video.currentTime = pct * video.duration;
                }
            });

            document.body.appendChild(lb);
            // Стартуем после открытия.
            video.play().then(() => setPlayIcon(true)).catch(() => setPlayIcon(false));
        }

        function seychVideoToggle(btn) {
            const wrap = btn && btn.closest ? btn.closest('.glass-video-wrap') : null;
            const v = wrap && wrap.querySelector ? wrap.querySelector('video') : null;
            if (!v) return;
            const icon = btn.querySelector('i');
            if (v.paused) {
                v.play().catch(() => {});
                if (icon) icon.className = 'fas fa-pause';
            } else {
                v.pause();
                if (icon) icon.className = 'fas fa-play';
            }
        }

        async function onChatMediaSelected(event) {
            const input = event?.target;
            const files = input?.files;
            const activeChat = resolveActiveMessengerChat();
            if (!files?.length || !activeChat || messengerComposeBlocked) return;
            const blockedPeer = (messengerProfile.blacklist || []).includes(String(messengerActivePeerId || ''));
            if (isDirectMessengerChat(activeChat) && blockedPeer) {
                showNotification('Мессенджер', 'Отправка недоступна', 'warning');
                input.value = '';
                return;
            }
            // При отправке своего вложения — прокручиваем вниз.
            messengerNewWhileScrolledCount = 0;
            updateMessengerNewWhileScrolledFabUI();
            messengerShouldAutoScroll = true;

            const file = files[0];
            const maxBytes = 50 * 1024 * 1024; // 50 МБ
            if (file.size > maxBytes) {
                showNotification('Файл', `Файл слишком большой (макс. 50 МБ)`, 'warning');
                input.value = '';
                return;
            }

            const isVid = /^video\//i.test(file.type || '');
            const isImg = /^image\//i.test(file.type || '');
            const isAud = /^audio\//i.test(file.type || '');
            if (!isVid && !isImg && !isAud) {
                showNotification('Файл', 'Выберите фото, видео или музыку', 'warning');
                input.value = '';
                return;
            }

            let b64 = '';
            await new Promise((resolve, reject) => {
                const r = new FileReader();
                r.onloadend = () => {
                    const raw = String(r.result || '');
                    b64 = raw.includes(',') ? raw.split(',')[1] : '';
                    resolve();
                };
                r.onerror = () => reject(new Error('file_reader_failed'));
                r.readAsDataURL(file);
            }).catch(() => {
                showNotification('Файл', 'Не удалось прочитать файл', 'error');
            });

            if (!b64) {
                input.value = '';
                return;
            }

            const messageKind = isVid ? 'video' : isImg ? 'image' : 'voice';
            const mime = isVid ? (file.type || 'video/mp4') : isImg ? (file.type || 'image/jpeg') : (file.type || 'audio/webm');
            composerMediaDraft = {
                kind: messageKind,
                b64,
                mime: String(mime || '').slice(0, 80),
                name: String(file.name || '').slice(0, 180),
                size: Number(file.size || 0)
            };
            input.value = '';
            if (shouldRenderMessengerUi()) renderMainScreen();
        }

        // Story state
        let stories = new Map(); // userId -> stories array
        let currentStoryIndex = 0;
        let currentStories = [];
        let storyVideo = null;
        let storyProgressRaf = 0;
        let storyViewed = new Set(); // storyId -> boolean
        let storyPointerState = null;

        // Story functions
        function requestStoriesForUser(userId) {
            const id = String(userId || '').trim();
            if (!id) return;
            sendMessengerEvent({
                type: 'messenger-get-stories',
                targetUserId: id
            });
        }

        function loadStories() {
            if (!authProfile?.appUserId) return;
            
            const friends = Array.isArray(friendsState.friends) ? friendsState.friends : [];
            const friendIds = friends.map(f => f.id);
            
            // Load stories for friends
            friendIds.forEach(friendId => {
                requestStoriesForUser(friendId);
            });
            
            // Load own stories for upload button
            requestStoriesForUser(authProfile.appUserId);
            const viewedProfileId = String(messengerViewedProfile?.targetUserId || messengerViewedProfile?.profile?.id || '').trim();
            if (viewedProfileId && !friendIds.includes(viewedProfileId) && viewedProfileId !== String(authProfile.appUserId || '')) {
                requestStoriesForUser(viewedProfileId);
            }
        }

        function getStoryAuthorInfo(story, fallbackUserId = '') {
            const userId = String(story?.userId || fallbackUserId || '').trim();
            const fallback = userId ? getUserInfo(userId) : { displayName: '', name: '', avatar: '', initials: '' };
            const displayName = String(story?.userDisplayName || fallback.displayName || fallback.name || userId).trim();
            const name = String(story?.userName || fallback.name || displayName || userId).trim();
            const avatar = String(story?.userAvatar || fallback.avatar || '').trim();
            const initials = String(story?.userInitials || fallback.initials || '').trim();
            return {
                userId,
                displayName,
                name,
                avatar,
                initials
            };
        }

        function buildStoryAvatarHtml(author) {
            return avatarMarkup(
                author?.displayName || author?.name || author?.userId || '',
                author?.avatar || '',
                author?.initials || ''
            );
        }

        function getCurrentStory() {
            if (currentStoryIndex < 0 || currentStoryIndex >= currentStories.length) return null;
            return currentStories[currentStoryIndex] || null;
        }

        function getUserStories(userId) {
            return stories.get(String(userId || '').trim()) || [];
        }

        function getFirstUnviewedStoryIndex(userId) {
            const list = getUserStories(userId);
            const index = list.findIndex(story => !storyViewed.has(story.id));
            return index >= 0 ? index : -1;
        }

        function userHasUnviewedStories(userId) {
            return getFirstUnviewedStoryIndex(userId) >= 0;
        }

        function getProfileStoryMeta(userId) {
            const userStories = getUserStories(userId);
            const firstUnviewedIndex = getFirstUnviewedStoryIndex(userId);
            return {
                stories: userStories,
                hasStories: userStories.length > 0,
                hasUnviewed: firstUnviewedIndex >= 0,
                firstUnviewedIndex
            };
        }

        function buildProfileAvatarBlock({ userId, displayName, avatar, initials, clickable = true }) {
            const meta = getProfileStoryMeta(userId);
            const canOpen = clickable && meta.hasStories;
            const avatarInner = `<div class="profile-avatar">${avatarMarkup(displayName || userId || '', avatar || '', initials || '')}</div>`;
            if (!meta.hasStories) return avatarInner;
            return `
                <div class="profile-avatar-story ${canOpen ? 'clickable' : ''}" ${canOpen ? `onclick="openProfileStory('${escapeHtml(userId || '')}')"` : ''} title="${escapeHtml(meta.hasUnviewed ? 'Открыть историю' : 'Открыть публикации')}">
                    <div class="profile-avatar-story-ring ${meta.hasUnviewed ? '' : 'viewed'}">
                        ${avatarInner}
                    </div>
                </div>
            `;
        }

        function profileCoverBackgroundStyle(coverUrl, avatarUrl) {
            const cover = String(coverUrl || '').trim();
            const avatar = String(avatarUrl || '').trim();
            const source = cover || avatar;
            if (!source) return '';
            return `background-image:url('${escapeHtml(source).replace(/'/g, '&#39;')}')`;
        }

        function renderProfileHeroCard({ userId = '', displayName = '', avatar = '', coverUrl = '', initials = '', username = '', subtitle = '', clickableAvatar = true }) {
            const coverStyle = profileCoverBackgroundStyle(coverUrl, avatar);
            const hasCover = !!String(coverUrl || '').trim();
            const avatarBlock = buildProfileAvatarBlock({ userId, displayName, avatar, initials, clickable: clickableAvatar });
            return `
                <div class="profile-cover-shell">
                    <div class="profile-cover-frame">
                        <div class="profile-cover-image ${hasCover ? '' : 'is-fallback'}" style="${coverStyle}"></div>
                        <div class="profile-cover-overlay"></div>
                    </div>
                    <div class="profile-cover-avatar">${avatarBlock}</div>
                </div>
                <div class="profile-hero-meta">
                    <div class="profile-name">${escapeHtml(displayName || userId || '')}</div>
                </div>
            `;
        }

        function buildProfileStoriesSection({ userId, title, own = false }) {
            const userStories = getUserStories(userId);
            if (!userStories.length) {
                if (!own) return '';
                return `
                    <div style="margin-top: 24px; padding: 20px; background: rgba(255,255,255,0.05); border-radius: 12px; text-align: center;">
                        <div style="color: rgba(255,255,255,0.6); margin-bottom: 12px;">
                            <i class="fas fa-video" style="font-size: 32px; margin-bottom: 8px; display: block;"></i>
                            У вас пока нет историй
                        </div>
                    </div>
                `;
            }
            return `
                <div style="margin-top: 24px; padding: 16px; background: rgba(255,255,255,0.05); border-radius: 12px;">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px;">
                        <h4 style="margin: 0; color: white; font-size: 16px; font-weight: 600;">
                            <i class="fas fa-video" style="margin-right: 8px; color: #667eea;"></i>${escapeHtml(title)} (${userStories.length})
                        </h4>
                        ${own ? `` : ''}
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(80px, 1fr)); gap: 12px;">
                        ${userStories.map((story, index) => {
                            const isViewed = storyViewed.has(story.id);
                            const canOpen = true;
                            return `
                                <div style="position: relative; cursor: ${canOpen ? 'pointer' : 'default'}; border-radius: 8px; overflow: hidden; aspect-ratio: 9/16; background: #000;"
                                     ${canOpen ? `onclick="openStoryViewer('${escapeHtml(userId || '')}', ${index})"` : ''}
                                     title="${escapeHtml(story.caption || 'История ' + (index + 1))}">
                                    ${story.thumbnailUrl
                                        ? `<img src="${escapeHtml(story.thumbnailUrl)}" alt="" style="width: 100%; height: 100%; object-fit: cover;">`
                                        : `<div style="width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; background: linear-gradient(135deg, #667eea, #764ba2); color: white; font-size: 24px;"><i class="fas fa-video"></i></div>`
                                    }
                                    <div style="position: absolute; bottom: 4px; right: 4px; background: rgba(0,0,0,0.7); color: white; padding: 2px 6px; border-radius: 4px; font-size: 10px;">
                                        ${Math.max(1, Math.round((story.durationMs || 0) / 1000))}с
                                    </div>
                                    <div style="position: absolute; top: 4px; left: 4px; background: rgba(0,0,0,0.7); color: white; padding: 2px 6px; border-radius: 999px; font-size: 10px;">
                                        ${own ? (story.privacy === 'all' ? '🌍' : story.privacy === 'friends' ? '👥' : '🚫') : (isViewed ? 'Просмотрено' : 'Новое')}
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            `;
        }

        function openProfileStory(userId) {
            const id = String(userId || '').trim();
            if (!id) return;
            const meta = getProfileStoryMeta(id);
            if (!meta.hasStories) return;
            const startIndex = meta.hasUnviewed ? meta.firstUnviewedIndex : Math.max(0, meta.stories.length - 1);
            openStoryViewer(id, startIndex);
        }

        function openStoryAuthorProfile(event) {
            if (event) {
                event.preventDefault();
                event.stopPropagation();
            }
            const story = getCurrentStory();
            const userId = String(story?.userId || '').trim();
            if (!userId) return;
            closeStoryViewsModal();
            closeStoryViewer();
            openUserProfile(userId);
        }

        function openStoryViewerProfile(userId, event) {
            if (event) {
                event.preventDefault();
                event.stopPropagation();
            }
            const id = String(userId || '').trim();
            if (!id) return;
            closeStoryViewsModal();
            closeStoryViewer();
            openUserProfile(id);
        }

        function handleRemoteStoryStateChange(ownerUserId) {
            const ownerId = String(ownerUserId || '').trim();
            if (!ownerId || !authProfile?.appUserId) return;
            // Запрашиваем истории всегда — сервер сам отфильтрует по приватности
            requestStoriesForUser(ownerId);
        }

        function renderStoryProgressSegments() {
            const progressBar = document.getElementById('storyProgressBar');
            if (!progressBar) return;
            progressBar.innerHTML = currentStories.map((_, index) => `
                <div class="story-progress-segment">
                    <div id="storyProgressFill-${index}" class="story-progress-fill"></div>
                </div>
            `).join('');
            updateStoryProgressBars(0);
        }

        function updateStoryProgressBars(activeRatio = 0) {
            currentStories.forEach((_, index) => {
                const fill = document.getElementById(`storyProgressFill-${index}`);
                if (!fill) return;
                if (index < currentStoryIndex) {
                    fill.style.width = '100%';
                } else if (index > currentStoryIndex) {
                    fill.style.width = '0%';
                } else {
                    fill.style.width = `${Math.max(0, Math.min(100, activeRatio * 100))}%`;
                }
            });
        }

        function stopStoryProgressLoop() {
            if (storyProgressRaf) {
                cancelAnimationFrame(storyProgressRaf);
                storyProgressRaf = 0;
            }
        }

        function syncStoryProgressLoop() {
            stopStoryProgressLoop();
            const video = document.getElementById('storyVideo');
            const tick = () => {
                const viewer = document.getElementById('storyViewer');
                if (!viewer || !viewer.classList.contains('active')) return;
                const story = getCurrentStory();
                if (!story || !video) return;
                const duration = Number(video.duration) > 0 ? Number(video.duration) : Math.max((Number(story.durationMs) || 0) / 1000, 0.001);
                const ratio = duration > 0 ? Math.max(0, Math.min(1, Number(video.currentTime || 0) / duration)) : 0;
                updateStoryProgressBars(ratio);
                storyProgressRaf = requestAnimationFrame(tick);
            };
            storyProgressRaf = requestAnimationFrame(tick);
        }

        function pauseCurrentStoryPlayback() {
            const video = document.getElementById('storyVideo');
            if (video && !video.paused) video.pause();
        }

        function resumeCurrentStoryPlayback() {
            const video = document.getElementById('storyVideo');
            if (!video) return;
            video.play().catch(() => {});
        }

        function resetStoryPointerState(shouldResume = false) {
            if (!storyPointerState) return;
            if (storyPointerState.holdTimer) {
                clearTimeout(storyPointerState.holdTimer);
            }
            const resume = shouldResume && !!storyPointerState.resumeAfterInteraction;
            storyPointerState = null;
            if (resume) {
                resumeCurrentStoryPlayback();
            }
        }

        function storyGestureTargetAllowed(target) {
            if (!target || !target.closest) return false;
            if (target.closest('.story-header, .story-footer, #storyCaption, .story-menu-dropdown, .story-views-modal, input, button')) return false;
            return !!target.closest('.story-content, #storyVideo');
        }

        function syncStoryActionButtons(story) {
            const isOwnStory = String(story?.userId || '') === String(authProfile?.appUserId || '');
            const likeBtn = document.getElementById('storyLikeBtn');
            const sendBtn = document.getElementById('storyCommentSendBtn');
            const inputWrap = document.getElementById('storyCommentInputWrap');
            const input = document.getElementById('storyReplyInput');
            const viewsBtn = document.getElementById('storyViewsBtn');
            if (likeBtn) likeBtn.style.display = isOwnStory ? 'none' : 'flex';
            if (sendBtn) sendBtn.style.display = isOwnStory ? 'none' : 'flex';
            if (inputWrap) inputWrap.style.display = isOwnStory ? 'none' : 'flex';
            if (viewsBtn) viewsBtn.style.display = isOwnStory ? 'flex' : 'none';
            if (input) {
                input.value = '';
                input.placeholder = 'Добавить комментарий...';
            }
        }

        function renderStories() {
            const desktopContainer = document.getElementById('storiesContainer');
            const mobileContainer = document.getElementById('mobileStoriesContainer');
            
            const friends = Array.isArray(friendsState.friends) ? friendsState.friends : [];
            const ownStories = stories.get(authProfile?.appUserId || '') || [];
            
            let html = '';
            
            // Add own stories or upload button
            if (ownStories.length > 0) {
                const hasUnviewedOwn = ownStories.some(story => !storyViewed.has(story.id));
                const latestOwnStory = ownStories[ownStories.length - 1];
                const ownAuthor = getStoryAuthorInfo(latestOwnStory, authProfile?.appUserId || '');
                const ownAvatarHtml = buildStoryAvatarHtml({
                    ...ownAuthor,
                    displayName: ownAuthor.displayName || authProfile?.displayName || authProfile?.name || 'Вы',
                    name: ownAuthor.name || authProfile?.name || 'Вы',
                    avatar: ownAuthor.avatar || authProfile?.avatar || '',
                    initials: ownAuthor.initials || authProfile?.initials || ''
                });
                
                html += `
                    <div class="story-upload-btn-wrapper mobile-right" onclick="openStoryUploadModal()" title="Добавить историю">
                        <div class="story-upload-btn">
                            <i class="fas fa-plus"></i>
                        </div>
                    </div>
                    <div class="story-avatar-wrapper" onclick="openStoryViewer('${escapeHtml(authProfile?.appUserId || '')}')" title="Мои истории">
                        <div class="story-avatar-ring ${hasUnviewedOwn ? '' : 'viewed'}">
                            ${ownAvatarHtml}
                        </div>
                    </div>
                `;
            } else {
                html += `
                    <div class="story-upload-btn-wrapper mobile-right" onclick="openStoryUploadModal()" title="Добавить историю">
                        <div class="story-upload-btn">
                            <i class="fas fa-plus"></i>
                        </div>
                    </div>
                `;
            }
            
            // Add friends with stories
            friends.forEach(friend => {
                const friendStories = stories.get(friend.id) || [];
                if (friendStories.length === 0) return;
                
                const hasUnviewed = friendStories.some(story => !storyViewed.has(story.id));
                const latestStory = friendStories[friendStories.length - 1];
                const author = getStoryAuthorInfo(latestStory, friend.id);
                const avatarHtml = buildStoryAvatarHtml({
                    ...author,
                    displayName: author.displayName || friend.displayName || friend.name || friend.id,
                    name: author.name || friend.name || friend.id,
                    avatar: author.avatar || friend.avatar || '',
                    initials: author.initials || friend.initials || ''
                });
                
                html += `
                    <div class="story-avatar-wrapper" onclick="openStoryViewer('${escapeHtml(friend.id)}')">
                        <div class="story-avatar-ring ${hasUnviewed ? '' : 'viewed'}">
                            ${avatarHtml}
                        </div>
                    </div>
                `;
            });
            
            // Add fallback content if no stories
            if (html === '') {
                html = `
                    <div class="story-avatar-wrapper" onclick="openStoryUploadModal()">
                        <div class="story-add-btn">
                            <i class="fas fa-plus"></i>
                        </div>
                    </div>
                `;
            }
            
            // Update both containers
            if (desktopContainer) desktopContainer.innerHTML = html;
            if (mobileContainer) mobileContainer.innerHTML = html;
        }

        function openStoryViewer(userId, startIndex = 0) {
            const userStories = stories.get(userId) || [];
            if (userStories.length === 0) return;
            
            currentStories = userStories;
            currentStoryIndex = Math.max(0, Math.min(startIndex, userStories.length - 1));
            
            const viewer = document.getElementById('storyViewer');
            viewer.classList.add('active');
            renderStoryProgressSegments();
            
            showStory(currentStoryIndex);
        }

        function showStory(index) {
            if (index < 0 || index >= currentStories.length) {
                closeStoryViewer();
                return;
            }
            
            const story = currentStories[index];
            const video = document.getElementById('storyVideo');
            const userAvatar = document.getElementById('storyUserAvatar');
            const userName = document.getElementById('storyUserName');
            const userTime = document.getElementById('storyTime');
            const caption = document.getElementById('storyCaption');
            const input = document.getElementById('storyReplyInput');
            const likeBtn = document.getElementById('storyLikeBtn');
            const userInfo = document.querySelector('.story-user-info');
            
            // Load user info
            const author = getStoryAuthorInfo(story, story.userId);
            userAvatar.innerHTML = buildStoryAvatarHtml(author);
            userName.textContent = author.displayName || author.name || author.userId;
            userTime.textContent = formatStoryTime(story.createdAt);
            if (userInfo) {
                userInfo.classList.add('story-user-info--clickable');
                userInfo.onclick = (event) => openStoryAuthorProfile(event);
            }
            if (input) input.value = '';
            if (likeBtn) likeBtn.classList.remove('liked');
            syncStoryActionButtons(story);
            renderStoryProgressSegments();
            updateStoryProgressBars(0);
            closeStoryMenu();
            resetStoryPointerState(false);
            
            // Load video with proper error handling
            video.pause();
            video.src = '';
            video.load(); // Reset video
            stopStoryProgressLoop();
            
            // Remove previous error div if exists
            const existingError = video.parentElement.querySelector('.video-error');
            if (existingError) existingError.remove();
            
            // Set video source
            video.src = story.videoUrl;
            video.currentTime = 0;
            video.style.display = 'block';
            
            // Add video event listeners for debugging
            video.onloadstart = () => {
                console.log('Video loading started:', story.videoUrl);
                video.style.display = 'block';
            };
            video.onloadeddata = () => {
                console.log('Video data loaded');
                video.style.display = 'block';
            };
            video.onloadedmetadata = () => {
                console.log('Video metadata loaded, duration:', video.duration);
                video.style.display = 'block';
                syncStoryProgressLoop();
                video.play().catch(e => console.log('Auto-play failed:', e));
            };
            video.onplay = () => syncStoryProgressLoop();
            video.onpause = () => updateStoryProgressBars(
                (Number(video.duration) > 0 && Number(video.currentTime) >= 0)
                    ? Math.max(0, Math.min(1, Number(video.currentTime) / Number(video.duration)))
                    : 0
            );
            video.onended = () => nextStory();
            video.onerror = (e) => {
                console.error('Video error:', e, story.videoUrl);
                // Show error message to user
                video.style.display = 'none';
                stopStoryProgressLoop();
                const errorDiv = document.createElement('div');
                errorDiv.className = 'video-error';
                errorDiv.style.cssText = 'position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); color: white; text-align: center; padding: 20px; background: rgba(0,0,0,0.8); border-radius: 8px;';
                errorDiv.innerHTML = '<i class="fas fa-exclamation-triangle" style="font-size: 48px; margin-bottom: 16px; display: block;"></i>Ошибка загрузки видео';
                video.parentNode.appendChild(errorDiv);
            };
            
            // Show caption
            caption.textContent = story.caption || '';
            caption.style.display = story.caption ? 'block' : 'none';
            
            // Mark as viewed
            if (!storyViewed.has(story.id)) {
                storyViewed.add(story.id);
                sendMessengerEvent({
                    type: 'messenger-view-story',
                    storyId: story.id
                });
                renderStories(); // Update rings
            }
            
            // Check like status
            sendMessengerEvent({
                type: 'messenger-check-story-like',
                storyId: story.id
            });

            // Play video with better error handling
            video.play().then(() => {
                console.log('Video playing successfully');
            }).catch(err => {
                console.error('Video play error:', err);
                // Try to autoplay after user interaction
                document.addEventListener('click', function playVideo() {
                    video.play().catch(e => console.error('Retry video play error:', e));
                    document.removeEventListener('click', playVideo);
                }, { once: true });
            });
        }

        function nextStory() {
            currentStoryIndex++;
            showStory(currentStoryIndex);
        }

        function prevStory() {
            currentStoryIndex--;
            showStory(currentStoryIndex);
        }

        function closeStoryViewer() {
            const viewer = document.getElementById('storyViewer');
            viewer.classList.remove('active');
            
            const video = document.getElementById('storyVideo');
            video.pause();
            video.src = '';
            stopStoryProgressLoop();
            closeStoryMenu();
            resetStoryPointerState(false);
            
            currentStories = [];
            currentStoryIndex = 0;
        }

        function toggleStoryLike() {
            if (currentStoryIndex >= currentStories.length) return;
            
            const story = currentStories[currentStoryIndex];
            const likeBtn = document.getElementById('storyLikeBtn');
            
            sendMessengerEvent({
                type: 'messenger-like-story',
                storyId: story.id
            });
        }

        function sendStoryComment() {
            if (currentStoryIndex >= currentStories.length) return;
            
            const story = currentStories[currentStoryIndex];
            const input = document.getElementById('storyReplyInput');
            const text = input.value.trim();
            
            if (!text) return;
            sendMessengerEvent({
                type: 'messenger-comment-story',
                storyId: story.id,
                comment: text
            });
        }

        function handleStoryReplyKeypress(event) {
            if (event.key === 'Enter') {
                sendStoryComment();
            }
        }

        function renderStoryPrivacyDropdown(currentVal = 'friends') {
            const safe = ['all', 'friends', 'nobody'].includes(currentVal) ? currentVal : 'friends';
            const labels = { all: '🌍 Все', friends: '👥 Друзья', nobody: '🚫 Никто' };
            const opts = ['all', 'friends', 'nobody']
                .map(v =>
                    `<button type="button" class="privacy-dd-opt ${v === safe ? 'active' : ''}" onclick="setStoryPrivacy('${v}')">${labels[v]}</button>`
                )
                .join('');
            return `<div class="privacy-dd"><button type="button" class="privacy-dd-trigger" onclick="toggleStoryPrivacyDropdown(event)">${labels[safe]} <i class="fas fa-chevron-down"></i></button><div class="privacy-dd-panel">${opts}</div></div>`;
        }

        function setStoryPrivacy(value) {
            window.currentStoryPrivacy = value;
            const dropdown = document.getElementById('storyPrivacyDropdown');
            if (dropdown) {
                dropdown.innerHTML = renderStoryPrivacyDropdown(value);
            }
            // Close all dropdown panels
            document.querySelectorAll('.privacy-dd-panel').forEach((p) => p.classList.remove('open'));
        }

        function toggleStoryPrivacyDropdown(event) {
            if (event) event.stopPropagation();
            const trigger = event.target.closest('.privacy-dd-trigger');
            if (!trigger) return;
            const panel = trigger.nextElementSibling;
            if (!panel) return;
            const willOpen = !panel.classList.contains('open');
            // Close all other dropdown panels
            document.querySelectorAll('.privacy-dd-panel').forEach((p) => p.classList.remove('open'));
            if (willOpen) panel.classList.add('open');
        }

        function openStoryUploadModal() {
            // Remove existing modal if any
            const existingModal = document.getElementById('storyUploadModalOverlay');
            if (existingModal) existingModal.remove();
            
            // Initialize story privacy
            window.currentStoryPrivacy = window.currentStoryPrivacy || 'friends';
            
            const overlay = document.createElement('div');
            overlay.className = 'modal-overlay';
            overlay.id = 'storyUploadModalOverlay';
            overlay.style.display = 'flex';
            overlay.style.opacity = '1';
            overlay.style.visibility = 'visible';
            overlay.onclick = (e) => { if (e.target === overlay) closeStoryUploadModal(); };
            
            overlay.innerHTML = `
                <div class="story-upload-modal">
                    <div class="story-upload-modal-header">
                        <h3><i class="fas fa-video" style="margin-right:8px;color:#667eea;"></i>Новая история</h3>
                        <button type="button" class="story-upload-modal-close" onclick="closeStoryUploadModal()">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                    <div class="story-upload-modal-body">
                        <div>
                            <label for="storyVideoInput">
                                <i class="fas fa-film" style="margin-right:4px;"></i>Видео (макс. 20 секунд)
                            </label>
                            <input type="file" id="storyVideoInput" accept="video/*" onchange="uploadStory()">
                        </div>
                        <div>
                            <label for="storyCaptionInput">
                                <i class="fas fa-comment" style="margin-right:4px;"></i>Подпись (необязательно)
                            </label>
                            <input type="text" id="storyCaptionInput" placeholder="Добавьте подпись..." maxlength="500">
                        </div>
                        <div>
                            <label>
                                <i class="fas fa-eye" style="margin-right:4px;"></i>Кто может видеть
                            </label>
                            <div id="storyPrivacyDropdown">${renderStoryPrivacyDropdown(window.currentStoryPrivacy)}</div>
                        </div>
                        <div id="storyUploadPreview" style="display:none;">
                            <label>
                                <i class="fas fa-play-circle" style="margin-right:4px;"></i>Предпросмотр
                            </label>
                            <video id="storyPreviewVideo" controls></video>
                        </div>
                    </div>
                    <div class="story-upload-modal-footer">
                        <button type="button" class="story-upload-modal-btn" onclick="closeStoryUploadModal()">
                            <i class="fas fa-times" style="margin-right:6px;"></i>Отмена
                        </button>
                        <button type="button" class="story-upload-modal-btn primary" onclick="completeStoryUpload()">
                            <i class="fas fa-paper-plane" style="margin-right:6px;"></i>Опубликовать
                        </button>
                    </div>
                </div>
            `;
            
            document.body.appendChild(overlay);
        }

        function closeStoryUploadModal() {
            const overlay = document.getElementById('storyUploadModalOverlay');
            if (overlay) overlay.remove();
        }

        function uploadStory() {
            const fileInput = document.getElementById('storyVideoInput');
            const file = fileInput.files[0];
            
            if (!file) {
                showNotification('', 'Выберите видео', 'warning');
                return;
            }
            
            if (file.size > 50 * 1024 * 1024) { // 50MB limit
                showNotification('', 'Размер файла не должен превышать 50MB', 'warning');
                return;
            }
            
            const reader = new FileReader();
            reader.onload = function(e) {
                const videoData = e.target.result;
                const video = document.getElementById('storyPreviewVideo');
                video.src = videoData;
                
                video.onloadedmetadata = function() {
                    if (video.duration > 20) {
                        showNotification('', 'Длительность видео не должна превышать 20 секунд', 'warning');
                        return;
                    }
                    
                    document.getElementById('storyUploadPreview').style.display = 'block';
                };
            };
            reader.readAsDataURL(file);
        }

        function completeStoryUpload() {
            const video = document.getElementById('storyPreviewVideo');
            const caption = document.getElementById('storyCaptionInput').value.trim();
            const privacy = window.currentStoryPrivacy || 'friends';
            const fileInput = document.getElementById('storyVideoInput');
            
            if (!video.src) {
                showNotification('', 'Сначала выберите видео', 'warning');
                return;
            }
            
            // Check video duration (max 20 seconds)
            if (video.duration > 20) {
                showNotification('', 'Видео должно быть не длиннее 20 секунд', 'error');
                return;
            }
            
            // Generate thumbnail from video
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            video.currentTime = 0.1; // Get frame from 0.1s
            
            video.onseeked = function() {
                try {
                    canvas.width = video.videoWidth;
                    canvas.height = video.videoHeight;
                    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
                    const thumbnailUrl = canvas.toDataURL('image/jpeg', 0.8);
                    
                    // Upload story
                    sendMessengerEvent({
                        type: 'messenger-upload-story',
                        videoUrl: video.src,
                        videoMime: fileInput.files[0] ? fileInput.files[0].type : 'video/mp4',
                        durationMs: Math.round(video.duration * 1000),
                        thumbnailUrl,
                        caption,
                        privacy
                    });
                    
                    closeStoryUploadModal();
                    showNotification('', 'История успешно опубликована', 'success');
                } catch (error) {
                    console.error('Error uploading story:', error);
                    showNotification('', 'Ошибка при загрузке истории', 'error');
                }
            };
            
            video.onerror = function() {
                showNotification('', 'Ошибка при обработке видео', 'error');
            };
        }

        function showStoryViewsModal(views) {
            const modal = document.getElementById('storyViewsModal');
            const list = document.getElementById('storyViewsList');
            
            if (!Array.isArray(views) || views.length === 0) {
                list.innerHTML = `<div style="padding:24px;text-align:center;color:rgba(255,255,255,0.7);">Пока нет просмотров</div>`;
                modal.classList.add('active');
                return;
            }

            const html = views.map(view => `
                <div class="story-view-item story-view-item--clickable" onclick="openStoryViewerProfile('${escapeHtml(view.userId || '')}', event)">
                    <div class="story-view-avatar">
                        ${view.avatar 
                            ? `<img src="${escapeHtml(view.avatar)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">`
                            : avatarMarkup(view.displayName, '', view.initials || '')
                        }
                    </div>
                    <div class="story-view-info">
                        <div class="story-view-name">
                            ${escapeHtml(view.displayName)}
                            ${view.liked ? '<i class="fas fa-heart story-view-heart"></i>' : ''}
                        </div>
                        <div class="story-view-time">${formatStoryTime(view.viewedAt)}</div>
                        ${view.comment ? `<div class="story-view-comment">${escapeHtml(view.comment)}</div>` : ''}
                    </div>
                </div>
            `).join('');
            
            list.innerHTML = html;
            modal.classList.add('active');
        }

        function closeStoryViewsModal() {
            document.getElementById('storyViewsModal').classList.remove('active');
        }

        function ensureStoryMenuStyle() {
            if (document.getElementById('storyMenuStyle')) return;
            const style = document.createElement('style');
            style.id = 'storyMenuStyle';
            style.textContent = `
                .story-menu-dropdown {
                    position: fixed;
                    background: rgba(0,0,0,0.92);
                    border-radius: 12px;
                    border: 1px solid rgba(255,255,255,0.12);
                    padding: 8px 0;
                    min-width: 180px;
                    z-index: 10001;
                    backdrop-filter: blur(12px);
                    box-shadow: 0 16px 36px rgba(0,0,0,0.35);
                }
                .story-menu-item {
                    padding: 12px 16px;
                    color: rgba(255,255,255,0.9);
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    font-size: 14px;
                    transition: all 0.2s ease;
                }
                .story-menu-item:hover {
                    background: rgba(255,255,255,0.1);
                    color: white;
                }
                .story-menu-item.danger {
                    color: #ff4458;
                }
                .story-menu-item.danger:hover {
                    background: rgba(255,68,88,0.2);
                }
            `;
            document.head.appendChild(style);
        }

        function toggleStoryMenu(event) {
            if (currentStoryIndex >= currentStories.length) return;
            if (event) {
                event.preventDefault();
                event.stopPropagation();
            }
            const story = currentStories[currentStoryIndex];
            const isOwnStory = story.userId === authProfile?.appUserId;
            
            // Remove existing menu
            const existingMenu = document.getElementById('storyMenuDropdown');
            if (existingMenu) {
                existingMenu.remove();
                document.removeEventListener('click', closeStoryMenu, true);
                return;
            }
            ensureStoryMenuStyle();
            
            // Create menu dropdown
            const menu = document.createElement('div');
            menu.id = 'storyMenuDropdown';
            menu.className = 'story-menu-dropdown';
            
            if (isOwnStory) {
                menu.innerHTML = `
                    <div class="story-menu-item" onclick="changeStoryPrivacy(${currentStoryIndex})">
                        <i class="fas fa-lock" style="width: 16px; margin-right: 12px;"></i>
                        Изменить приватность
                    </div>
                    <div class="story-menu-item danger" onclick="deleteStory(${currentStoryIndex})">
                        <i class="fas fa-trash" style="width: 16px; margin-right: 12px;"></i>
                        Удалить
                    </div>
                `;
            } else {
                menu.innerHTML = `
                    <div class="story-menu-item" onclick="reportStory(${currentStoryIndex})">
                        <i class="fas fa-flag" style="width: 16px; margin-right: 12px;"></i>
                        Пожаловаться
                    </div>
                `;
            }
            const btn = event?.currentTarget || document.getElementById('storyMenuBtn');
            const rect = btn ? btn.getBoundingClientRect() : { left: window.innerWidth - 200, bottom: 40, width: 36 };
            const desiredLeft = Math.min(window.innerWidth - 192, Math.max(12, rect.right - 180));
            menu.style.top = `${rect.bottom + 8}px`;
            menu.style.left = `${desiredLeft}px`;
            document.body.appendChild(menu);
            
            // Close menu when clicking outside
            setTimeout(() => {
                document.addEventListener('click', closeStoryMenu, true);
            }, 100);
        }
        
        function closeStoryMenu(event) {
            if (event && event.target && event.target.closest && event.target.closest('#storyMenuDropdown, #storyMenuBtn')) return;
            const menu = document.getElementById('storyMenuDropdown');
            if (menu) menu.remove();
            document.removeEventListener('click', closeStoryMenu, true);
        }
        
        function showStoryViews(index) {
            if (index >= currentStories.length) return;
            const story = currentStories[index];
            closeStoryMenu();
            sendMessengerEvent({
                type: 'messenger-get-story-views',
                storyId: story.id
            });
        }
        
        function changeStoryPrivacy(index) {
            if (index >= currentStories.length) return;
            const story = currentStories[index];
            closeStoryMenu();
            
            // Create privacy modal
            const modal = document.createElement('div');
            modal.className = 'modal-overlay';
            modal.style.cssText = `
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: rgba(0,0,0,0.7);
                backdrop-filter: blur(12px);
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 10002;
            `;
            
            modal.innerHTML = `
                <div class="story-upload-modal" style="max-width: 400px;">
                    <div class="story-upload-modal-header">
                        <h3>Приватность истории</h3>
                        <button type="button" onclick="this.closest('.modal-overlay').remove()">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                    <div class="story-upload-modal-body">
                        <div>
                            <label>Кто может видеть</label>
                            <select id="storyPrivacySelect">
                                <option value="all" ${story.privacy === 'all' ? 'selected' : ''}>🌍 Все</option>
                                <option value="friends" ${story.privacy === 'friends' ? 'selected' : ''}>👥 Друзья</option>
                                <option value="nobody" ${story.privacy === 'nobody' ? 'selected' : ''}>🚫 Никто</option>
                            </select>
                        </div>
                    </div>
                    <div class="story-upload-modal-footer">
                        <button type="button" class="story-upload-modal-btn" onclick="this.closest('.modal-overlay').remove()">Отмена</button>
                        <button type="button" class="story-upload-modal-btn primary" onclick="updateStoryPrivacy('${story.id}', this)">Сохранить</button>
                    </div>
                </div>
            `;
            
            document.body.appendChild(modal);
        }
        
        function updateStoryPrivacy(storyId, button) {
            const privacy = document.getElementById('storyPrivacySelect').value;
            sendMessengerEvent({
                type: 'messenger-update-story-privacy',
                storyId,
                privacy
            });
            button.closest('.modal-overlay').remove();
        }
        
        function deleteStory(index) {
            if (index >= currentStories.length) return;
            const story = currentStories[index];
            closeStoryMenu();
            
            if (confirm('Удалить эту историю?')) {
                sendMessengerEvent({
                    type: 'messenger-delete-story',
                    storyId: story.id
                });
                closeStoryViewer();
            }
        }
        
        function reportStory(index) {
            if (index >= currentStories.length) return;
            closeStoryMenu();
            showNotification('', 'Жалоба отправлена', 'success');
        }

        function handleStoryViewerClick(event) {
            // Close if clicking outside content area
            if (event.target && event.target.id === 'storyViewer') {
                closeStoryViewer();
            } else {
                closeStoryMenu(event);
            }
        }

        function handleStoryPointerDown(event) {
            if (!storyGestureTargetAllowed(event.target)) return;
            const video = document.getElementById('storyVideo');
            if (!video) return;
            if (event.cancelable) event.preventDefault();
            storyPointerState = {
                pointerId: event.pointerId,
                startX: event.clientX,
                startY: event.clientY,
                startTime: Number(video.currentTime || 0),
                moved: false,
                scrubbing: false,
                holdActive: false,
                resumeAfterInteraction: !video.paused,
                holdTimer: setTimeout(() => {
                    if (!storyPointerState) return;
                    storyPointerState.holdActive = true;
                    pauseCurrentStoryPlayback();
                }, 160)
            };
        }

        function handleStoryPointerMove(event) {
            if (!storyPointerState || storyPointerState.pointerId !== event.pointerId) return;
            const video = document.getElementById('storyVideo');
            if (!video) return;
            if (event.cancelable) event.preventDefault();
            const dx = event.clientX - storyPointerState.startX;
            const dy = event.clientY - storyPointerState.startY;
            if (Math.abs(dx) > 10 || Math.abs(dy) > 10) {
                storyPointerState.moved = true;
            }
            if (Math.abs(dx) > 14 && Math.abs(dx) > Math.abs(dy)) {
                if (storyPointerState.holdTimer) {
                    clearTimeout(storyPointerState.holdTimer);
                    storyPointerState.holdTimer = null;
                }
                storyPointerState.scrubbing = true;
                pauseCurrentStoryPlayback();
                const duration = Number(video.duration || 0) || Math.max((Number(getCurrentStory()?.durationMs) || 0) / 1000, 0);
                if (duration > 0) {
                    const nextTime = Math.max(0, Math.min(duration, storyPointerState.startTime + dx * 0.05));
                    video.currentTime = nextTime;
                    updateStoryProgressBars(duration > 0 ? nextTime / duration : 0);
                }
            }
        }

        function handleStoryPointerUp(event) {
            if (!storyPointerState || storyPointerState.pointerId !== event.pointerId) return;
            const video = document.getElementById('storyVideo');
            if (event.cancelable) event.preventDefault();
            const dx = event.clientX - storyPointerState.startX;
            const tapAllowed = !storyPointerState.moved && !storyPointerState.scrubbing && !storyPointerState.holdActive && video;
            const tapX = tapAllowed ? event.clientX - video.getBoundingClientRect().left : 0;
            const tapWidth = tapAllowed ? video.getBoundingClientRect().width : 0;
            const shouldResume = storyPointerState.scrubbing || storyPointerState.holdActive;
            resetStoryPointerState(shouldResume);
            if (tapAllowed && tapWidth > 0) {
                if (tapX < tapWidth * 0.35) {
                    prevStory();
                } else if (tapX > tapWidth * 0.65) {
                    nextStory();
                }
            }
        }

        function handleStoryPointerCancel(event) {
            if (!storyPointerState || storyPointerState.pointerId !== event.pointerId) return;
            resetStoryPointerState(true);
        }

        function handleStoryContextMenu(event) {
            if (event) {
                event.preventDefault();
                event.stopPropagation();
            }
            return false;
        }

        // Add keyboard navigation
        document.addEventListener('keydown', function(event) {
            const viewer = document.getElementById('storyViewer');
            if (!viewer.classList.contains('active')) return;
            
            switch(event.key) {
                case 'ArrowLeft':
                    prevStory();
                    break;
                case 'ArrowRight':
                    nextStory();
                    break;
                case 'Escape':
                    closeStoryViewer();
                    break;
            }
        });

        function formatStoryTime(timestamp) {
            const date = new Date(timestamp);
            const now = new Date();
            const diffMs = now - date;
            const diffMins = Math.floor(diffMs / 60000);
            const diffHours = Math.floor(diffMs / 3600000);
            
            if (diffMins < 1) return 'Только что';
            if (diffMins < 60) return `${diffMins} мин назад`;
            if (diffHours < 24) return `${diffHours} ч назад`;
            return `${Math.floor(diffHours / 24)} д назад`;
        }

        function getUserInfo(userId) {
            const friends = Array.isArray(friendsState.friends) ? friendsState.friends : [];
            const friend = friends.find(f => f.id === userId);
            
            if (friend) {
                return {
                    displayName: friend.displayName || friend.name || friend.id,
                    name: friend.name || friend.id,
                    avatar: friend.avatar || '',
                    initials: friend.initials || ''
                };
            }
            
            // Fallback to own profile
            if (userId === authProfile?.appUserId) {
                return {
                    displayName: authProfile.name || authProfile.appUserId || '',
                    name: authProfile.appUserId || '',
                    avatar: authProfile.avatar || '',
                    initials: ''
                };
            }
            
            return {
                displayName: userId,
                name: userId,
                avatar: '',
                initials: ''
            };
        }

        function renderMessengerWorkspace() {
            let activeChat = resolveActiveMessengerChat();
            // Дополнительная защита: если нет activeChat, но есть messengerActiveChatId и messengerActivePeerId,
            // то это может быть прямой чат, который еще не загружен в messengerChats
            if (!activeChat && messengerActivePeerId && messengerActiveChatId) {
                const peer = resolvePeerDisplay(messengerActivePeerId);
                activeChat = { id: messengerActiveChatId, peer, lastMessage: null };
            }
            if (!activeChat || (!isGroupMessengerChat(activeChat) && !messengerActivePeerId)) {
                return `
                    <div class="workspace-empty" style="display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;">
                        <div style="opacity:.9;font-size:20px;font-weight:700;">Чат не выбран</div>
                        <div style="opacity:.72;">Напишите или позвоните пользователю</div>
                        <div class="workspace-empty-cards">
                            <div class="workspace-empty-card" onclick="setMessengerView('calls')"><i class="fas fa-phone"></i><div>Позвонить</div></div>
                            <div class="workspace-empty-card" onclick="setMessengerView('friends')"><i class="fas fa-comment-dots"></i><div>Написать</div></div>
                        </div>
                    </div>
                `;
            }
            const activeChatIdResolved = String(messengerActiveChatId || activeChat?.id || '').trim();
            const activeChatTitle = String(activeChat.peer?.displayName || activeChat.peer?.name || activeChat.group?.title || activeChat.id || '—').trim() || '—';
            const leftState = getGroupLeaveStateClient(activeChat, authProfile?.appUserId || '');
            const isLeft = !!leftState;
            const frozenAt = Number(leftState?.frozenAt || leftState?.leftAt || 0);
            const messages = resolveChatMessages(activeChatIdResolved).filter((m) => {
                if (!m || m.deletedAt) return false;
                if (!isLeft || !frozenAt) return true;
                return Number(m.createdAt || 0) <= frozenAt;
            });
            const peerTypingState = getMessengerPeerActivityState(messengerActivePeerId);
            const statusText = formatPeerStatusLine(activeChat.peer, peerTypingState);
            const blockedPeer = (messengerProfile.blacklist || []).includes(String(messengerActivePeerId || ''));
            const groupRestriction = isGroupMessengerChat(activeChat) ? activeChat.group?.restriction || null : null;
            const groupBanned = !!groupRestriction && groupRestriction.type === 'banned';
            const composerLocked = !isLeft && (messengerComposeBlocked || blockedPeer || groupBanned);
            const composerPlaceholder = composerLocked
                ? (blockedPeer ? 'Вы не можете отправить сообщение этому пользователю' : (messengerComposeHint || getGroupRestrictionHintClient(groupRestriction) || 'Вы не можете отправить сообщение этому пользователю'))
                : (isLeft ? 'Вернитесь в чат, чтобы писать снова' : 'Сообщение…');
            const myId = String(authProfile?.appUserId || '');
            const peerId = String(messengerActivePeerId || '');
            const dayKeyOf = (ts) => {
                const d = new Date(Number(ts || Date.now()));
                const y = d.getFullYear();
                const m = String(d.getMonth() + 1).padStart(2, '0');
                const dd = String(d.getDate()).padStart(2, '0');
                return `${y}-${m}-${dd}`;
            };
            const formatDayLabel = (ts) => {
                const d = new Date(Number(ts || Date.now()));
                const now = new Date();
                const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
                const startOfThat = new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
                const diffDays = Math.round((startOfToday - startOfThat) / 86400000);
                if (diffDays === 0) return 'Сегодня';
                if (diffDays === 1) return 'Вчера';
                const month = d.toLocaleString('ru-RU', { month: 'long' });
                const day = d.getDate();
                const year = d.getFullYear();
                const curYear = now.getFullYear();
                return year === curYear ? `${day} ${month}` : `${day} ${month} ${year}`;
            };
            const reactionOrder = ['❤️', '👍', '👎', '😂', '😮', '😢', '😡', '🔥', '🎉', '👏', '😍', '🤔', '🙏', '💯', '😎'];
            const renderReactions = (msg) => {
                const r = msg && msg.reactions && typeof msg.reactions === 'object' ? msg.reactions : {};
                const entries = Object.entries(r)
                    .map(([emoji, users]) => [String(emoji || ''), Array.isArray(users) ? users : []])
                    .filter(([emoji, users]) => emoji && users.length > 0);
                if (!entries.length) return '';
                entries.sort((a, b) => reactionOrder.indexOf(a[0]) - reactionOrder.indexOf(b[0]));
                const html = entries
                    .map(([emoji, users]) => {
                        const list = Array.from(new Set(users.map((u) => String(u)).filter(Boolean)));
                        const active = list.includes(myId);
                        const shown = list.slice(0, 3);
                        const avatars = shown
                            .map((uid) => {
                                const peer = resolvePeerDisplay(uid);
                                const title = String(peer?.displayName || peer?.name || uid || '').trim() || uid;
                                const avatar = String(peer?.avatar || '').trim();
                                const initials = String(peer?.initials || '').trim() || title.split(/\s+/).filter(Boolean).map((p) => p.charAt(0)).join('').slice(0, 2).toUpperCase();
                                const inner = avatar
                                    ? `<img src="${escapeHtml(avatar)}" alt="" referrerpolicy="no-referrer" draggable="false" oncontextmenu="return false" style="width:100%;height:100%;object-fit:cover;">`
                                    : `<div class="messenger-avatar-fallback" style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:800;">${escapeHtml(initials || '·')}</div>`;
                                return `<div class="chat-msg-reaction-user" title="${escapeHtml(title)}">${inner}</div>`;
                            })
                            .join('');
                        const more = list.length > 3 ? `<span class="chat-msg-reaction-more">+${list.length - 3}</span>` : '';
                        const usersHtml = `<span class="chat-msg-reaction-users">${avatars}</span>${more}`;
                        return `<div class="chat-msg-reaction ${active ? 'active' : ''}" onclick="toggleMessageReaction('${escapeHtml(msg.id || '')}','${escapeHtml(emoji)}')"><span>${escapeHtml(emoji)}</span>${usersHtml}</div>`;
                    })
                    .join('');
                return html ? `<div class="chat-msg-reactions">${html}</div>` : '';
            };
            const rows = messages.length
                ? (() => {
                    const parts = [];
                    let lastDay = '';
                    for (const msg of messages) {
                        const dk = dayKeyOf(msg.createdAt);
                        if (dk !== lastDay) {
                            lastDay = dk;
                            parts.push(`<div class="chat-day-sep">${escapeHtml(formatDayLabel(msg.createdAt))}</div>`);
                        }
                    const mine = String(msg.fromId || '') === String(authProfile?.appUserId || '');
                    const msgIdSafe = messengerSafeId(msg.id);
                    const isVoice = msg.messageKind === 'voice';
                    const isMusic = isVoice && !!String(msg.text || '') && String(msg.text || '') !== 'Голосовое сообщение';
                    const isImage = msg.messageKind === 'image';
                    const isVideo = msg.messageKind === 'video';
                    const isCurrentMusic = musicPlayer.playing
                        && String(musicPlayer.chatId || '') === String(activeChat.id || '')
                        && String(musicPlayer.msgId || '') === String(msg.id || '');
                    let body;
                    if (isVoice) {
                        if (!msg.audioBase64) {
                            body = `<div class="chat-msg-pending">${isMusic ? 'Музыка загружается…' : 'Голосовое загружается…'}</div>`;
                        } else if (isMusic) {
                            const iconClass = isCurrentMusic ? 'fas fa-pause' : 'fas fa-play';
                            const safeMsgId = String(msg.id || '').replace(/[^a-zA-Z0-9_-]/g, '');
                            const pct = (() => {
                                try {
                                    const a = musicPlayer.audioEl;
                                    if (!a || !a.duration) return 0;
                                    return Math.max(0, Math.min(100, (a.currentTime / a.duration) * 100));
                                } catch (_) {
                                    return 0;
                                }
                            })();
                            body = `<div class="glass-music-inline">
                                <button type="button" class="music-inline-play-btn" onclick="toggleMusicFromMessage(this)" data-chat-id="${escapeHtml(activeChat.id)}" data-msg-id="${escapeHtml(msg.id || '')}" aria-label="Музыка">
                                    <i class="${iconClass}"></i>
                                </button>
                                <div style="flex:1;min-width:0;">
                                    <div class="music-inline-title">${escapeHtml(msg.text || 'Музыка')}</div>
                                    <div class="music-inline-progress" aria-hidden="true"><div class="music-inline-progress-fill" id="musicInlineProgressFill-${safeMsgId}" style="width:${isCurrentMusic ? pct : 0}%;"></div></div>
                                </div>
                                <button type="button" class="music-inline-stop-btn" onclick="stopMusicPlayer(true)" style="display:${isCurrentMusic ? 'inline-flex' : 'none'};" aria-label="Стоп">
                                    <i class="fas fa-stop"></i>
                                </button>
                            </div>`;
                        } else {
                            if (!msg.audioBase64) {
                                body = `<div class="chat-msg-pending">Голосовое загружается…</div>`;
                            } else {
                                const waveHeights = voiceWaveBarsFromSeed(msg.id || msg.createdAt, 22);
                                const waveHtml = waveHeights.map((ht) => `<span style="height:${ht}px"></span>`).join('');
                                const durLabel = formatVoiceDurationMs(Number(msg.durationMs) || 0);
                                const iconClass = isCurrentMusic ? 'fas fa-pause' : 'fas fa-play';
                                body = `<div class="glass-voice-player">
                                    <button type="button" class="voice-play-btn" onclick="toggleMusicFromMessage(this)" data-chat-id="${escapeHtml(activeChat.id)}" data-msg-id="${escapeHtml(msg.id || '')}" aria-label="Голосовое">
                                        <i class="${iconClass}"></i>
                                    </button>
                                    <div style="flex:1;min-width:0;display:flex;flex-direction:column;gap:6px;">
                                        <div class="voice-wave">${waveHtml}</div>
                                        <div class="voice-progress"><div class="voice-progress-fill"></div></div>
                                    </div>
                                    <div class="voice-meta"><span class="voice-dur">${durLabel}</span></div>
                                </div>`;
                            }
                        }
                    } else if (isImage) {
                        if (!msg.imageBase64) {
                            body = `<div class="chat-msg-pending">Фото загружается…</div>`;
                        } else {
                            const mime = /^image\/(jpeg|png|gif|webp)$/i.test(String(msg.mimeType || '')) ? msg.mimeType : 'image/jpeg';
                            const b64 = String(msg.imageBase64 || '').replace(/[^a-zA-Z0-9+/=]/g, '');
                            const url = `data:${mime};base64,${b64}`;
                            body = `${msg.text ? `<div style="margin-bottom:6px;">${linkifyMessengerText(msg.text || '', { includePreview: true })}</div>` : ''}<img class="chat-msg-thumb" src="${escapeHtml(url)}" alt="" draggable="false" oncontextmenu="return false" onclick="openImageLightboxFromImg(this)">`;
                        }
                    } else if (isVideo) {
                        if (!msg.videoBase64) {
                            body = `<div class="chat-msg-pending">Видео загружается…</div>`;
                        } else {
                            const chatIdEsc = escapeHtml(activeChat.id || '');
                            const msgIdEsc = escapeHtml(msg.id || '');
                            body = `${msg.text ? `<div style="margin-bottom:6px;">${linkifyMessengerText(msg.text || '', { includePreview: true })}</div>` : ''}<div class="glass-video-thumb" role="button" tabindex="0" onclick="openVideoLightboxFromMsg('${chatIdEsc}','${msgIdEsc}')">
                                <div class="video-thumb-overlay"><i class="fas fa-play"></i></div>
                            </div>`;
                        }
                    } else {
                        body = linkifyMessengerText(msg.text || '', { includePreview: true });
                    }
                    const ts = new Date(Number(msg.createdAt || Date.now())).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                    let reply = '';
                    if (msg.replyTo) {
                        const rq = messages.find((x) => x && x.id === msg.replyTo);
                        if (rq && !rq.deletedAt) {
                            const rName = String(rq.senderDisplayName || rq.fromId || 'Пользователь');
                            const rAvatar = String(rq.senderAvatar || '');
                            const rInitials = String(rq.senderInitials || '');
                            reply = `<div role="button" tabindex="0" onclick="scrollAndHighlightMessengerMessage('${escapeHtml(msg.replyTo || '')}')"
                                style="cursor:pointer;font-size:12px;opacity:.98;margin-bottom:6px;border-left:2px solid rgba(255,255,255,.42);padding-left:8px;">
                                <div style="display:flex;align-items:center;gap:8px;min-width:0;">
                                    <div style="width:22px;height:22px;flex-shrink:0;opacity:.98;overflow:hidden;">${avatarMarkup(rName, rAvatar, rInitials)}</div>
                                    <div style="font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${escapeHtml(rName)}</div>
                                </div>
                                <div style="margin-top:3px;opacity:.82;white-space:normal;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden;">
                                    ${linkifyMessengerText(rq.text || '')}
                                </div>
                            </div>`;
                        } else {
                            reply = `<div style="font-size:12px;opacity:.72;margin-bottom:6px;border-left:2px solid rgba(255,255,255,.35);padding-left:8px;">Ответ на сообщение</div>`;
                        }
                    }
                    let forwardedBlock = '';
                    let finalBody = body;
                    const fp = msg && msg.forwardedPreview && typeof msg.forwardedPreview === 'object' ? msg.forwardedPreview : null;
                    if (fp && (fp.fromUserId || fp.displayName || fp.text)) {
                        const fName = String(fp.displayName || fp.fromUserId || 'Пользователь');
                        const fAvatar = String(fp.avatar || '');
                        const fIni = String(fp.initials || '');
                        const fText = String(fp.text || '');
                        forwardedBlock = `<div style="font-size:12px;opacity:.98;margin-bottom:6px;border-left:2px solid rgba(255,255,255,.42);padding-left:8px;">
                            <div style="font-size:11px;opacity:.78;font-weight:900;margin-bottom:3px;">Переслано</div>
                            <div style="display:flex;align-items:center;gap:8px;min-width:0;">
                                <div style="width:22px;height:22px;flex-shrink:0;opacity:.98;overflow:hidden;">${avatarMarkup(fName, fAvatar, fIni)}</div>
                                <div style="font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${escapeHtml(fName)}</div>
                            </div>
                            ${fText ? `<div style="margin-top:3px;opacity:.82;white-space:normal;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden;">${linkifyMessengerText(fText)}</div>` : ''}
                        </div>`;
                    } else if (/^Переслано:/i.test(String(msg.text || ''))) {
                        const raw = String(msg.text || '');
                        const lines = raw.split('\n');
                        const first = lines.shift() || '';
                        const fName = first.replace(/^Переслано:\s*/i, '').trim();
                        const fText = lines.join('\n').trim();
                        forwardedBlock = `<div style="font-size:12px;opacity:.98;margin-bottom:6px;border-left:2px solid rgba(255,255,255,.42);padding-left:8px;">
                            <div style="font-size:11px;opacity:.78;font-weight:900;margin-bottom:3px;">Переслано</div>
                            <div style="display:flex;align-items:center;gap:8px;min-width:0;">
                                <div style="width:22px;height:22px;flex-shrink:0;opacity:.98;overflow:hidden;">${avatarMarkup(fName, '', '')}</div>
                                <div style="font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${escapeHtml(fName || 'Пользователь')}</div>
                            </div>
                            <div style="margin-top:3px;opacity:.82;white-space:normal;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden;">
                                ${linkifyMessengerText(fText)}
                            </div>
                        </div>`;
                        finalBody = '';
                    }
                    if (String(msg.messageKind || '') === 'system') {
                        parts.push(`<div class="chat-system-msg">${linkifyMessengerText(String(msg.text || ''))}</div>`);
                        continue;
                    }
                    const groupEvent = parseGroupEventPayload(msg.text || '');
                    if (groupEvent) {
                        parts.push(renderGroupEventBlock(groupEvent));
                        continue;
                    }
                    const uploadPct = msg.uploading ? Math.max(0, Math.min(100, Math.round(Number(msg.uploadProgress || 0)))) : 0;
                    const uploadTag = msg.uploading
                        ? `<div class="chat-upload-tag"><i class="fas fa-circle-notch fa-spin"></i>${uploadPct ? ` ${uploadPct}%` : ''}</div>`
                        : '';
                    const readBy = Array.isArray(msg.readBy) ? msg.readBy.map((x) => String(x)) : [];
                    const isRead = !!msg.read || (mine && peerId && readBy.includes(peerId));
                    const checksHtml = mine
                        ? `<span class="chat-msg-checks ${isRead ? 'read' : ''}"><i class="fas fa-check"></i>${isRead ? '<i class="fas fa-check"></i>' : ''}</span>`
                        : '';
                    const reactionsHtml = renderReactions(msg);
                    const canCtx = !msg.uploading;
                    const dbl = canCtx ? ` ondblclick="quickReactToMessage(event,'${escapeHtml(msg.id || '')}','❤️')"` : '';
                    const evt = canCtx
                        ? `oncontextmenu="openMessageMenu(event,'${escapeHtml(msg.id || '')}',${mine ? 'true' : 'false'})" ontouchstart="startMessageHold(event,'${escapeHtml(msg.id || '')}',${mine ? 'true' : 'false'}); startMessageSwipeStart(event)" ontouchend="handleMessageSwipeEnd(event,'${escapeHtml(msg.id || '')}')" ontouchcancel="cancelMessageHold()"`
                        : '';
                    const senderId = String(msg.fromId || '').trim();
                    const senderName = String(msg.senderDisplayName || msg.fromId || 'Пользователь');
                    const senderAvatar = String(msg.senderAvatar || '');
                    const senderInitials = String(msg.senderInitials || '');
                    const senderLine = !mine && isGroupMessengerChat(activeChat)
                        ? `<div class="chat-sender-line"><span class="chat-sender-name" ${senderId ? `onclick="openUserProfile('${escapeHtml(senderId)}')"` : ''}>${escapeHtml(senderName)}</span></div>`
                        : '';
                    const hoverReplyBtn = !mine
                        ? `<button type="button" class="chat-msg-reply-hover-btn" title="Ответить" aria-label="Ответить" onclick="event.stopPropagation(); setReplyToMessage('${escapeHtml(msg.id || '')}')"><i class="fas fa-reply"></i></button>`
                        : '';
                    const msgHtml = `<div id="chatMsg-${escapeHtml(msgIdSafe)}" class="chat-msg ${mine ? 'out' : ''}" ${evt}${dbl}>${senderLine}${reply}${forwardedBlock}${finalBody}${uploadTag}${reactionsHtml}${hoverReplyBtn}<div class="chat-msg-meta"><span>${ts}${msg.editedAt ? ' • изм.' : ''}</span>${checksHtml}</div></div>`;
                    if (!mine && isGroupMessengerChat(activeChat)) {
                        parts.push(
                            `<div class="chat-msg-row"><div class="chat-msg-row-avatar" ${senderId ? `onclick="openUserProfile('${escapeHtml(senderId)}')"` : ''}>${avatarMarkup(senderName, senderAvatar, senderInitials)}</div><div class="chat-msg-row-body">${msgHtml}</div></div>`
                        );
                    } else {
                        parts.push(msgHtml);
                    }
                    }
                    return parts.join('');
                })()
                : `<div class="chat-empty-card"><i class="fas fa-comment-dots"></i><p id="emptyChatPhrase">${getInitialEmptyChatPhrase()}</p></div>`;
            const leftComposerHint = '';
            const baseComposerHint = isLeft
                ? leftComposerHint
                : composerEditMessageId
                ? `<div style="width:100%;margin:0;padding:8px 10px;border-radius:10px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.05);display:flex;justify-content:space-between;gap:8px;align-items:center;box-sizing:border-box;"><span>Редактирование сообщения</span><button type="button" class="contact-btn secondary" onclick="clearComposerReplyEdit()">Отмена</button></div>`
                : composerReplyMessage
                    ? `<div class="composer-reply-preview" style="width:100%;margin:0;padding:8px 10px;border-radius:10px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.05);display:flex;justify-content:space-between;gap:10px;align-items:flex-start;box-sizing:border-box;">
                        <div style="display:flex;gap:10px;align-items:flex-start;min-width:0;flex:1;">
                            <div style="width:34px;height:34px;flex-shrink:0;">${avatarMarkup(
                                String(composerReplyMessage.senderDisplayName || composerReplyMessage.fromId || 'Пользователь'),
                                String(composerReplyMessage.senderAvatar || ''),
                                String(composerReplyMessage.senderInitials || '')
                            )}</div>
                            <div style="min-width:0;display:flex;flex-direction:column;gap:3px;">
                                <div style="font-size:12px;opacity:.78;font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                                    Ответ: ${escapeHtml(String(composerReplyMessage.senderDisplayName || composerReplyMessage.fromId || 'Пользователь'))}
                                </div>
                                <div class="composer-reply-text" style="font-size:13px;opacity:.92;white-space:normal;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;">
                                    ${escapeHtml((composerReplyMessage.text || '').length > 30 ? (composerReplyMessage.text || '').substring(0, 30) + '...' : (composerReplyMessage.text || ''))}
                                </div>
                            </div>
                        </div>
                        <button type="button" class="contact-btn secondary" onclick="clearComposerReplyEdit()" style="flex-shrink:0;">Отмена</button>
                    </div>`
                    : '';
            const mediaComposerHint = !isLeft && composerMediaDraft && composerMediaDraft.b64
                ? (() => {
                    const kind = String(composerMediaDraft.kind || '').trim();
                    const mime = String(composerMediaDraft.mime || '').trim() || (kind === 'video' ? 'video/mp4' : kind === 'image' ? 'image/jpeg' : 'audio/webm');
                    const src = `data:${mime};base64,${String(composerMediaDraft.b64 || '')}`;
                    const title = kind === 'video' ? 'Видео' : kind === 'image' ? 'Фото' : 'Аудио';
                    const fileName = String(composerMediaDraft.name || '').trim();
                    const preview =
                        kind === 'image'
                            ? `<img src="${escapeHtml(src)}" alt="" style="width:46px;height:46px;border-radius:10px;object-fit:cover;border:1px solid rgba(255,255,255,.12);" draggable="false">`
                            : kind === 'video'
                                ? `<video src="${escapeHtml(src)}" muted playsinline style="width:46px;height:46px;border-radius:10px;object-fit:cover;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.35);"></video>`
                                : `<div style="width:46px;height:46px;border-radius:10px;border:1px solid rgba(255,255,255,.12);display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.06);"><i class="fas fa-music"></i></div>`;
                    return `<div style="width:100%;margin:0;padding:8px 10px;border-radius:10px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.05);display:flex;justify-content:space-between;gap:10px;align-items:flex-start;box-sizing:border-box;">
                        <div style="display:flex;gap:10px;align-items:flex-start;min-width:0;flex:1;">
                            ${preview}
                            <div style="min-width:0;display:flex;flex-direction:column;gap:3px;">
                                <div style="font-size:12px;opacity:.78;font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                                    ${escapeHtml(title)}
                                </div>
                                <div style="font-size:13px;opacity:.92;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                                    ${escapeHtml(fileName || '')}
                                </div>
                            </div>
                        </div>
                        <button type="button" class="contact-btn secondary" onclick="clearComposerMediaDraft()" style="flex-shrink:0;">Удалить</button>
                    </div>`;
                })()
                : '';
            const composerHint = [baseComposerHint, mediaComposerHint].filter(Boolean).join('');
            const recBarsHtml = Array.from({ length: 14 }, (_, i) => `<span style="height:${10 + (i % 5) * 4}px"></span>`).join('');
            let composerRowInner = '';
            if (isLeft) {
                composerRowInner = `
                            <button type="button" class="contact-btn" onclick="rejoinGroupChat('${escapeHtml(activeChatIdResolved)}')" style="width:100%;padding:14px 18px;font-size:15px;">
                                <i class="fas fa-arrow-left"></i> Вернуться в чат
                            </button>`;
            } else if (voiceRecordingActive) {
                composerRowInner = `
                            <div class="voice-rec-bar">
                                <div class="voice-rec-bars">${recBarsHtml}</div>
                                <span class="voice-rec-timer" id="voiceRecTimerUi">0:00</span>
                            </div>
                            <button type="button" id="chatComposerActionBtn" class="messenger-nav-btn" onclick="stopVoiceRecordingCapture()" title="Стоп"><i class="fas fa-stop"></i></button>`;
            } else if (voiceRecordPreview) {
                const prevDur = formatVoiceDurationMs(voiceRecordPreview.durationMs || 0);
                composerRowInner = `
                            <div class="voice-preview-bar">
                                <button type="button" class="messenger-nav-btn" onclick="toggleVoicePreviewPlay(this)" title="Прослушать"><i class="fas fa-play"></i></button>
                                <span style="font-size:13px;font-variant-numeric:tabular-nums;opacity:.88;">${prevDur}</span>
                                <span style="flex:1"></span>
                                <button type="button" class="messenger-nav-btn" onclick="discardVoicePreview()" title="Удалить"><i class="fas fa-trash"></i></button>
                            </div>
                            <input type="file" id="chatMediaInput" accept="image/*,video/*,audio/*" style="display:none" onchange="onChatMediaSelected(event)">
                            <button type="button" class="messenger-nav-btn" disabled title="Фото или видео"><i class="fas fa-paperclip"></i></button>
                            <button type="button" id="chatComposerActionBtn" class="messenger-nav-btn" onclick="sendVoiceFromPreview()" title="Отправить"><i class="fas fa-paper-plane"></i></button>`;
            } else {
                composerRowInner = `
                            <textarea id="chatComposerInput" placeholder="${escapeHtml(composerPlaceholder)}" ${composerLocked ? 'disabled' : ''} oninput="onComposerInput()" onkeydown="onComposerKeydown(event)" onblur="handleComposerBlur()"></textarea>
                            <input type="file" id="chatMediaInput" accept="image/*,video/*,audio/*" style="display:none" onchange="onChatMediaSelected(event)">
                            <button type="button" class="messenger-nav-btn" title="Фото или видео" ${composerLocked ? 'disabled' : ''} onclick="document.getElementById('chatMediaInput')?.click()"><i class="fas fa-paperclip"></i></button>
                            <button type="button" id="chatComposerActionBtn" class="messenger-nav-btn sm-composer-send" ${composerLocked ? 'disabled' : ''} onclick="composerPrimaryAction(event)"><i class="fas fa-microphone"></i></button>`;
            }
            const callBannerHtml = isGroupMessengerChat(activeChat) ? renderActiveGroupCallBanner(activeChat) : '';
            const callBannerBlock = `<div class="chat-call-banner" style="display:${callBannerHtml ? 'block' : 'none'};">${callBannerHtml}</div>`;
            const wallpaper = String(messengerAppearance?.chatWallpaper || '').trim();
            const wallpaperBlur = messengerAppearance?.chatWallpaperBlur !== false;
            return `
                <div class="chat-workspace">
                    <div class="chat-wallpaper-layer ${wallpaper && wallpaperBlur ? 'blur' : ''}" style="display:${wallpaper ? 'block' : 'none'};${wallpaper ? `background-image:url('${escapeHtml(wallpaper).replace(/'/g, '&#39;')}');` : ''}"></div>
                    <div class="chat-topbar">
                        <div style="display:flex;align-items:center;gap:10px;">
                            ${isMobileLayout() ? `<button type="button" class="messenger-nav-btn" onclick="closeMobileChatView()" aria-label="Назад"><i class="fas fa-arrow-left"></i></button>` : ''}
                            <div class="messenger-avatar" onclick="${isGroupMessengerChat(activeChat) ? `openGroupProfileModal('${escapeHtml(activeChat.id || '')}')` : `openUserProfile('${escapeHtml(activeChat.peer?.id || '')}')`}" style="cursor:pointer;">${avatarMarkup(activeChatTitle, activeChat.peer?.avatar || activeChat.group?.avatar || '', String(activeChat.peer?.initials || ''))}</div>
                            <div>
                                <div style="font-weight:700;">${escapeHtml(activeChatTitle)}</div>
                                <div style="font-size:12px;opacity:.8;"><span class="chat-header-status">${escapeHtml(isGroupMessengerChat(activeChat) ? getGroupChatStatusText(activeChat) : statusText)}</span></div>
                            </div>
                        </div>
                        <div style="display:flex;gap:8px;">
                            <button type="button" class="messenger-nav-btn" ${isGroupMessengerChat(activeChat) ? (isLeft ? 'disabled' : '') : (composerLocked ? 'disabled' : '')} onclick="${isGroupMessengerChat(activeChat) ? `startGroupCallForChat('${escapeHtml(activeChat.id || '')}')` : `callFriend('${escapeHtml(activeChat.peer?.id || '')}')`}" title="${isGroupMessengerChat(activeChat) ? (isLeft ? 'Сначала вернитесь в чат' : 'Групповой звонок') : 'Позвонить'}"><i class="fas fa-phone" style="${isGroupMessengerChat(activeChat) && activeChat.group?.activeCall?.roomId ? 'color:#5be37a;' : ''}"></i></button>
                            ${isGroupMessengerChat(activeChat) ? `<button type="button" class="messenger-nav-btn" onclick="openGroupProfileModal('${escapeHtml(activeChat.id || '')}')" title="Информация"><i class="fas fa-ellipsis-v"></i></button>` : `<button type="button" class="messenger-nav-btn" onclick="toggleBlockActivePeer()" title="${blockedPeer ? 'Разблокировать' : 'Заблокировать'}"><i class="fas ${blockedPeer ? 'fa-unlock' : 'fa-ban'}"></i></button>`}
                        </div>
                    </div>
                    ${callBannerBlock}
                    <div class="chat-history">${groupBanned ? renderGroupBlockedScreen(activeChat) : (rows || '')}</div>
                    ${groupBanned ? '' : `<div class="chat-fab-stack">
                        <div id="scrollToMentionFabWrap" class="scroll-to-bottom-fab-wrap" style="display:${messengerMentionWhileScrolledCount ? 'flex' : 'none'};">
                            <button type="button" class="scroll-to-bottom-fab" onclick="scrollMessengerHistoryToNextMention()" aria-label="Перейти к упоминанию">
                                <i class="fas fa-at"></i>
                                <span id="scrollToMentionFabBadge" class="scroll-to-bottom-fab-badge">${messengerMentionWhileScrolledCount > 99 ? '99+' : (messengerMentionWhileScrolledCount || 0)}</span>
                            </button>
                        </div>
                        <div id="scrollToBottomFabWrap" class="scroll-to-bottom-fab-wrap" style="display:none;">
                            <button type="button" class="scroll-to-bottom-fab" onclick="scrollMessengerHistoryToBottom()" aria-label="Перейти вниз">
                                <i class="fas fa-arrow-down"></i>
                                <span id="scrollToBottomFabBadge" class="scroll-to-bottom-fab-badge">${messengerNewWhileScrolledCount > 99 ? '99+' : (messengerNewWhileScrolledCount || 0)}</span>
                            </button>
                        </div>
                    </div><div class="chat-input-wrap">
                        ${composerHint}
                        <div id="composerMentionMenuHost" class="composer-mention-host"></div>
                        <div class="chat-composer-row">
                            ${composerRowInner}
                        </div>
                    </div>`}
                </div>
            `;
        }

        function renderMainScreen() {
            if (!authProfile) {
                renderAuthScreen();
                return;
            }
            const focusSnap = captureMessengerFocusSnapshot();
            // Если пользователь прямо сейчас скроллит историю — не перерисовываем чат,
            // иначе скролл "дергается" (особенно на мобиле).
            if ((messengerView === 'chats' && messengerIsUserScrolling) || (messengerView === 'notifications' && messengerWorkspaceIsUserScrolling)) {
                messengerRenderPendingAfterScroll = true;
                return;
            }
            // Snapshot прокрутки истории перед перерисовкой (чтобы не прыгало вверх/вниз).
            const hist = document.querySelector('.chat-history');
            const histSnapshot = hist
                ? {
                      scrollTop: hist.scrollTop,
                      distFromBottom: hist.scrollHeight - hist.scrollTop - hist.clientHeight,
                      wasNearBottom: hist.scrollHeight - hist.scrollTop - hist.clientHeight < 80
                  }
                : null;
            // Не даём слушателю скролла “стрелять” в момент пользовательского скролла.
            const autoScrollAllowed = !messengerIsUserScrolling;
            try {
                if (voiceRecordingActive && voiceMediaRecorder && messengerView !== 'chats') {
                    const mr = voiceMediaRecorder;
                    mr.onstop = () => {
                        clearVoiceRecTimerUi();
                        try {
                            if (voiceMediaStream) voiceMediaStream.getTracks().forEach((t) => t.stop());
                        } catch (_) {}
                        voiceMediaStream = null;
                        voiceMediaRecorder = null;
                        voiceRecordChunks = [];
                        voiceRecordingActive = false;
                        voiceRecordStartedAt = 0;
                    };
                    if (typeof mr.requestData === 'function') mr.requestData();
                    mr.stop();
                }
            } catch (_) {}
            const isMobile = isMobileLayout();
            const sidebarVisible = !messengerMobileWorkspaceOpen();
            const sidebarActiveGroupCallsHtml = sidebarVisible ? renderGlobalActiveGroupCallWidgets() : '';
            const mobileWorkspaceActiveCallsHtml = isMobile && messengerView !== 'chats' ? renderGlobalActiveGroupCallWidgets() : '';
            const mobileBackBar = (isMobile && messengerView !== 'chats')
                ? `<div class="mobile-workspace-bar sm-mobile-bar"><button type="button" class="messenger-nav-btn" onclick="setMessengerView('chats')" aria-label="Назад"><i class="fas fa-arrow-left"></i></button><span>${escapeHtml({ friends: 'Друзья', settings: 'Настройки', profile: 'Профиль', calls: 'Звонки', notifications: 'Уведомления' }[messengerView] || '')}</span></div>`
                : '';
            const friendsPanelHtml = `
                    <div class="workspace-scroll sm-workspace sm-workspace--friends messenger-friends-tab">
                        <div class="sm-friends-v4-head">
                            <div class="sm-friends-v4-title">Друзья</div>
                            <div class="sm-friends-v4-sub">Контакты, заявки и поиск</div>
                        </div>
                        <div class="sm-friends-v4-search">
                            <i class="fas fa-search"></i>
                            <input id="friendsSearchInput" class="modal-input sm-input" placeholder="Поиск по ID, имени или username" autocomplete="off" oninput="onFriendsSearchInput(event)">
                        </div>
                        <div class="sm-friends-v4-body">${renderFriendsTabContent()}</div>
                    </div>`;
            const workspaceHtml = messengerView === 'calls'
                ? mobileBackBar + `
                    <div class="workspace-scroll sm-workspace sm-workspace--calls">
                        <div class="sm-workspace-hero">
                            <div class="sm-workspace-hero-icon"><i class="fas fa-phone-alt"></i></div>
                            <h2>Звонки</h2>
                            <p>Создайте комнату или подключитесь по ссылке</p>
                        </div>
                        <div class="sm-calls-grid">
                            <button type="button" class="sm-action-card" onclick="createRoom()">
                                <span class="sm-action-card-icon"><i class="fas fa-video"></i></span>
                                <span class="sm-action-card-title">Создать комнату</span>
                                <span class="sm-action-card-desc">Групповой видеозвонок</span>
                            </button>
                            <button type="button" class="sm-action-card" onclick="showJoinModal()">
                                <span class="sm-action-card-icon"><i class="fas fa-link"></i></span>
                                <span class="sm-action-card-title">Подключиться</span>
                                <span class="sm-action-card-desc">Войти по ID комнаты</span>
                            </button>
                        </div>
                    </div>
                `
                : messengerView === 'friends'
                    ? mobileBackBar + friendsPanelHtml
                    : messengerView === 'notifications'
                        ? mobileBackBar + renderNotificationsWorkspace()
                    : messengerView === 'settings'
                            ? mobileBackBar + `
                                <div class="workspace-scroll sm-workspace sm-workspace--settings sm-settings sm-settings-v3">
                                    <div class="sm-v3-page">
                                        <div class="sm-v3-page-head">
                                            <div class="sm-v3-page-title">Настройки</div>
                                            <div class="sm-v3-page-subtitle">Аккаунт • приватность • оформление</div>
                                        </div>
                                        
                                        <div class="sm-v3-section">
                                            <div class="sm-v3-section-title">Аккаунт</div>
                                            <button type="button" class="sm-v3-row" onclick="setMessengerView('profile')">
                                                <span class="sm-v3-row-icon"><i class="fas fa-user"></i></span>
                                                <span class="sm-v3-row-body">
                                                    <span class="sm-v3-row-title">Профиль</span>
                                                    <span class="sm-v3-row-desc">Просмотр и редактирование</span>
                                                </span>
                                                <span class="sm-v3-row-right"><i class="fas fa-chevron-right"></i></span>
                                            </button>
                                            <button type="button" class="sm-v3-row" onclick="openDevicesSettingsModal()">
                                                <span class="sm-v3-row-icon"><i class="fas fa-laptop-mobile-screen"></i></span>
                                                <span class="sm-v3-row-body">
                                                    <span class="sm-v3-row-title">Устройства</span>
                                                    <span class="sm-v3-row-desc">Сессии и вход по QR</span>
                                                </span>
                                                <span class="sm-v3-row-right"><i class="fas fa-chevron-right"></i></span>
                                            </button>
                                        </div>

                                        <div class="sm-v3-section">
                                            <div class="sm-v3-section-title">Конфиденциальность</div>
                                            <button type="button" class="sm-v3-row" onclick="openPrivacySettingsModal()">
                                                <span class="sm-v3-row-icon"><i class="fas fa-user-shield"></i></span>
                                                <span class="sm-v3-row-body">
                                                    <span class="sm-v3-row-title">Приватность</span>
                                                    <span class="sm-v3-row-desc">Кто может писать, звонить и видеть профиль</span>
                                                </span>
                                                <span class="sm-v3-row-right"><i class="fas fa-chevron-right"></i></span>
                                            </button>
                                            <button type="button" class="sm-v3-row" onclick="openBlacklistModal()">
                                                <span class="sm-v3-row-icon"><i class="fas fa-ban"></i></span>
                                                <span class="sm-v3-row-body">
                                                    <span class="sm-v3-row-title">Чёрный список</span>
                                                    <span class="sm-v3-row-desc">Заблокированные пользователи</span>
                                                </span>
                                                <span class="sm-v3-row-right">
                                                    <span class="sm-v3-pill">${(messengerProfile.blacklist || []).length}</span>
                                                    <i class="fas fa-chevron-right"></i>
                                                </span>
                                            </button>
                                        </div>

                                        <div class="sm-v3-section">
                                            <div class="sm-v3-section-title">Внешний вид</div>
                                            <button type="button" class="sm-v3-row" onclick="openAppearanceSettingsModal()">
                                                <span class="sm-v3-row-icon"><i class="fas fa-palette"></i></span>
                                                <span class="sm-v3-row-body">
                                                    <span class="sm-v3-row-title">Оформление</span>
                                                    <span class="sm-v3-row-desc">Тема, обои чата и цвета</span>
                                                </span>
                                                <span class="sm-v3-row-right"><i class="fas fa-chevron-right"></i></span>
                                            </button>
                                        </div>

                                        <button type="button" class="sm-v3-danger" onclick="signOutProfile()">
                                            <i class="fas fa-sign-out-alt"></i>
                                            <span>Выйти из аккаунта</span>
                                        </button>
                                    </div>
                                </div>
                            `
                        : renderMessengerWorkspace();
            const notificationTotal = getMessengerNotificationUnreadTotal();
            // Show all chats — direct chats included
            const visibleChats = messengerChats;
            const chatsFiltered = visibleChats;
            const chatItems = chatsFiltered.length
                ? chatsFiltered.map((chat) => {
                    const myId = String(authProfile?.appUserId || '').trim();
                    const isDirect = isDirectMessengerChat(chat);
                    const isGroup = isGroupMessengerChat(chat);
                    const leftState = isGroup ? getGroupLeaveStateClient(chat, myId) : null;
                    const frozenAt = Math.max(0, Number(leftState?.frozenAt || leftState?.leftAt || 0)) || 0;
                    let preview = '';
                    let finalPreview = '';

                    // Typing indicator overrides preview
                    if (isDirect) {
                        const typing = messengerTypingByUser.get(chat.peer?.id);
                        if (typing && typing.isTyping && (typing.chatId === chat.id || typing.withUserId === myId)) {
                            const name = chat.peer?.displayName || chat.peer?.name || 'Пользователь';
                            preview = `${name} ${typing.activity === 'voice' ? 'записывает аудио' : 'печатает...'}`;
                            finalPreview = preview;
                        } else {
                            const lm = chat.lastMessage && (!frozenAt || Number(chat.lastMessage.createdAt || 0) <= frozenAt)
                                ? chat.lastMessage
                                : null;
                            const kind = String(lm?.messageKind || '');
                            if (!lm) {
                                preview = 'История очищена';
                            } else {
                                const lmText = String(lm?.text || '');
                                const isVoiceRec = lm?.messageKind === 'voice' && lmText === 'Голосовое сообщение';
                                const isMusic = lm?.messageKind === 'voice' && !!lmText && lmText !== 'Голосовое сообщение';
                                if (isVoiceRec) preview = 'Голосовое сообщение';
                                else if (isMusic) preview = lmText;
                                else preview = lmText || 'История очищена';
                            }
                            finalPreview = kind === 'system' ? messengerPlainTextPreview(preview) : preview;
                            if (lm && kind !== 'system') {
                                const fromId = String(lm?.fromId || '').trim();
                                if (fromId && fromId === myId) {
                                    finalPreview = `Вы: ${preview}`;
                                } else if (fromId && isGroup) {
                                    const senderName = getGroupParticipantDisplayName(chat, fromId)
                                        || resolvePeerDisplay(fromId)?.displayName
                                        || resolvePeerDisplay(fromId)?.name
                                        || fromId;
                                    finalPreview = `${senderName}: ${preview}`;
                                }
                            }
                        }
                    } else if (isGroup) {
                        const gTyping = getGroupChatTypingState(chat);
                        if (gTyping && Array.isArray(gTyping.entries) && gTyping.entries.length) {
                            const names = gTyping.entries.map(entry => entry.name);
                            const verb = gTyping.activity === 'voice' ? 'записывает аудио' : 'печатает';
                            let nameStr = '';
                            if (names.length === 1) nameStr = names[0];
                            else if (names.length === 2) nameStr = names[0] + ' и ' + names[1];
                            else nameStr = names.slice(0, -1).join(', ') + ' и ' + names[names.length - 1];
                            preview = `${nameStr} ${verb}...`;
                            finalPreview = preview;
                        } else {
                            const lm = chat.lastMessage;
                            const kind = String(lm?.messageKind || '');
                            if (!lm) {
                                preview = 'История очищена';
                            } else {
                                const lmText = String(lm?.text || '');
                                const isVoiceRec = lm?.messageKind === 'voice' && lmText === 'Голосовое сообщение';
                                const isMusic = lm?.messageKind === 'voice' && !!lmText && lmText !== 'Голосовое сообщение';
                                if (isVoiceRec) preview = 'Голосовое сообщение';
                                else if (isMusic) preview = lmText;
                                else preview = lmText || 'История очищена';
                            }
                            finalPreview = kind === 'system' ? messengerPlainTextPreview(preview) : preview;
                            if (lm && kind !== 'system') {
                                const fromId = String(lm?.fromId || '').trim();
                                if (fromId && fromId === myId) {
                                    finalPreview = `Вы: ${preview}`;
                                } else if (fromId && isGroup) {
                                    const senderName = getGroupParticipantDisplayName(chat, fromId)
                                        || resolvePeerDisplay(fromId)?.displayName
                                        || resolvePeerDisplay(fromId)?.name
                                        || fromId;
                                    finalPreview = `${senderName}: ${preview}`;
                                }
                            }
                        }
                    } else {
                        const lm = chat.lastMessage;
                        const kind = String(lm?.messageKind || '');
                        if (!lm) preview = 'История очищена';
                        else {
                            const lmText = String(lm?.text || '');
                            const isVoiceRec = lm?.messageKind === 'voice' && lmText === 'Голосовое сообщение';
                            const isMusic = lm?.messageKind === 'voice' && !!lmText && lmText !== 'Голосовое сообщение';
                            if (isVoiceRec) preview = 'Голосовое сообщение';
                            else if (isMusic) preview = lmText;
                            else preview = lmText || 'История очищена';
                        }
                        finalPreview = kind === 'system' ? messengerPlainTextPreview(preview) : preview;
                    }
                    if (isGroup && frozenAt) {
                        preview = 'Вы вышли из чата';
                        finalPreview = preview;
                    }
                    const pdn = chat.peer?.displayName || chat.peer?.name || chat.peer?.id || '';
                    const unread = frozenAt ? 0 : getMessengerUnreadForChat(chat.id);
                    const chatTimeRaw = Number(chat.lastMessage?.createdAt || chat.lastMessage?.timestamp || 0) || 0;
                    const chatTime = chatTimeRaw > 0
                        ? new Date(chatTimeRaw).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                        : '';
                    return `
                    <div class="messenger-chat-item ${chat.id === messengerActiveChatId ? 'active' : ''}" data-chat-id="${escapeHtml(chat.id)}" data-peer-id="${escapeHtml(isDirect ? (chat.peer?.id || '') : '')}" onclick="openMessengerChatById('${escapeHtml(chat.id)}')" oncontextmenu="openChatListContextMenu(event,'${escapeHtml(isDirect ? (chat.peer?.id || '') : '')}','${escapeHtml(chat.id)}')" ontouchstart="startChatListHold(event,'${escapeHtml(isDirect ? (chat.peer?.id || '') : '')}','${escapeHtml(chat.id)}')" ontouchend="cancelChatListHold()" ontouchcancel="cancelChatListHold()">
                        <div class="sm-v3-chatrow">
                            <div class="sm-v3-chatrow-avatar">
                                <div class="messenger-avatar">
                                    ${avatarMarkup(pdn, chat.peer?.avatar || '', chat.peer?.initials)}
                                    ${isDirect ? '<div class="online-indicator" style="' + (chat.peer?.online ? '' : 'display:none') + '"></div>' : ''}
                                </div>
                            </div>
                            <div class="sm-v3-chatrow-main">
                                <div class="sm-v3-chatrow-top">
                                    <div class="sm-v3-chatrow-title">${renderMaybeMarqueeText(pdn, 100, 'messenger-chat-title-text')}</div>
                                </div>
                                <div class="sm-v3-chatrow-bottom">
                                    <div class="sm-v3-chatrow-preview">${escapeHtml(finalPreview)}</div>
                                </div>
                            </div>
                            <div class="sm-v3-chatrow-right">
                                ${chatTime ? `<div class="sm-v3-chatrow-time">${escapeHtml(chatTime)}</div>` : ''}
                                ${unread ? `<div class="sm-v3-unread">${unread > 99 ? '99+' : unread}</div>` : ''}
                            </div>
                        </div>
                    </div>`;
                }).join('')
                : '<div class="chats-empty-card"><i class="fas fa-comments"></i><p>Чатов пока нет</p></div>';
            {
                const prevTa = document.getElementById('chatComposerInput');
                if (prevTa && messengerView === 'chats' && messengerActiveChatId) {
                    composerDraftByPeerId.set(messengerActiveChatId || messengerActivePeerId, prevTa.value);
                }
            }
            const ownProfileName = String(authProfile?.name || authProfile?.appUserId || 'Профиль').trim() || 'Профиль';
            const ownProfileNameShort = ownProfileName.length > 10 ? `${ownProfileName.slice(0, 10)}...` : ownProfileName;
            const bottomNavHtml = isMobile
                ? `<nav class="messenger-bottom-nav" aria-label="Навигация">
                        <button type="button" class="${messengerView === 'chats' && !isChatOpen ? 'active' : ''}" onclick="setMessengerView('chats')"><i class="fas fa-comments"></i><span>Чаты</span></button>
                        <button type="button" class="${messengerView === 'friends' ? 'active' : ''}" onclick="setMessengerView('friends')"><i class="fas fa-user-friends"></i><span>Друзья</span></button>
                        <button type="button" class="${messengerView === 'calls' ? 'active' : ''}" onclick="setMessengerView('calls')"><i class="fas fa-phone"></i><span>Звонки</span></button>
                        <button type="button" class="messenger-bottom-nav-profile ${messengerView === 'profile' ? 'active' : ''}" onclick="setMessengerView('profile')">
                            <span class="messenger-bottom-nav-profile-avatar">${avatarMarkup(ownProfileName, authProfile?.avatar || '', authProfile?.initials || '')}</span>
                            <span class="messenger-bottom-nav-profile-name">${escapeHtml(ownProfileNameShort)}</span>
                        </button>
                    </nav>`
                : '';
            const shellMobile = isMobile ? 'messenger-shell--mobile' : '';
            const shellWs = messengerMobileWorkspaceOpen() ? 'messenger-shell--workspace' : '';
            const shellTabPager = '';
            const shellMobileConversation =
                isMobile && isChatOpen && messengerView === 'chats' ? 'messenger-shell--mobile-conversation' : '';
            const connState = getMessengerConnectionState();
            const brandConnClass = connState === 'online' ? 'online' : connState === 'offline' ? 'offline' : 'connecting';
            const mobileStoriesBlock = '';
            const sidebarHtml = `
                        <aside class="messenger-sidebar">
                            <div class="sidebar-header sm-v3-sidebar-head">
                                <div class="sm-v3-sidebar-top">
                                    <button type="button" class="sm-v3-iconbtn sidebar-burger sidebar-menu-btn" onclick="toggleMobileNavDrawer();event.stopPropagation();" onpointerdown="event.stopPropagation();" aria-label="Меню">
                                        <i class="fas fa-bars"></i>
                                    </button>
                                    <div class="sm-v3-brand">
                                        <div class="sidebar-brand ${brandConnClass}">${escapeHtml(getMessengerSidebarBrandLabel())}</div>
                                        <div class="messenger-connection">${escapeHtml(connState === 'online' ? 'Онлайн' : connState === 'offline' ? 'Оффлайн' : 'Подключение…')}</div>
                                    </div>
                                    ${isMobile && messengerView === 'chats' && lastActiveChatId && !messengerActiveChatId ? `<button type="button" class="sm-v3-iconbtn" onclick="openReturnToChatButton()" title="Вернуться в чат" aria-label="Вернуться в чат"><i class="fas fa-arrow-left"></i></button>` : ''}
                                    <button type="button" class="sm-v3-iconbtn" onclick="setMessengerView('notifications')" title="Уведомления" aria-label="Уведомления">
                                        <i class="fas fa-bell"></i>${notificationTotal ? `<span class="nav-badge">${notificationTotal > 99 ? '99+' : notificationTotal}</span>` : ''}
                                    </button>
                                </div>
                            </div>
                            <div class="messenger-sidebar-body">
                                ${isMobile ? '' : '<div id="storiesContainer" class="stories-container"></div>'}
                                ${sidebarActiveGroupCallsHtml}
                                <div class="messenger-chat-list sm-v3-chatlist">${chatItems}</div>
                                <button type="button" class="sm-v3-fab" onclick="openCreateGroupModal()" aria-label="Создать чат"><i class="fas fa-pen"></i></button>
                            </div>
                            <div class="sidebar-footer-nav">
                                <button type="button" class="messenger-nav-btn ${messengerView === 'calls' ? 'active' : ''}" onclick="setMessengerView('calls')" title="Звонки"><i class="fas fa-phone"></i></button>
                                <button type="button" class="messenger-nav-btn ${messengerView === 'friends' ? 'active' : ''}" onclick="setMessengerView('friends')" title="Друзья"><i class="fas fa-user-friends"></i></button>
                                <button type="button" class="messenger-nav-btn ${messengerView === 'settings' ? 'active' : ''}" onclick="setMessengerView('settings')" title="Настройки"><i class="fas fa-sliders-h"></i></button>
                                <button type="button" class="messenger-nav-btn ${messengerView === 'profile' ? 'active' : ''}" onclick="setMessengerView('profile')" title="Профиль"><i class="fas fa-user"></i></button>
                            </div>
                        </aside>`;
            // Скролл истории дергаем только когда реально надо (после загрузки истории/открытия чата).
            const profileScrollSnapshot = (() => {
                if (messengerView !== 'profile') return null;
                const el = document.querySelector('.messenger-workspace .workspace-scroll');
                if (!el) return null;
                return { scrollTop: Number(el.scrollTop || 0) || 0 };
            })();
            const notificationsScrollSnapshot = (() => {
                if (messengerView !== 'notifications') return null;
                const el = document.querySelector('.messenger-workspace .workspace-scroll');
                if (!el) return null;
                return { scrollTop: Number(el.scrollTop || 0) || 0 };
            })();
            document.getElementById('app').innerHTML = `
                <div class="main-screen main-screen--messenger">
                    <div class="gradient-bg messenger-gradient"></div>
                    <div class="messenger-shell ${shellMobile} ${shellWs} ${shellTabPager} ${shellMobileConversation}">
                        ${mobileStoriesBlock}
                        ${sidebarHtml}
                        <div class="messenger-workspace">
                            ${mobileWorkspaceActiveCallsHtml}
                            ${messengerView === 'profile'
                                ? (() => {
                                    const own = !messengerViewedProfile;
                                    if (own) {
                                        const storiesHtml = buildProfileStoriesSection({
                                            userId: authProfile?.appUserId || '',
                                            title: 'Мои истории',
                                            own: true
                                        });
                                        const ownUsername = ensureGeneratedMessengerUsername(messengerProfile.username || authProfile.vkUsername || '', authProfile?.appUserId || appUserId);
                                        return `${mobileBackBar}<div class="workspace-scroll sm-workspace sm-workspace--profile"><div class="profile-card" style="max-width:580px;width:100%;margin:6px 0;">
                                            ${renderProfileHeroCard({
                                                userId: authProfile?.appUserId || '',
                                                displayName: authProfile.name || authProfile.appUserId || '',
                                                avatar: authProfile.avatar || '',
                                                coverUrl: authProfile.coverUrl || '',
                                                initials: authProfile.initials || '',
                                                username: ownUsername,
                                                subtitle: messengerProfile.statusText || 'Без статуса',
                                                clickableAvatar: true
                                            })}
                                            <div class="profile-detail-grid">
                                                <div class="contact-item" style="justify-content:space-between;gap:12px;">
                                                    <div><div class="contact-chat">Username</div><div class="contact-name">@${escapeHtml(ownUsername)}</div></div>
                                                    <button type="button" class="contact-btn" onclick="copyTextToClipboard('@${escapeHtml(ownUsername)}','Username скопирован')" title="Скопировать username" style="padding:4px 8px;min-width:auto;"><i class="fas fa-copy"></i></button>
                                                </div>
                                                <div class="contact-item" style="justify-content:space-between;gap:12px;">
                                                    <div><div class="contact-chat">О себе</div><div class="contact-name">${escapeHtml(messengerProfile.statusText || 'Не указано')}</div></div>
                                                </div>
                                                <div class="contact-item" style="justify-content:space-between;gap:12px;">
                                                    <div><div class="contact-chat">ID</div><div class="contact-name">${escapeHtml(authProfile.appUserId || '')}</div></div>
                                                    <button type="button" class="contact-btn" onclick="copyAppUserId()" title="Скопировать ID" style="padding:4px 8px;min-width:auto;"><i class="fas fa-copy"></i></button>
                                                </div>
                                            </div>
                                            <div class="profile-actions"><button type="button" class="contact-btn" onclick="openProfileEditModal()" title="Редактировать"><i class="fas fa-pen"></i></button><button type="button" class="contact-btn" onclick="setMessengerView('settings')" title="Настройки"><i class="fas fa-sliders-h"></i></button></div>
                                            ${storiesHtml}
                                        </div></div>`;
                                    }
                                    const view = messengerViewedProfile || {};
                                    const profile = view.profile || {};
                                    if (!view.ok && view.reason === 'private') {
                                        return `${mobileBackBar}<div class="workspace-scroll"><div class="profile-card" style="max-width:560px;margin:6px 0;"><div class="profile-avatar"><i class="fas fa-gavel"></i></div><div class="profile-name">Профиль закрыт</div><div class="messenger-connection">Доступ к анкете ограничен настройками приватности.</div></div></div>`;
                                    }
                                    if (!view.ok && view.reason === 'blocked') {
                                        return `${mobileBackBar}<div class="workspace-scroll"><div class="profile-card" style="max-width:560px;margin:6px 0;"><div class="profile-avatar">${profile.avatar ? `<img src="${escapeHtml(profile.avatar)}" alt="" referrerpolicy="no-referrer">` : `<i class="fas fa-ban"></i>`}</div><div class="profile-name">${escapeHtml(profile.name || profile.id || '')}</div><div class="messenger-connection">Этот аккаунт ограничил с вами общение.</div><div class="messenger-connection" style="opacity:.85;">${escapeHtml(profile.statusText || '')}</div></div></div>`;
                                    }
                                    const pid = String(profile.id || view.targetUserId || '').trim();
                                    const isSelf = !!pid && String(authProfile?.appUserId || '') === pid;
                                    const isFriend = !isSelf && (friendsState.friends || []).some((f) => String(f.id) === pid);
                                    const dispName = profile.displayName || profile.name || pid || '';
                                    const avLetter = profile.initials || (dispName.trim().split(/\s+/).filter(Boolean).map((p) => p.charAt(0)).join('').slice(0, 2).toUpperCase() || pid.slice(0, 2).toUpperCase());
                                    const effectiveUsername = ensureGeneratedMessengerUsername(profile.username || '', pid);
                                    const unameLine = `@${escapeHtml(effectiveUsername)}`;
                                    const addBtn = !isSelf && !isFriend ? `<button class="contact-btn" title="Добавить" onclick="sendFriendRequest('${escapeHtml(pid)}')"><i class="fas fa-user-plus"></i></button>` : '';
                                    const canAddToChats = !isSelf && canCurrentUserAddProfileToChats(view, isFriend);
                                    const addToGroupBtn = canAddToChats ? `<button class="contact-btn" title="Добавить в чат" onclick="openAddUserToGroupModal('${escapeHtml(pid)}')"><i class="fas fa-comments"></i></button>` : '';
                                    const msgBtn = !isSelf ? `<button class="contact-btn" title="Написать" onclick="openMessengerChat('${escapeHtml(pid)}')"><i class="fas fa-paper-plane"></i></button>` : '';
                                    const callBtn = !isSelf ? `<button class="contact-btn" title="Позвонить" onclick="callFriend('${escapeHtml(pid)}')"><i class="fas fa-phone"></i></button>` : '';
                                    const storiesHtml = buildProfileStoriesSection({
                                        userId: pid,
                                        title: 'Публикации',
                                        own: false
                                    });
                                    return `${mobileBackBar}<div class="workspace-scroll sm-workspace sm-workspace--profile"><div class="profile-card" style="max-width:580px;width:100%;margin:6px 0;">
                                        ${renderProfileHeroCard({
                                            userId: pid,
                                            displayName: dispName,
                                            avatar: profile.avatar || '',
                                            coverUrl: profile.coverUrl || '',
                                            initials: avLetter,
                                            username: effectiveUsername,
                                            subtitle: profile.statusText || '',
                                            clickableAvatar: true
                                        })}
                                        <div class="profile-detail-grid">
                                            <div class="contact-item" style="justify-content:flex-start;"><div><div class="contact-chat">О себе</div><div class="contact-name">${escapeHtml(profile.statusText || 'Не указано')}</div></div></div>
                                            <div class="contact-item" style="justify-content:space-between;gap:12px;">
                                                <div><div class="contact-chat">Username</div><div class="contact-name">${unameLine}</div></div>
                                                <button type="button" class="contact-btn" onclick="copyTextToClipboard('@${escapeHtml(effectiveUsername)}','Username скопирован')" title="Скопировать username" style="padding:4px 8px;min-width:auto;"><i class="fas fa-copy"></i></button>
                                            </div>
                                        </div>
                                        <div class="profile-actions">${addBtn}${addToGroupBtn}${msgBtn}${callBtn}</div>
                                        ${storiesHtml}
                                    </div></div>`;
                                })()
                                : workspaceHtml}
                        </div>
                    </div>
                    ${bottomNavHtml}
                    ${renderMessengerNavDrawer(notificationTotal)}
                </div>
            `;
            syncCallScreenLayoutMode();
            syncMusicIslandWidget();
            // Render stories on all messenger views
            renderStories();
            requestAnimationFrame(() => {
                if (document.getElementById('emptyChatPhrase')) {
                    startEmptyChatPhraseRotation();
                } else {
                    stopEmptyChatPhraseRotation();
                }
                // Restore chat scroll BEFORE binding scroll guard to avoid treating programmatic scroll as user scroll
                const chatHist = document.querySelector('.chat-history');
                if (chatHist && autoScrollAllowed) {
                    if (messengerShouldAutoScroll) {
                        chatHist.scrollTop = chatHist.scrollHeight;
                        messengerShouldAutoScroll = false;
                    } else if (histSnapshot) {
                        if (histSnapshot.wasNearBottom) {
                            chatHist.scrollTop = Math.max(0, chatHist.scrollHeight - chatHist.clientHeight - histSnapshot.distFromBottom);
                        } else {
                            const maxScrollTop = Math.max(0, chatHist.scrollHeight - chatHist.clientHeight);
                            chatHist.scrollTop = Math.max(0, Math.min(Number(histSnapshot.scrollTop || 0), maxScrollTop));
                        }
                    }
                }
                // Гарантируем защиту от “дерганья” скролла.
                bindMessengerHistoryScrollGuard();
                bindMessengerWorkspaceScrollGuard();
                hydrateMessengerLinkPreviews();
                if (focusSnap) {
                    restoreMessengerFocusSnapshot(focusSnap);
                } else {
                    const ta = document.getElementById('chatComposerInput');
                    if (ta && messengerView === 'chats' && messengerActiveChatId) {
                        const draftKey = messengerActiveChatId || messengerActivePeerId;
                        if (composerDraftByPeerId.has(draftKey)) {
                            ta.value = composerDraftByPeerId.get(draftKey);
                        }
                        onComposerInput();
                    }
                    const fsi = document.getElementById('friendsSearchInput');
                    if (fsi && messengerView === 'friends') {
                        fsi.value = friendsSearchValue;
                    }
                }
                if (messengerView === 'chats') {
                    syncComposerMentionMenuDom(resolveActiveMessengerChat());
                }
                if (messengerView === 'profile' && profileScrollSnapshot) {
                    const scrollEl = document.querySelector('.messenger-workspace .workspace-scroll');
                    if (scrollEl) scrollEl.scrollTop = profileScrollSnapshot.scrollTop;
                }
                if (messengerView === 'notifications' && notificationsScrollSnapshot) {
                    const scrollEl = document.querySelector('.messenger-workspace .workspace-scroll');
                    if (scrollEl) scrollEl.scrollTop = notificationsScrollSnapshot.scrollTop;
                }
                updateMessengerNewWhileScrolledFabUI();
                updateMessengerMentionFabUI();
            });
        }

        window.toggleVoicePlay = toggleVoicePlay;
        window.toggleMusicFromMessage = toggleMusicFromMessage;
        window.toggleMusicIslandPlayPause = toggleMusicIslandPlayPause;
        window.stopMusicPlayer = stopMusicPlayer;
        window.seekMusicBy = seekMusicBy;
        window.syncMusicIslandWidget = syncMusicIslandWidget;
        window.scrollAndHighlightMessengerMessage = scrollAndHighlightMessengerMessage;
        window.scrollMessengerHistoryToBottom = scrollMessengerHistoryToBottom;
        window.avatarImgOnError = avatarImgOnError;
        window.togglePrivacyDropdown = togglePrivacyDropdown;
        window.setStoryPrivacy = setStoryPrivacy;
        window.toggleStoryPrivacyDropdown = toggleStoryPrivacyDropdown;
        window.toggleVoicePreviewPlay = toggleVoicePreviewPlay;
        window.stopVoiceRecordingCapture = stopVoiceRecordingCapture;
        window.sendVoiceFromPreview = sendVoiceFromPreview;
        window.createRoom = createRoom;
        window.joinRoom = joinRoom;
        window.showJoinModal = showJoinModal;
        window.toggleDurakCallPanel = toggleDurakCallPanel;
        window.toggleVideo = toggleVideo;
        window.switchCameraFacingMode = switchCameraFacingMode;
        window.toggleAudio = toggleAudio;
        window.startScreenShare = startScreenShare;
        window.endCall = endCall;
        window.copyRoomId = copyRoomId;
        window.showWatchPartyModal = showWatchPartyModal;
        window.stopWatchParty = stopWatchParty;
        window.showContextMenu = showContextMenu;
        window.forceToggleRemoteVideo = forceToggleRemoteVideo;
        window.forceToggleRemoteAudio = forceToggleRemoteAudio;
        window.toggleAdmin = toggleAdmin;
        window.kickUser = kickUser;
        window.toggleParticipantsPanel = toggleParticipantsPanel;
        window.closeParticipantsPanel = closeParticipantsPanel;
        window.showRoomSettingsMenu = showRoomSettingsMenu;
        window.toggleRoomPrivacy = toggleRoomPrivacy;
        window.closeRoomForEveryone = closeRoomForEveryone;
        window.approveJoinRequest = approveJoinRequest;
        window.rejectJoinRequest = rejectJoinRequest;
        window.handleTelegramAuth = handleTelegramAuth;
        window.signOutProfile = signOutProfile;
        window.openDevicesSettingsModal = openDevicesSettingsModal;
        window.openQrScannerModal = openQrScannerModal;
        window.closeQrScannerModal = closeQrScannerModal;
        window.revokeDeviceSessionRemote = revokeDeviceSessionRemote;
        window.toggleAuthClassicProviders = toggleAuthClassicProviders;
        window.renderContactsModal = renderContactsModal;
        window.renderVkContactsModal = renderVkContactsModal;
        window.refreshTelegramContacts = refreshTelegramContacts;
        window.refreshVkContacts = refreshVkContacts;
        window.callTelegramContact = callTelegramContact;
        window.callVkContact = callVkContact;
        window.addTelegramContactFromModal = addTelegramContactFromModal;
        window.addVkContactFromModal = addVkContactFromModal;
        window.removeTelegramContact = removeTelegramContact;
        window.removeVkContact = removeVkContact;
        window.handleParticipantTap = handleParticipantTap;
        window.requestFriendFromCall = requestFriendFromCall;
        window.setFriendsTab = setFriendsTab;
        window.toggleFriendsHomePanel = toggleFriendsHomePanel;
        window.closeFriendsHomePanel = closeFriendsHomePanel;
        window.onFriendsSearchInput = onFriendsSearchInput;
        window.sendFriendRequest = sendFriendRequest;
        window.handleFriendRequest = handleFriendRequest;
        window.deleteFriend = deleteFriend;
        window.callFriend = callFriend;
        window.replyIncomingCall = replyIncomingCall;
        window.copyAppUserId = copyAppUserId;
        window.showFriendsSettingsMenu = showFriendsSettingsMenu;
        window.persistFriendsNotifyValue = persistFriendsNotifyValue;
        window.closeIncomingFriendModal = closeIncomingFriendModal;
        window.acceptIncomingFriendFromModal = acceptIncomingFriendFromModal;
        window.setMessengerView = setMessengerView;
        window.toggleMobileNavDrawer = toggleMobileNavDrawer;
        window.closeMobileNavDrawer = closeMobileNavDrawer;
        window.navigateFromNavDrawer = navigateFromNavDrawer;
        window.handleMessengerPagerTouchStart = handleMessengerPagerTouchStart;
        window.handleMessengerPagerTouchMove = handleMessengerPagerTouchMove;
        window.handleMessengerPagerTouchEnd = handleMessengerPagerTouchEnd;
        window.openMessengerNotification = openMessengerNotification;
        window.openMessengerChat = openMessengerChat;
        window.openChatListContextMenu = openChatListContextMenu;
        window.startChatListHold = startChatListHold;
        window.cancelChatListHold = cancelChatListHold;
        window.clearChatHistoryForMe = clearChatHistoryForMe;
        window.openDeleteChatModal = openDeleteChatModal;
        window.confirmDeleteChat = confirmDeleteChat;
        window.openMentionProfile = openMentionProfile;
        window.openUserProfile = openUserProfile;
        window.openStoryAuthorProfile = openStoryAuthorProfile;
        window.openStoryViewerProfile = openStoryViewerProfile;
        window.sendMessageFromComposer = sendMessageFromComposer;
        window.openAppearanceSettingsModal = openAppearanceSettingsModal;
        window.composerPrimaryAction = composerPrimaryAction;
        window.updateMessengerSidebarStatus = updateMessengerSidebarStatus;
        window.onComposerKeydown = onComposerKeydown;
        window.onComposerInput = onComposerInput;
        window.openProfileEditModal = openProfileEditModal;
        window.openImageLightbox = openImageLightbox;
        window.openImageLightboxFromImg = openImageLightboxFromImg;
        window.openVideoLightboxFromMsg = openVideoLightboxFromMsg;
        window.openVideoLightbox = openVideoLightbox;
        window.seychVideoToggle = seychVideoToggle;
        window.onChatMediaSelected = onChatMediaSelected;
        window.setPrivacyRule = setPrivacyRule;
        window.removeUserFromBlacklist = removeUserFromBlacklist;
        window.openBlacklistModal = openBlacklistModal;
        window.closeBlacklistModal = closeBlacklistModal;
        window.getInitialEmptyChatPhrase = getInitialEmptyChatPhrase;
        window.startEmptyChatPhraseRotation = startEmptyChatPhraseRotation;
        window.stopEmptyChatPhraseRotation = stopEmptyChatPhraseRotation;
        window.toggleBlockActivePeer = toggleBlockActivePeer;
        window.openMessageMenu = openMessageMenu;
        window.copyMessengerMessage = copyMessengerMessage;
        window.startMessageHold = startMessageHold;
        window.cancelMessageHold = cancelMessageHold;
        window.setReplyToMessage = setReplyToMessage;
        window.startEditMessage = startEditMessage;
        window.deleteMessageById = deleteMessageById;
        window.openForwardModal = openForwardModal;
        window.forwardMessageToChat = forwardMessageToChat;
        window.clearComposerReplyEdit = clearComposerReplyEdit;
        window.closeMobileChatView = closeMobileChatView;
        window.minimizeCallToIsland = minimizeCallToIsland;
        window.restoreCallFromIsland = restoreCallFromIsland;
        window.startGroupCallForChat = startGroupCallForChat;
        window.joinActiveGroupCall = joinActiveGroupCall;
        window.openCreateGroupModal = openCreateGroupModal;
        window.openGroupProfileModal = openGroupProfileModal;
        window.openGroupSettingsModal = openGroupSettingsModal;
        window.openGroupEditModal = openGroupEditModal;
        window.openAddMembersToGroupModal = openAddMembersToGroupModal;
        window.openGroupMemberActionModal = openGroupMemberActionModal;
        window.toggleGroupMemberActionFields = toggleGroupMemberActionFields;
        window.leaveGroupChat = leaveGroupChat;
        window.joinGroupByInvite = joinGroupByInvite;
        window.openAddUserToGroupModal = openAddUserToGroupModal;
        window.addUserToGroupChat = addUserToGroupChat;

        async function bootApp() {
            initSoundEffects();
            initMessengerAntiCopyGuards();
            loadMessengerTheme();
            authProfile = loadStoredProfile();
            messengerProfile = getStoredMessengerProfile();
            appUserId = authProfile?.appUserId || '';
            friendsNotificationsEnabled = getStoredFriendsNotifyValue();
            telegramContacts = loadTelegramContacts();
            // Initialize Durak card back style
            updateDurakCardBackStyle();
            vkCustomContacts = loadVkCustomContacts();
            vkHiddenContactIds = loadVkHiddenContacts();
            window.addEventListener('offline', () => {
                updateMessengerSidebarStatus();
                if (roomId) reconnectNow();
            });
            window.addEventListener('online', () => {
                updateMessengerSidebarStatus();
                if (roomId) reconnectNow();
            });
            document.addEventListener('visibilitychange', () => {
                if (!document.hidden) {
                    try {
                        messengerIsUserScrolling = false;
                        messengerWorkspaceIsUserScrolling = false;
                        if (messengerUserScrollTimer) {
                            clearTimeout(messengerUserScrollTimer);
                            messengerUserScrollTimer = null;
                        }
                    } catch (_) {}
                    restoreChatHistoryScrollState();
                    if (!roomId) recoverAfterTabWakeup();
                } else {
                    saveChatHistoryScrollState();
                }
            });
            window.addEventListener('focus', () => {
                recoverAfterTabWakeup();
            });
            window.addEventListener('pageshow', () => {
                recoverAfterTabWakeup();
            });
            if (!HOSTING_SAFE_MODE && 'serviceWorker' in navigator) {
                navigator.serviceWorker.addEventListener('message', handleServiceWorkerMessage);
            }
            if (!window.__seychUiDelegatesInit) {
                window.__seychUiDelegatesInit = true;
                document.body.addEventListener('click', (e) => {
                    const playBtn = e.target.closest && e.target.closest('.voice-play-btn');
                    if (playBtn) {
                        // На кнопке уже есть inline `onclick`, поэтому делегированный обработчик
                        // может вызывать двойное переключение (включить и тут же выключить).
                        // Оставляем поведение inline-обработчика.
                    }
                    const inviteLink = e.target.closest && e.target.closest('a.chat-msg-link, a.msg-link-preview-card');
                    if (inviteLink) {
                        const inviteCode = extractGroupInviteCodeFromHref(inviteLink.getAttribute('href') || inviteLink.href || '');
                        if (inviteCode) {
                            e.preventDefault();
                            e.stopPropagation();
                            pendingGroupInviteCode = inviteCode;
                            consumePendingGroupInviteIfAny(inviteCode);
                            return;
                        }
                    }
                    if (e.target.closest && (e.target.closest('.privacy-dd-trigger') || e.target.closest('.privacy-dd-panel'))) {
                        return;
                    }
                    document.querySelectorAll('.privacy-dd-panel').forEach((p) => p.classList.remove('open'));
                });
            }
            let messengerResizeTimer = null;
            let lastMessengerLayoutWidth = window.innerWidth;
            window.addEventListener('resize', () => {
                updateParticipantsResponsiveUI();
                if (durakGameState) {
                    clearTimeout(window.__durakResizeUiT);
                    window.__durakResizeUiT = setTimeout(() => {
                        renderDurakUi();
                    }, 120);
                }
                if (document.activeElement && document.activeElement.id === 'chatComposerInput') return;
                if (!roomId && authProfile) {
                    const w = window.innerWidth;
                    if (Math.abs(w - lastMessengerLayoutWidth) < 48) return;
                    lastMessengerLayoutWidth = w;
                    if (messengerResizeTimer) clearTimeout(messengerResizeTimer);
                    messengerResizeTimer = setTimeout(() => { renderMainScreen(); }, 200);
                }
            }, { passive: true });
            document.addEventListener('pointerdown', () => {
                if (roomId) primeCallAudioSession();
            }, { passive: true });
            if (authProfile) {
                userName = authProfile.name || '';
                userAvatar = authProfile.avatar || '';
                restoreMessengerSessionPeer();
                void (async () => {
                    await registerCurrentDeviceSession();
                    const valid = await validateCurrentDeviceSession();
                    if (!valid) return;
                    if (!ws || ws.readyState !== WebSocket.OPEN) {
                        connectWS({
                            type: 'messenger-register',
                            appUserId: authProfile.appUserId || appUserId,
                            deviceSessionId: getDeviceSessionId(),
                            userName,
                            userAvatar
                        });
                    } else {
                        syncMessengerIdentity();
                    }
                    startDeviceSessionWatchdog();
                    const qrTok = parseQrLoginFromLocation() || pendingQrLoginToken;
                    pendingQrLoginToken = '';
                    if (qrTok) {
                        setTimeout(() => openQrApproveModal(qrTok), 500);
                    }
                })();
                pendingGroupInviteCode = parseGroupInviteFromLocation();
                consumePendingGroupInviteIfAny();
            }
            pendingRoomJoin = parseRoomFromPath();
            if (!pendingGroupInviteCode) {
                pendingGroupInviteCode = parseGroupInviteFromLocation();
            }
            if (authProfile?.provider === 'telegram' && authProfile.telegramId) {
                fetchTelegramContactsFromApi().finally(() => {
                    ensureFriendsRuntime();
                    renderMainScreen();
                    consumePendingGroupInviteIfAny();
                    if (pendingRoomJoin) {
                        const roomToJoin = pendingRoomJoin;
                        pendingRoomJoin = null;
                        joinRoom(roomToJoin);
                    }
                });
                return;
            }
            if (authProfile?.provider === 'vk' && authProfile.vkAccessToken) {
                fetchVkFriendsFromApi().finally(() => {
                    ensureFriendsRuntime();
                    renderMainScreen();
                    consumePendingGroupInviteIfAny();
                    if (pendingRoomJoin) {
                        const roomToJoin = pendingRoomJoin;
                        pendingRoomJoin = null;
                        joinRoom(roomToJoin);
                    }
                });
                return;
            }
            if (authProfile) {
                ensureFriendsRuntime();
            }
            renderMainScreen();
            consumePendingGroupInviteIfAny();
            if (authProfile && pendingRoomJoin) {
                const roomToJoin = pendingRoomJoin;
                pendingRoomJoin = null;
                joinRoom(roomToJoin);
            }
        }

        startAppWithConditionalLoader();

        // Mobile keyboard fix using Visual Viewport API
        (function initKeyboardFix() {
            if (!window.visualViewport) return;
            const updateViewport = () => {
                const vh = window.visualViewport.height;
                const winH = window.innerHeight;
                const keyboardHeight = Math.max(0, winH - vh);
                document.documentElement.style.setProperty('--vh', `${vh}px`);
                document.documentElement.style.setProperty('--keyboard-height', `${keyboardHeight}px`);
            };
            window.visualViewport.addEventListener('resize', updateViewport);
            window.visualViewport.addEventListener('scroll', updateViewport);
            // Initial call
            updateViewport();
        })();
