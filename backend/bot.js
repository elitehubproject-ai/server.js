// Telegram Bot - токен из переменных окружения Render
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const BOT_USERNAME = process.env.BOT_USERNAME;
const APP_URL = process.env.APP_URL;
const fs = require('fs');
const path = require('path');
const USERS_FILE = path.join(__dirname, 'telegram_users.json');

if (!TELEGRAM_TOKEN) {
    console.error('❌ TELEGRAM_TOKEN error');
    process.exit(1);
}

console.log('🤖 Telegram bot starting...');

let lastUpdateId = 0;

function loadUsers() {
    try {
        if (!fs.existsSync(USERS_FILE)) return [];
        const raw = fs.readFileSync(USERS_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
        return [];
    }
}

function saveUsers(users) {
    fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf8');
}

function upsertTelegramUser(user) {
    const id = String(user?.id || '').trim();
    if (!id) return;
    const usernameRaw = String(user?.username || '').trim();
    const username = usernameRaw ? `@${usernameRaw.replace(/^@+/, '')}` : '';
    const firstName = String(user?.first_name || '').trim();
    const lastName = String(user?.last_name || '').trim();
    const fullName = `${firstName} ${lastName}`.trim() || String(user?.name || 'Telegram User');
    const users = loadUsers();
    const idx = users.findIndex((u) => String(u?.id || '') === id);
    const next = {
        id,
        username,
        name: fullName,
        last_seen: Math.floor(Date.now() / 1000)
    };
    if (idx >= 0) {
        users[idx] = { ...users[idx], ...next };
    } else {
        users.push(next);
    }
    saveUsers(users);
}

async function sendMessageWithKeyboard(chatId, text, keyboard) {
    try {
        await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: chatId,
                text: text,
                reply_markup: JSON.stringify(keyboard),
                disable_web_page_preview: true
            })
        });
    } catch (err) {
        console.error('Send error:', err.message);
    }
}

async function sendMiniAppMessage(chatId, url, text) {
    await sendMessageWithKeyboard(chatId, text, {
        inline_keyboard: [[{ text: '🚀 Открыть Seych', web_app: { url: url } }]]
    });
}

async function sendMainMenu(chatId) {
    await sendMessageWithKeyboard(chatId, '🎥 Seych\n\nВыберите действие:', {
        inline_keyboard: [
            [{ text: '🎥 Создать комнату', web_app: { url: APP_URL + '/' } }],
            [{ text: '🔗 Подключиться', web_app: { url: APP_URL + '/' } }],
            [{ text: '👥 Позвонить контактам', web_app: { url: APP_URL + '/' } }]
        ]
    });
}

async function processMessage(chatId, text, firstName, userId) {
    text = text.trim();
    
    if (text.startsWith('/start')) {
        const parts = text.split(/\s+/);
        const payloadRoom = parts[1] || '';
        
        if (payloadRoom.match(/^id[a-z0-9_-]+$/i)) {
            await sendMiniAppMessage(chatId, APP_URL + '/' + payloadRoom, '🔗 Вход в комнату\n\nID: ' + payloadRoom + '\n\nНажмите кнопку, чтобы присоединиться.');
            await sendMessageWithKeyboard(chatId, '🔗 Ссылка для друга:\nhttps://t.me/' + BOT_USERNAME + '?startapp=' + payloadRoom + '\n\n🌐 Или в браузере:\n' + APP_URL + '/' + payloadRoom, { inline_keyboard: [] });
        } else {
            await sendMainMenu(chatId);
        }
        return;
    }
    
    if (text.startsWith('/join ')) {
        const roomId = text.substring(6).trim();
        if (roomId.match(/^id[a-z0-9_-]+$/i)) {
            await sendMiniAppMessage(chatId, APP_URL + '/' + roomId, '🔗 Подключение к комнате\n\nID: ' + roomId + '\n\nНажмите кнопку, чтобы присоединиться.');
        } else {
            await sendMessageWithKeyboard(chatId, '❌ Неверный формат ID\n\nИспользуйте: /join id12345678', { inline_keyboard: [] });
        }
        return;
    }
    
    if (text === '/contacts') {
        try {
            const response = await fetch('https://seych-call.gt.tc/backend/telegram_contacts.php?telegram_id=' + userId);
            const data = await response.json();
            if (data.success && data.data && data.data.contacts && data.data.contacts.length > 0) {
                let contactsText = '👥 Ваши контакты:\n\n';
                data.data.contacts.forEach(contact => {
                    contactsText += '• ' + contact.name;
                    if (contact.username) contactsText += ' (' + contact.username + ')';
                    contactsText += '\n';
                });
                await sendMessageWithKeyboard(chatId, contactsText, { inline_keyboard: [] });
            } else {
                await sendMessageWithKeyboard(chatId, '👥 Контакты\n\nКонтактов пока нет. Попросите друзей написать боту /start.', { inline_keyboard: [] });
            }
        } catch (err) {
            await sendMessageWithKeyboard(chatId, '👥 Контакты\n\nКонтактов пока нет. Попросите друзей написать боту /start.', { inline_keyboard: [] });
        }
        return;
    }
    
    if (text === '/help') {
        const helpText = '📱 Помощь по Seych\n\n' +
            '1️⃣ Откройте сайт и нажмите "Создать комнату"\n' +
            '2️⃣ Скопируйте ID комнаты\n' +
            '3️⃣ Отправьте ID другу\n' +
            '4️⃣ Друг нажимает "Подключиться" и вводит ID\n\n' +
            '🎤 Микрофон включен по умолчанию\n' +
            '📹 Камера выключена - включите сами\n' +
            '🖥️ Демонстрация экрана (ПК)\n' +
            '👑 Создатель может назначать администраторов\n\n' +
            '💡 Команды бота:\n' +
            '/start - главное меню\n' +
            '/join ID - подключиться к комнате\n' +
            '/contacts - список контактов\n' +
            '/help - эта справка';
        await sendMessageWithKeyboard(chatId, helpText, { inline_keyboard: [] });
        return;
    }
    
    if (text.match(/^id[a-z0-9_-]+$/i)) {
        await sendMiniAppMessage(chatId, APP_URL + '/' + text, '🔗 Присоединиться к комнате\n\nID: ' + text + '\n\nНажмите кнопку, чтобы начать звонок!');
        return;
    }
    
    await sendMessageWithKeyboard(chatId, 
        '❓ Неизвестная команда\n\nДоступные команды:\n/start - главное меню\n/join ID - подключиться к комнате\n/contacts - список контактов\n/help - помощь',
        { inline_keyboard: [] }
    );
}

async function pollTelegram() {
    try {
        const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/getUpdates?timeout=30&offset=${lastUpdateId + 1}`;
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.ok && data.result && data.result.length > 0) {
            for (const update of data.result) {
                lastUpdateId = update.update_id;
                if (update.message && update.message.text) {
                    const chatId = update.message.chat.id;
                    const text = update.message.text.trim();
                    const firstName = update.message.chat.first_name || 'Пользователь';
                    const userId = update.message.from?.id || '';
                    upsertTelegramUser(update.message.from || {});
                    console.log(`📩 ${new Date().toLocaleTimeString()} - ${firstName}: ${text}`);
                    await processMessage(chatId, text, firstName, userId);
                }
            }
        }
    } catch (err) {
        console.error('Polling error:', err.message);
    }
    setTimeout(pollTelegram, 1000);
}

// Запуск
pollTelegram();
console.log('✅ Telegram bot started');
