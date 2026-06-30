<?php
require_once __DIR__ . '/telegram_bot_config.php';
require_once __DIR__ . '/telegram_users_store.php';

function tgRequest($method, $data) {
    $url = 'https://api.telegram.org/bot' . TELEGRAM_BOT_TOKEN . '/' . $method;
    $ch = curl_init($url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
    curl_setopt($ch, CURLOPT_TIMEOUT, 15);
    $result = curl_exec($ch);
    curl_close($ch);
    return $result;
}

function sendMiniAppMessage($chatId, $url, $text) {
    $keyboard = [
        'inline_keyboard' => [
            [
                ['text' => 'Открыть Seych', 'web_app' => ['url' => $url]]
            ]
        ]
    ];

    tgRequest('sendMessage', [
        'chat_id' => $chatId,
        'text' => $text,
        'reply_markup' => json_encode($keyboard, JSON_UNESCAPED_UNICODE)
    ]);
}

function sendMainMenu($chatId, $baseUrl) {
    $keyboard = [
        'inline_keyboard' => [
            [
                ['text' => 'Создать комнату', 'web_app' => ['url' => $baseUrl . '/']]
            ],
            [
                ['text' => 'Подключиться', 'web_app' => ['url' => $baseUrl . '/']]
            ],
            [
                ['text' => 'Позвонить контактам', 'web_app' => ['url' => $baseUrl . '/']]
            ]
        ]
    ];
    tgRequest('sendMessage', [
        'chat_id' => $chatId,
        'text' => "Seych\nВыберите действие:",
        'reply_markup' => json_encode($keyboard, JSON_UNESCAPED_UNICODE)
    ]);
}

function generateRoomId() {
    return 'id' . substr(bin2hex(random_bytes(6)), 0, 10);
}

function buildBotRoomLink($roomId) {
    return 'https://t.me/' . TELEGRAM_BOT_USERNAME . '?startapp=' . urlencode($roomId);
}

if (TELEGRAM_BOT_TOKEN === '') {
    http_response_code(500);
    exit();
}

$update = json_decode(file_get_contents('php://input'), true);
if (!is_array($update)) {
    echo 'ok';
    exit();
}

if (isset($update['callback_query'])) {
    tgUpsertUser($update['callback_query']['from'] ?? []);
    $callbackId = $update['callback_query']['id'] ?? '';
    $data = $update['callback_query']['data'] ?? '';
    $message = $update['callback_query']['message'] ?? [];
    $chatId = $message['chat']['id'] ?? null;
    $messageId = $message['message_id'] ?? null;

    if ($callbackId !== '') {
        tgRequest('answerCallbackQuery', [
            'callback_query_id' => $callbackId,
            'text' => 'Звонок сброшен',
            'show_alert' => false
        ]);
    }

    if (strpos($data, 'decline_') === 0 && $chatId !== null && $messageId !== null) {
        tgRequest('editMessageReplyMarkup', [
            'chat_id' => $chatId,
            'message_id' => $messageId,
            'reply_markup' => json_encode(['inline_keyboard' => []], JSON_UNESCAPED_UNICODE)
        ]);
    }

    echo 'ok';
    exit();
}

if (!isset($update['message'])) {
    echo 'ok';
    exit();
}

$message = $update['message'];
$chatId = $message['chat']['id'] ?? null;
$text = trim((string)($message['text'] ?? ''));
tgUpsertUser($message['from'] ?? []);

if ($chatId === null) {
    echo 'ok';
    exit();
}

$baseUrl = rtrim(SEYCH_MINIAPP_URL, '/');

if (strpos($text, '/start') === 0) {
    $parts = preg_split('/\s+/', $text);
    $payloadRoom = $parts[1] ?? '';
    if (preg_match('/^id[a-z0-9_-]+$/i', $payloadRoom)) {
        $roomLink = $baseUrl . '/' . $payloadRoom;
        sendMiniAppMessage($chatId, $roomLink, "Вход в комнату {$payloadRoom}");
        tgRequest('sendMessage', [
            'chat_id' => $chatId,
            'text' => "Прямая ссылка через бота:\n" . buildBotRoomLink($payloadRoom)
        ]);
    } else {
        sendMainMenu($chatId, $baseUrl);
    }
    echo 'ok';
    exit();
}

if ($text === '/create') {
    $roomId = generateRoomId();
    $roomLink = $baseUrl . '/' . $roomId;
    $botRoomLink = buildBotRoomLink($roomId);
    sendMiniAppMessage($chatId, $roomLink, "Комната создана: {$roomId}\nСсылка: {$roomLink}\nBot-link: {$botRoomLink}");
    echo 'ok';
    exit();
}

if (strpos($text, '/join') === 0) {
    $parts = preg_split('/\s+/', $text);
    $roomId = $parts[1] ?? '';
    if (preg_match('/^id[a-z0-9_-]+$/i', $roomId)) {
        sendMiniAppMessage($chatId, $baseUrl . '/' . $roomId, 'Подключение к комнате ' . $roomId);
    } else {
        tgRequest('sendMessage', [
            'chat_id' => $chatId,
            'text' => 'Используйте: /join id1234'
        ]);
    }
    echo 'ok';
    exit();
}

if ($text === '/contacts') {
    $me = isset($message['from']['id']) ? (string)$message['from']['id'] : '';
    $contacts = tgGetContactsFor($me);
    if (!$contacts) {
        tgRequest('sendMessage', [
            'chat_id' => $chatId,
            'text' => 'Контактов пока нет. Попросите друзей написать боту /start.'
        ]);
    } else {
        $rows = array_map(function ($contact) {
            $line = $contact['name'];
            if (!empty($contact['username'])) {
                $line .= ' (' . $contact['username'] . ')';
            }
            return '• ' . $line;
        }, $contacts);
        tgRequest('sendMessage', [
            'chat_id' => $chatId,
            'text' => "Доступные контакты:\n" . implode("\n", $rows)
        ]);
    }
    echo 'ok';
    exit();
}

if (mb_strtolower($text) === 'создать комнату') {
    $roomId = generateRoomId();
    $roomLink = $baseUrl . '/' . $roomId;
    sendMiniAppMessage($chatId, $roomLink, "Комната создана: {$roomId}\nСсылка: {$roomLink}");
    echo 'ok';
    exit();
}

tgRequest('sendMessage', [
    'chat_id' => $chatId,
    'text' => "Команды:\n/start\n/create\n/join id1234\n/contacts"
]);

echo 'ok';
?>
