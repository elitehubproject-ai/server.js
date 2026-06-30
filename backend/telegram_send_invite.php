<?php
header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(200);
    exit();
}

require_once __DIR__ . '/telegram_bot_config.php';
require_once __DIR__ . '/telegram_users_store.php';

function out($success, $error = null, $data = null) {
    echo json_encode(['success' => $success, 'error' => $error, 'data' => $data], JSON_UNESCAPED_UNICODE);
    exit();
}

if (TELEGRAM_BOT_TOKEN === '') {
    out(false, 'TELEGRAM_BOT_TOKEN not configured');
}

$input = json_decode(file_get_contents('php://input'), true);
$target = trim((string)($input['target'] ?? ''));
$roomLink = trim((string)($input['roomLink'] ?? ''));
$roomId = trim((string)($input['roomId'] ?? ''));
$callerName = trim((string)($input['callerName'] ?? 'Пользователь'));
$callerUsername = trim((string)($input['callerUsername'] ?? ''));
$contactName = trim((string)($input['contactName'] ?? ''));

if ($target === '' || $roomLink === '' || $roomId === '') {
    out(false, 'Missing target, roomLink or roomId');
}

$targetResolved = $target;
if (strpos($targetResolved, '@') === 0) {
    $targetByUsername = tgFindUserIdByUsername($targetResolved);
    if ($targetByUsername !== '') {
        $targetResolved = $targetByUsername;
    }
}

$botRoomLink = 'https://t.me/' . TELEGRAM_BOT_USERNAME . '?startapp=' . urlencode($roomId);
$webRoomLink = rtrim(SEYCH_MINIAPP_URL, '/') . '/' . rawurlencode($roomId);

$title = 'Вам позвонили!';
$namePart = $contactName !== '' ? "\nКонтакт: {$contactName}" : '';
$usernamePart = $callerUsername !== '' ? "\nTelegram: @" . ltrim($callerUsername, '@') : '';
$text = "{$title}\nОт: {$callerName}{$usernamePart}{$namePart}\n\nЯ звоню тебе в Seych.\nЧтобы ответить, перейди по ссылкам:\n{$botRoomLink}\n{$webRoomLink}";

$inlineKeyboard = [
    'inline_keyboard' => [
        [
            ['text' => 'Ответить в Telegram', 'url' => $botRoomLink],
            ['text' => 'Ответить в браузере', 'url' => $webRoomLink]
        ],
        [
            ['text' => 'Сбросить', 'callback_data' => 'decline_' . $roomId]
        ]
    ]
];

$payload = [
    'chat_id' => $targetResolved,
    'text' => $text,
    'reply_markup' => json_encode($inlineKeyboard, JSON_UNESCAPED_UNICODE),
    'parse_mode' => 'HTML'
];

$url = 'https://api.telegram.org/bot' . TELEGRAM_BOT_TOKEN . '/sendMessage';
$ch = curl_init($url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_POST, true);
curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
curl_setopt($ch, CURLOPT_TIMEOUT, 15);
$result = curl_exec($ch);
$curlError = curl_error($ch);
curl_close($ch);

if ($result === false) {
    out(false, $curlError !== '' ? $curlError : 'Telegram API request failed');
}

$decoded = json_decode($result, true);
if (!is_array($decoded) || !($decoded['ok'] ?? false)) {
    $description = is_array($decoded) ? ($decoded['description'] ?? 'Telegram API error') : 'Telegram API invalid response';
    out(false, $description);
}

out(true, null, ['message_id' => $decoded['result']['message_id'] ?? null]);
?>
