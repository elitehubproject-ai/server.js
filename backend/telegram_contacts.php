<?php
header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(200);
    exit();
}

require_once __DIR__ . '/telegram_users_store.php';
require_once __DIR__ . '/telegram_bot_config.php';
$method = $_SERVER['REQUEST_METHOD'] ?? 'GET';

function resolveTelegramContactProfile($username) {
    $normalized = tgNormalizeUsername($username);
    if ($normalized === '') {
        return ['ok' => false, 'reason' => 'invalid_format'];
    }
    $foundUser = tgFindUserByUsername($normalized);
    if ($foundUser) {
        $displayName = trim((string)($foundUser['name'] ?? ''));
        if ($displayName === '') {
            $displayName = ltrim($normalized, '@');
        }
        return [
            'ok' => true,
            'profile' => [
                'username' => $normalized,
                'name' => $displayName,
                'linkedTelegramId' => (string)($foundUser['id'] ?? ''),
                'avatar' => tgUsernameAvatar($normalized)
            ]
        ];
    }

    if (defined('TELEGRAM_BOT_TOKEN') && TELEGRAM_BOT_TOKEN !== '') {
        $url = 'https://api.telegram.org/bot' . TELEGRAM_BOT_TOKEN . '/getChat?chat_id=' . rawurlencode($normalized);
        $raw = false;
        if (function_exists('curl_init')) {
            $ch = curl_init($url);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_TIMEOUT, 8);
            $raw = curl_exec($ch);
            curl_close($ch);
        }
        if ($raw === false || $raw === null || $raw === '') {
            $raw = @file_get_contents($url);
        }
        if ($raw !== false && $raw !== null && $raw !== '') {
            $decoded = json_decode($raw, true);
            if (is_array($decoded) && !empty($decoded['ok']) && is_array($decoded['result'] ?? null)) {
                $result = $decoded['result'];
                $apiUsername = tgNormalizeUsername((string)($result['username'] ?? $normalized));
                if ($apiUsername === '') {
                    $apiUsername = $normalized;
                }
                $firstName = trim((string)($result['first_name'] ?? ''));
                $lastName = trim((string)($result['last_name'] ?? ''));
                $title = trim((string)($result['title'] ?? ''));
                $name = trim($firstName . ' ' . $lastName);
                if ($name === '') $name = $title;
                if ($name === '') $name = ltrim($apiUsername, '@');
                return [
                    'ok' => true,
                    'profile' => [
                        'username' => $apiUsername,
                        'name' => $name,
                        'linkedTelegramId' => (string)($result['id'] ?? ''),
                        'avatar' => tgUsernameAvatar($apiUsername)
                    ]
                ];
            }
        }
    }

    $fallbackName = ltrim($normalized, '@');
    return [
        'ok' => true,
        'profile' => [
            'username' => $normalized,
            'name' => $fallbackName !== '' ? $fallbackName : 'Telegram User',
            'linkedTelegramId' => '',
            'avatar' => tgUsernameAvatar($normalized)
        ]
    ];
}

if ($method === 'POST') {
    $input = json_decode(file_get_contents('php://input'), true);
    $action = trim((string)($input['action'] ?? ''));
    $telegramId = trim((string)($input['telegram_id'] ?? ''));
    if ($telegramId === '' || !preg_match('/^\d+$/', $telegramId)) {
        echo json_encode(['success' => false, 'error' => 'Invalid telegram_id'], JSON_UNESCAPED_UNICODE);
        exit();
    }
    if ($action === 'add') {
        $username = trim((string)($input['username'] ?? ''));
        $check = resolveTelegramContactProfile($username);
        if (!($check['ok'] ?? false)) {
            echo json_encode(['success' => false, 'error' => 'Пользователь не найден в Telegram по username'], JSON_UNESCAPED_UNICODE);
            exit();
        }
        $addResult = tgAddContactFor($telegramId, $check['profile']);
        if (!($addResult['success'] ?? false)) {
            echo json_encode(['success' => false, 'error' => (string)($addResult['error'] ?? 'Cannot save contact')], JSON_UNESCAPED_UNICODE);
            exit();
        }
        $contacts = tgGetContactsFor($telegramId);
        echo json_encode(['success' => true, 'data' => ['contacts' => $contacts]], JSON_UNESCAPED_UNICODE);
        exit();
    }
    if ($action === 'delete') {
        $contactId = trim((string)($input['contact_id'] ?? ''));
        $deleteResult = tgDeleteContactFor($telegramId, $contactId);
        if (!($deleteResult['success'] ?? false)) {
            echo json_encode(['success' => false, 'error' => (string)($deleteResult['error'] ?? 'Cannot delete contact')], JSON_UNESCAPED_UNICODE);
            exit();
        }
        $contacts = tgGetContactsFor($telegramId);
        echo json_encode(['success' => true, 'data' => ['contacts' => $contacts]], JSON_UNESCAPED_UNICODE);
        exit();
    }
    echo json_encode(['success' => false, 'error' => 'Unknown action'], JSON_UNESCAPED_UNICODE);
    exit();
}

$telegramId = trim((string)($_GET['telegram_id'] ?? ''));
if ($telegramId === '' || !preg_match('/^\d+$/', $telegramId)) {
    echo json_encode(['success' => false, 'error' => 'Invalid telegram_id'], JSON_UNESCAPED_UNICODE);
    exit();
}

$contacts = tgGetContactsFor($telegramId);
echo json_encode(['success' => true, 'data' => ['contacts' => $contacts]], JSON_UNESCAPED_UNICODE);
?>
