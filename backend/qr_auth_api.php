<?php
header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

if (($_SERVER['REQUEST_METHOD'] ?? '') === 'OPTIONS') {
    http_response_code(200);
    exit();
}

const QR_STORE_PATH = __DIR__ . '/qr_auth_store.json';
const QR_TTL_SEC = 300;
const DEVICE_SESSION_MAX = 40;

function qrResponse($success, $data = null, $error = null) {
    echo json_encode([
        'success' => $success,
        'data' => $data,
        'error' => $error
    ], JSON_UNESCAPED_UNICODE);
    exit();
}

function qrLoadStore() {
    $default = ['qr_pending' => [], 'device_sessions' => []];
    if (!file_exists(QR_STORE_PATH)) {
        return $default;
    }
    $raw = @file_get_contents(QR_STORE_PATH);
    if ($raw === false || trim($raw) === '') {
        return $default;
    }
    $parsed = json_decode($raw, true);
    if (!is_array($parsed)) {
        return $default;
    }
    if (!isset($parsed['qr_pending']) || !is_array($parsed['qr_pending'])) {
        $parsed['qr_pending'] = [];
    }
    if (!isset($parsed['device_sessions']) || !is_array($parsed['device_sessions'])) {
        $parsed['device_sessions'] = [];
    }
    return $parsed;
}

function qrSaveStore($store) {
    $encoded = json_encode($store, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
    if ($encoded === false) {
        return false;
    }
    return @file_put_contents(QR_STORE_PATH, $encoded, LOCK_EX) !== false;
}

function qrNow() {
    return time();
}

function qrNormalizeId($value) {
    return trim((string)$value);
}

function qrRandomToken($bytes = 24) {
    try {
        return bin2hex(random_bytes($bytes));
    } catch (Exception $e) {
        return sha1(uniqid('', true) . mt_rand());
    }
}

function qrClientIp() {
    $xff = $_SERVER['HTTP_X_FORWARDED_FOR'] ?? '';
    if ($xff !== '') {
        $parts = explode(',', $xff);
        $ip = trim($parts[0]);
        if ($ip !== '') return $ip;
    }
    return trim((string)($_SERVER['REMOTE_ADDR'] ?? ''));
}

function qrClientUserAgent() {
    return trim((string)($_SERVER['HTTP_USER_AGENT'] ?? ''));
}

function qrParseDeviceName($ua) {
    $ua = (string)$ua;
    if ($ua === '') return 'Браузер';
    if (preg_match('/iPhone/i', $ua)) return 'iPhone';
    if (preg_match('/iPad/i', $ua) || (preg_match('/Macintosh/i', $ua) && preg_match('/Mobile/i', $ua))) return 'iPad';
    if (preg_match('/Android/i', $ua)) {
        if (preg_match('/Mobile/i', $ua)) return 'Android';
        return 'Android-планшет';
    }
    if (preg_match('/Windows/i', $ua)) return 'Windows';
    if (preg_match('/Macintosh|Mac OS X/i', $ua)) return 'Mac';
    if (preg_match('/Linux/i', $ua)) return 'Linux';
    return 'Браузер';
}

function qrResolveLocation($ip) {
    $ip = trim((string)$ip);
    if ($ip === '' || $ip === '127.0.0.1' || $ip === '::1') {
        return 'Локальная сеть';
    }
    if (preg_match('/^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[0-1])\.)/', $ip)) {
        return 'Локальная сеть';
    }
    $ctx = stream_context_create(['http' => ['timeout' => 2.5, 'header' => "User-Agent: SeychMessenger\r\n"]]);
    $url = 'http://ip-api.com/json/' . rawurlencode($ip) . '?fields=status,country,city,regionName&lang=ru';
    $raw = @file_get_contents($url, false, $ctx);
    if ($raw === false) {
        return 'Не определено';
    }
    $json = json_decode($raw, true);
    if (!is_array($json) || ($json['status'] ?? '') !== 'success') {
        return 'Не определено';
    }
    $city = trim((string)($json['city'] ?? ''));
    $region = trim((string)($json['regionName'] ?? ''));
    $country = trim((string)($json['country'] ?? ''));
    $parts = array_values(array_filter([$city, $region, $country]));
    return $parts ? implode(', ', $parts) : 'Не определено';
}

function qrCleanupStore(&$store) {
    $now = qrNow();
    foreach ($store['qr_pending'] as $token => $row) {
        $expires = (int)($row['expiresAt'] ?? 0);
        $status = (string)($row['status'] ?? 'pending');
        if ($expires > 0 && $expires < $now && $status === 'pending') {
            $store['qr_pending'][$token]['status'] = 'expired';
        }
        if ($expires > 0 && ($now - $expires) > 600) {
            unset($store['qr_pending'][$token]);
        }
    }
    foreach ($store['device_sessions'] as $sid => $row) {
        if (!empty($row['revoked'])) {
            if (($now - (int)($row['revokedAt'] ?? $now)) > 86400) {
                unset($store['device_sessions'][$sid]);
            }
            continue;
        }
        $last = (int)($row['lastSeenAt'] ?? $row['createdAt'] ?? 0);
        if ($last > 0 && ($now - $last) > 86400 * 90) {
            unset($store['device_sessions'][$sid]);
        }
    }
}

function qrRequireUserId($body) {
    $id = qrNormalizeId($body['app_user_id'] ?? '');
    if ($id === '') {
        qrResponse(false, null, 'app_user_id required');
    }
    return $id;
}

function qrFormatSessionRow($sid, $row, $currentSid = '') {
    return [
        'deviceSessionId' => $sid,
        'deviceName' => (string)($row['deviceName'] ?? 'Устройство'),
        'location' => (string)($row['location'] ?? 'Не определено'),
        'createdAt' => (int)($row['createdAt'] ?? 0),
        'lastSeenAt' => (int)($row['lastSeenAt'] ?? $row['createdAt'] ?? 0),
        'isCurrent' => $currentSid !== '' && $currentSid === $sid
    ];
}

function qrCreateDeviceSession(&$store, $appUserId, $deviceName, $location, $ua, $isMobile) {
    $sid = 'ds_' . qrRandomToken(12);
    $now = qrNow();
    $store['device_sessions'][$sid] = [
        'appUserId' => $appUserId,
        'deviceName' => $deviceName,
        'location' => $location,
        'userAgent' => $ua,
        'isMobile' => !!$isMobile,
        'createdAt' => $now,
        'lastSeenAt' => $now,
        'revoked' => false
    ];
    $userSessions = [];
    foreach ($store['device_sessions'] as $id => $row) {
        if (qrNormalizeId($row['appUserId'] ?? '') !== $appUserId) continue;
        if (!empty($row['revoked'])) continue;
        $userSessions[$id] = (int)($row['lastSeenAt'] ?? 0);
    }
    if (count($userSessions) > DEVICE_SESSION_MAX) {
        asort($userSessions);
        $drop = count($userSessions) - DEVICE_SESSION_MAX;
        foreach (array_keys($userSessions) as $id) {
            if ($drop <= 0) break;
            if ($id === $sid) continue;
            $store['device_sessions'][$id]['revoked'] = true;
            $store['device_sessions'][$id]['revokedAt'] = $now;
            $drop--;
        }
    }
    return $sid;
}

$rawBody = file_get_contents('php://input');
$body = json_decode($rawBody ?: '{}', true);
if (!is_array($body)) {
    $body = [];
}
$action = qrNormalizeId($body['action'] ?? ($_GET['action'] ?? ''));

$store = qrLoadStore();
qrCleanupStore($store);

switch ($action) {
    case 'create_qr': {
        $ua = qrNormalizeId($body['user_agent'] ?? '') ?: qrClientUserAgent();
        $ip = qrNormalizeId($body['client_ip'] ?? '') ?: qrClientIp();
        $isMobile = !empty($body['is_mobile']);
        $deviceName = qrNormalizeId($body['device_name'] ?? '') ?: qrParseDeviceName($ua);
        $location = qrResolveLocation($ip);
        $token = qrRandomToken(18);
        $now = qrNow();
        $store['qr_pending'][$token] = [
            'status' => 'pending',
            'createdAt' => $now,
            'expiresAt' => $now + QR_TTL_SEC,
            'deviceName' => $deviceName,
            'location' => $location,
            'userAgent' => $ua,
            'isMobile' => $isMobile,
            'approvedBy' => '',
            'approvedProfile' => null,
            'desktopDeviceSessionId' => ''
        ];
        qrSaveStore($store);
        qrResponse(true, [
            'token' => $token,
            'expiresAt' => $now + QR_TTL_SEC,
            'deviceName' => $deviceName,
            'location' => $location
        ]);
    }

    case 'poll_qr': {
        $token = qrNormalizeId($body['token'] ?? '');
        if ($token === '' || !isset($store['qr_pending'][$token])) {
            qrResponse(true, ['status' => 'expired']);
        }
        $row = $store['qr_pending'][$token];
        $now = qrNow();
        if ((string)($row['status'] ?? '') === 'pending' && (int)($row['expiresAt'] ?? 0) < $now) {
            $store['qr_pending'][$token]['status'] = 'expired';
            qrSaveStore($store);
            qrResponse(true, ['status' => 'expired']);
        }
        $status = (string)($row['status'] ?? 'pending');
        if ($status !== 'approved') {
            qrResponse(true, ['status' => $status]);
        }
        $profile = is_array($row['approvedProfile'] ?? null) ? $row['approvedProfile'] : null;
        $deviceSessionId = (string)($row['desktopDeviceSessionId'] ?? '');
        qrResponse(true, [
            'status' => 'approved',
            'profile' => $profile,
            'deviceSessionId' => $deviceSessionId
        ]);
    }

    case 'qr_info': {
        $token = qrNormalizeId($body['token'] ?? '');
        if ($token === '' || !isset($store['qr_pending'][$token])) {
            qrResponse(false, null, 'QR-код не найден или устарел');
        }
        $row = $store['qr_pending'][$token];
        $now = qrNow();
        if ((string)($row['status'] ?? '') === 'pending' && (int)($row['expiresAt'] ?? 0) < $now) {
            $store['qr_pending'][$token]['status'] = 'expired';
            qrSaveStore($store);
            qrResponse(false, null, 'QR-код устарел');
        }
        if ((string)($row['status'] ?? '') !== 'pending') {
            qrResponse(false, null, 'QR-код уже использован');
        }
        qrResponse(true, [
            'deviceName' => (string)($row['deviceName'] ?? 'Компьютер'),
            'location' => (string)($row['location'] ?? 'Не определено'),
            'expiresAt' => (int)($row['expiresAt'] ?? 0)
        ]);
    }

    case 'approve_qr': {
        $appUserId = qrRequireUserId($body);
        $token = qrNormalizeId($body['token'] ?? '');
        if ($token === '' || !isset($store['qr_pending'][$token])) {
            qrResponse(false, null, 'QR-код не найден');
        }
        $row = $store['qr_pending'][$token];
        $now = qrNow();
        if ((string)($row['status'] ?? '') !== 'pending') {
            qrResponse(false, null, 'QR-код уже использован');
        }
        if ((int)($row['expiresAt'] ?? 0) < $now) {
            $store['qr_pending'][$token]['status'] = 'expired';
            qrSaveStore($store);
            qrResponse(false, null, 'QR-код устарел');
        }
        $profile = [
            'provider' => qrNormalizeId($body['provider'] ?? 'qr'),
            'name' => qrNormalizeId($body['name'] ?? ''),
            'avatar' => qrNormalizeId($body['avatar'] ?? ''),
            'appUserId' => $appUserId,
            'externalKey' => qrNormalizeId($body['external_key'] ?? ''),
            'telegramId' => qrNormalizeId($body['telegram_id'] ?? ''),
            'googleSub' => qrNormalizeId($body['google_sub'] ?? ''),
            'googleEmail' => qrNormalizeId($body['google_email'] ?? ''),
            'vkUserId' => qrNormalizeId($body['vk_user_id'] ?? ''),
            'vkUsername' => qrNormalizeId($body['vk_username'] ?? ''),
            'username' => qrNormalizeId($body['username'] ?? '')
        ];
        $desktopSid = qrCreateDeviceSession(
            $store,
            $appUserId,
            (string)($row['deviceName'] ?? 'Компьютер'),
            (string)($row['location'] ?? 'Не определено'),
            (string)($row['userAgent'] ?? ''),
            !empty($row['isMobile']) ? false : true
        );
        $store['qr_pending'][$token]['status'] = 'approved';
        $store['qr_pending'][$token]['approvedBy'] = $appUserId;
        $store['qr_pending'][$token]['approvedProfile'] = $profile;
        $store['qr_pending'][$token]['desktopDeviceSessionId'] = $desktopSid;
        $store['qr_pending'][$token]['approvedAt'] = $now;
        qrSaveStore($store);
        qrResponse(true, [
            'deviceSessionId' => $desktopSid,
            'desktopDeviceName' => (string)($row['deviceName'] ?? 'Компьютер')
        ]);
    }

    case 'register_device': {
        $appUserId = qrRequireUserId($body);
        $ua = qrNormalizeId($body['user_agent'] ?? '') ?: qrClientUserAgent();
        $ip = qrNormalizeId($body['client_ip'] ?? '') ?: qrClientIp();
        $isMobile = !empty($body['is_mobile']);
        $deviceName = qrNormalizeId($body['device_name'] ?? '') ?: qrParseDeviceName($ua);
        $location = qrResolveLocation($ip);
        $existingSid = qrNormalizeId($body['device_session_id'] ?? '');
        if ($existingSid !== '' && isset($store['device_sessions'][$existingSid])) {
            $row = $store['device_sessions'][$existingSid];
            if (qrNormalizeId($row['appUserId'] ?? '') === $appUserId && empty($row['revoked'])) {
                $store['device_sessions'][$existingSid]['lastSeenAt'] = qrNow();
                $store['device_sessions'][$existingSid]['deviceName'] = $deviceName;
                $store['device_sessions'][$existingSid]['location'] = $location;
                qrSaveStore($store);
                qrResponse(true, ['deviceSessionId' => $existingSid, 'location' => $location, 'deviceName' => $deviceName]);
            }
        }
        $sid = qrCreateDeviceSession($store, $appUserId, $deviceName, $location, $ua, $isMobile);
        qrSaveStore($store);
        qrResponse(true, ['deviceSessionId' => $sid, 'location' => $location, 'deviceName' => $deviceName]);
    }

    case 'touch_device': {
        $appUserId = qrRequireUserId($body);
        $sid = qrNormalizeId($body['device_session_id'] ?? '');
        if ($sid === '' || !isset($store['device_sessions'][$sid])) {
            qrResponse(false, null, 'session not found');
        }
        $row = $store['device_sessions'][$sid];
        if (qrNormalizeId($row['appUserId'] ?? '') !== $appUserId) {
            qrResponse(false, null, 'forbidden');
        }
        if (!empty($row['revoked'])) {
            qrResponse(true, ['revoked' => true]);
        }
        $store['device_sessions'][$sid]['lastSeenAt'] = qrNow();
        qrSaveStore($store);
        qrResponse(true, ['revoked' => false]);
    }

    case 'validate_session': {
        $appUserId = qrRequireUserId($body);
        $sid = qrNormalizeId($body['device_session_id'] ?? '');
        if ($sid === '' || !isset($store['device_sessions'][$sid])) {
            qrResponse(true, ['valid' => false]);
        }
        $row = $store['device_sessions'][$sid];
        $valid = qrNormalizeId($row['appUserId'] ?? '') === $appUserId && empty($row['revoked']);
        qrResponse(true, ['valid' => $valid]);
    }

    case 'list_devices': {
        $appUserId = qrRequireUserId($body);
        $currentSid = qrNormalizeId($body['device_session_id'] ?? '');
        $list = [];
        foreach ($store['device_sessions'] as $sid => $row) {
            if (qrNormalizeId($row['appUserId'] ?? '') !== $appUserId) continue;
            if (!empty($row['revoked'])) continue;
            $list[] = qrFormatSessionRow($sid, $row, $currentSid);
        }
        usort($list, function ($a, $b) {
            return (int)($b['lastSeenAt'] ?? 0) <=> (int)($a['lastSeenAt'] ?? 0);
        });
        qrResponse(true, ['devices' => $list]);
    }

    case 'revoke_device': {
        $appUserId = qrRequireUserId($body);
        $currentSid = qrNormalizeId($body['device_session_id'] ?? '');
        $sid = qrNormalizeId($body['revoke_session_id'] ?? $body['target_device_session_id'] ?? '');
        if ($sid === '') {
            qrResponse(false, null, 'session id required');
        }
        if ($currentSid !== '' && $sid === $currentSid) {
            qrResponse(false, null, 'cannot revoke current session');
        }
        if (!isset($store['device_sessions'][$sid])) {
            qrResponse(false, null, 'session not found');
        }
        $row = $store['device_sessions'][$sid];
        if (qrNormalizeId($row['appUserId'] ?? '') !== $appUserId) {
            qrResponse(false, null, 'forbidden');
        }
        $store['device_sessions'][$sid]['revoked'] = true;
        $store['device_sessions'][$sid]['revokedAt'] = qrNow();
        qrSaveStore($store);
        qrResponse(true, ['revoked' => true, 'deviceSessionId' => $sid]);
    }

    default:
        qrResponse(false, null, 'Unknown action');
}
