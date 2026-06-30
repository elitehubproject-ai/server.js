<?php
header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

if (($_SERVER['REQUEST_METHOD'] ?? '') === 'OPTIONS') {
    http_response_code(200);
    exit();
}

const FRIENDS_STORE_PATH = __DIR__ . '/friends_store.json';

function friendsResponse($success, $data = null, $error = null) {
    echo json_encode([
        'success' => $success,
        'data' => $data,
        'error' => $error
    ], JSON_UNESCAPED_UNICODE);
    exit();
}

function friendsLoadStore() {
    $default = [
        'users' => [],
        'requests' => [],
        'friends' => [],
        'calls' => [],
        'push_subscriptions' => [],
        'push_vapid' => [],
        'account_links' => []
    ];
    if (!file_exists(FRIENDS_STORE_PATH)) {
        return $default;
    }
    $raw = @file_get_contents(FRIENDS_STORE_PATH);
    if ($raw === false || trim($raw) === '') {
        return $default;
    }
    $parsed = json_decode($raw, true);
    if (!is_array($parsed)) {
        return $default;
    }
    foreach ($default as $key => $value) {
        if (!isset($parsed[$key]) || !is_array($parsed[$key])) {
            $parsed[$key] = $value;
        }
    }
    return $parsed;
}

function friendsSaveStore($store) {
    $dir = dirname(FRIENDS_STORE_PATH);
    if (!is_dir($dir)) {
        @mkdir($dir, 0777, true);
    }
    $encoded = json_encode($store, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
    if ($encoded === false) {
        return false;
    }
    return @file_put_contents(FRIENDS_STORE_PATH, $encoded, LOCK_EX) !== false;
}

function nowTs() {
    return time();
}

function normalizeId($value) {
    return trim((string)$value);
}

function normalizeText($value) {
    return trim((string)$value);
}

function normalizeUsernameValue($value) {
    $raw = mb_strtolower(ltrim(normalizeText($value), '@'));
    $raw = preg_replace('/[^a-z0-9]/', '', $raw);
    $raw = (string)$raw;
    return strlen($raw) > 50 ? substr($raw, 0, 50) : $raw;
}

function buildGeneratedUsername($userId) {
    $clean = preg_replace('/[^a-z0-9]/', '', mb_strtolower(normalizeId($userId)));
    $clean = (string)$clean;
    $suffix = str_pad(substr($clean, -8), 8, '0', STR_PAD_LEFT);
    return substr('user' . $suffix, 0, 50);
}

function normalizeExternalKey($value) {
    $key = mb_strtolower(normalizeText($value));
    if ($key === '') return '';
    return strlen($key) > 180 ? substr($key, 0, 180) : $key;
}

function normalizeIdentityKeys($rawKeys, $externalKey = '') {
    $keys = [];
    if (is_array($rawKeys)) {
        foreach ($rawKeys as $value) {
            $normalized = normalizeExternalKey($value);
            if ($normalized === '') continue;
            $keys[$normalized] = true;
        }
    }
    $external = normalizeExternalKey($externalKey);
    if ($external !== '') {
        $keys[$external] = true;
    }
    return array_keys($keys);
}

function hashIdentityPart($value, $seed = 5381) {
    $hash = ((int)$seed) & 0xffffffff;
    $input = (string)$value;
    $len = strlen($input);
    for ($i = 0; $i < $len; $i++) {
        $code = ord($input[$i]);
        $hash = (((($hash << 5) & 0xffffffff) + $hash) & 0xffffffff) ^ $code;
        $hash &= 0xffffffff;
    }
    $hex = dechex($hash & 0xffffffff);
    return str_pad($hex, 8, '0', STR_PAD_LEFT);
}

function buildCanonicalUserIdFromIdentityKey($identityKey) {
    $key = normalizeExternalKey($identityKey);
    if ($key === '') return '';
    $h1 = hashIdentityPart($key, 5381);
    $h2 = hashIdentityPart('seych:' . $key, 2166136261);
    return 'u' . $h1 . $h2;
}

function pickPrimaryIdentityKey($identityKeys) {
    if (!is_array($identityKeys) || !count($identityKeys)) return '';
    foreach ($identityKeys as $key) {
        $normalized = normalizeExternalKey($key);
        if ($normalized !== '') return $normalized;
    }
    return '';
}

function resolveMappedUserIdByIdentity($store, $identityKeys) {
    if (!is_array($identityKeys) || !count($identityKeys)) return '';
    $links = is_array($store['account_links'] ?? null) ? $store['account_links'] : [];
    foreach ($identityKeys as $identityKey) {
        $key = normalizeExternalKey($identityKey);
        if ($key === '') continue;
        $mappedId = normalizeId($links[$key] ?? '');
        if ($mappedId === '') continue;
        if (findUserIndexById($store, $mappedId) >= 0) {
            return $mappedId;
        }
    }
    return '';
}

function bindIdentityKeysToUser(&$store, $userId, $identityKeys) {
    $id = normalizeId($userId);
    if ($id === '') return false;
    if (!is_array($identityKeys) || !count($identityKeys)) return false;
    if (!isset($store['account_links']) || !is_array($store['account_links'])) {
        $store['account_links'] = [];
    }
    $changed = false;
    foreach ($identityKeys as $identityKey) {
        $key = normalizeExternalKey($identityKey);
        if ($key === '') continue;
        if (normalizeId($store['account_links'][$key] ?? '') === $id) continue;
        $store['account_links'][$key] = $id;
        $changed = true;
    }
    return $changed;
}

function normalizeBoolFlag($value, $default = false) {
    if (is_bool($value)) return $value;
    if (is_int($value) || is_float($value)) return ((int)$value) !== 0;
    $text = mb_strtolower(normalizeText($value));
    if ($text === '') return (bool)$default;
    if (in_array($text, ['1', 'true', 'yes', 'on'], true)) return true;
    if (in_array($text, ['0', 'false', 'no', 'off'], true)) return false;
    return (bool)$default;
}

function b64urlEncode($binary) {
    return rtrim(strtr(base64_encode((string)$binary), '+/', '-_'), '=');
}

function b64urlDecode($value) {
    $safe = strtr((string)$value, '-_', '+/');
    $padding = strlen($safe) % 4;
    if ($padding > 0) {
        $safe .= str_repeat('=', 4 - $padding);
    }
    $decoded = base64_decode($safe, true);
    return $decoded === false ? '' : $decoded;
}

function isValidVapidPublicKey($encoded) {
    $raw = b64urlDecode($encoded);
    return is_string($raw) && strlen($raw) === 65 && ord($raw[0]) === 0x04;
}

function friendsBuildRawEcPublicKey($details) {
    if (!is_array($details)) return '';
    $ec = is_array($details['ec'] ?? null) ? $details['ec'] : [];
    $x = $ec['x'] ?? '';
    $y = $ec['y'] ?? '';
    if (is_string($x) && is_string($y) && strlen($x) === 32 && strlen($y) === 32) {
        return "\x04" . $x . $y;
    }
    $public = $ec['public_key'] ?? '';
    if (is_string($public) && strlen($public) === 65 && ord($public[0]) === 0x04) {
        return $public;
    }
    return '';
}

function friendsAsn1ReadLength($data, &$offset) {
    if ($offset >= strlen($data)) return -1;
    $length = ord($data[$offset++]);
    if (($length & 0x80) === 0) {
        return $length;
    }
    $bytesCount = $length & 0x7f;
    if ($bytesCount < 1 || $bytesCount > 4 || ($offset + $bytesCount) > strlen($data)) {
        return -1;
    }
    $length = 0;
    for ($i = 0; $i < $bytesCount; $i++) {
        $length = ($length << 8) | ord($data[$offset++]);
    }
    return $length;
}

function friendsDerToJoseSignature($der, $partLength = 32) {
    if (!is_string($der) || $der === '') return '';
    $offset = 0;
    if (ord($der[$offset++]) !== 0x30) return '';
    $seqLen = friendsAsn1ReadLength($der, $offset);
    if ($seqLen < 0 || ($offset + $seqLen) > strlen($der)) return '';
    if (ord($der[$offset++]) !== 0x02) return '';
    $rLen = friendsAsn1ReadLength($der, $offset);
    if ($rLen < 1 || ($offset + $rLen) > strlen($der)) return '';
    $r = substr($der, $offset, $rLen);
    $offset += $rLen;
    if (ord($der[$offset++]) !== 0x02) return '';
    $sLen = friendsAsn1ReadLength($der, $offset);
    if ($sLen < 1 || ($offset + $sLen) > strlen($der)) return '';
    $s = substr($der, $offset, $sLen);
    $r = ltrim($r, "\x00");
    $s = ltrim($s, "\x00");
    $r = str_pad(substr($r, -$partLength), $partLength, "\x00", STR_PAD_LEFT);
    $s = str_pad(substr($s, -$partLength), $partLength, "\x00", STR_PAD_LEFT);
    return $r . $s;
}

function friendsGetVapidKeys(&$store) {
    $existingPublic = normalizeText($store['push_vapid']['public_key'] ?? '');
    $existingPrivate = normalizeText($store['push_vapid']['private_pem'] ?? '');
    if ($existingPublic !== '' && $existingPrivate !== '' && isValidVapidPublicKey($existingPublic)) {
        return ['public_key' => $existingPublic, 'private_pem' => $existingPrivate];
    }
    $resource = openssl_pkey_new([
        'private_key_type' => OPENSSL_KEYTYPE_EC,
        'curve_name' => 'prime256v1'
    ]);
    if (!$resource) {
        return ['public_key' => '', 'private_pem' => ''];
    }
    $privatePem = '';
    if (!openssl_pkey_export($resource, $privatePem)) {
        return ['public_key' => '', 'private_pem' => ''];
    }
    $details = openssl_pkey_get_details($resource);
    $rawPublic = friendsBuildRawEcPublicKey($details);
    if ($rawPublic === '') {
        return ['public_key' => '', 'private_pem' => ''];
    }
    $keys = [
        'public_key' => b64urlEncode($rawPublic),
        'private_pem' => $privatePem
    ];
    $store['push_vapid'] = $keys;
    return $keys;
}

function friendsSanitizeSubscription($subscription) {
    if (!is_array($subscription)) return null;
    $endpoint = normalizeText($subscription['endpoint'] ?? '');
    if ($endpoint === '' || stripos($endpoint, 'https://') !== 0) return null;
    $keys = is_array($subscription['keys'] ?? null) ? $subscription['keys'] : [];
    $auth = normalizeText($keys['auth'] ?? '');
    $p256dh = normalizeText($keys['p256dh'] ?? '');
    $contentEncoding = normalizeText($subscription['contentEncoding'] ?? '');
    return [
        'endpoint' => $endpoint,
        'keys' => [
            'auth' => $auth,
            'p256dh' => $p256dh
        ],
        'contentEncoding' => $contentEncoding
    ];
}

function friendsStorePushSubscription(&$store, $appUserId, $subscription) {
    $normalized = friendsSanitizeSubscription($subscription);
    if (!$normalized) return false;
    $userKey = normalizeId($appUserId);
    if ($userKey === '') return false;
    if (!isset($store['push_subscriptions'][$userKey]) || !is_array($store['push_subscriptions'][$userKey])) {
        $store['push_subscriptions'][$userKey] = [];
    }
    $saved = false;
    foreach ($store['push_subscriptions'][$userKey] as $index => $row) {
        $existingEndpoint = normalizeText($row['endpoint'] ?? '');
        if ($existingEndpoint !== $normalized['endpoint']) continue;
        $store['push_subscriptions'][$userKey][$index]['keys'] = $normalized['keys'];
        $store['push_subscriptions'][$userKey][$index]['contentEncoding'] = $normalized['contentEncoding'];
        $store['push_subscriptions'][$userKey][$index]['updated_at'] = nowTs();
        $saved = true;
        break;
    }
    if (!$saved) {
        $store['push_subscriptions'][$userKey][] = [
            'endpoint' => $normalized['endpoint'],
            'keys' => $normalized['keys'],
            'contentEncoding' => $normalized['contentEncoding'],
            'created_at' => nowTs(),
            'updated_at' => nowTs()
        ];
    }
    return true;
}

function friendsSendWebPush($endpoint, $vapidPublic, $vapidPrivatePem) {
    if (!function_exists('curl_init')) return ['ok' => false, 'status' => 0];
    $parts = parse_url($endpoint);
    $scheme = strtolower((string)($parts['scheme'] ?? ''));
    $host = normalizeText($parts['host'] ?? '');
    if (($scheme !== 'https' && $scheme !== 'http') || $host === '') {
        return ['ok' => false, 'status' => 0];
    }
    $aud = $scheme . '://' . $host;
    $header = ['typ' => 'JWT', 'alg' => 'ES256'];
    $payload = [
        'aud' => $aud,
        'exp' => nowTs() + 12 * 60 * 60,
        'sub' => 'mailto:notify@seych-call.local'
    ];
    $tokenPayload = b64urlEncode(json_encode($header, JSON_UNESCAPED_UNICODE)) . '.' . b64urlEncode(json_encode($payload, JSON_UNESCAPED_UNICODE));
    $signatureDer = '';
    $signed = openssl_sign($tokenPayload, $signatureDer, $vapidPrivatePem, OPENSSL_ALGO_SHA256);
    if (!$signed) {
        return ['ok' => false, 'status' => 0];
    }
    $signatureJose = friendsDerToJoseSignature($signatureDer, 32);
    if ($signatureJose === '') {
        return ['ok' => false, 'status' => 0];
    }
    $jwt = $tokenPayload . '.' . b64urlEncode($signatureJose);
    $send = function ($headers) use ($endpoint) {
        $ch = curl_init($endpoint);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_POSTFIELDS, '');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HEADER, false);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 5);
        curl_setopt($ch, CURLOPT_TIMEOUT, 8);
        curl_exec($ch);
        $status = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);
        return $status;
    };
    $headers = [
        'TTL: 60',
        'Urgency: high',
        'Authorization: vapid t=' . $jwt . ', k=' . $vapidPublic,
        'Crypto-Key: p256ecdsa=' . $vapidPublic,
        'Content-Length: 0'
    ];
    $status = $send($headers);
    if ($status === 400 || $status === 401 || $status === 403) {
        $status = $send([
            'TTL: 60',
            'Urgency: high',
            'Authorization: WebPush ' . $jwt,
            'Crypto-Key: p256ecdsa=' . $vapidPublic,
            'Content-Length: 0'
        ]);
    }
    $ok = $status >= 200 && $status < 300;
    return ['ok' => $ok, 'status' => $status];
}

function friendsNotifyIncomingCall(&$store, $targetId) {
    $targetKey = normalizeId($targetId);
    if ($targetKey === '') return;
    if (isUserActiveOnSite($store, $targetKey)) return;
    $subscriptions = $store['push_subscriptions'][$targetKey] ?? [];
    if (!is_array($subscriptions) || !$subscriptions) return;
    $keys = friendsGetVapidKeys($store);
    $publicKey = normalizeText($keys['public_key'] ?? '');
    $privatePem = normalizeText($keys['private_pem'] ?? '');
    if ($publicKey === '' || $privatePem === '') return;
    $aliveSubscriptions = [];
    foreach ($subscriptions as $subscription) {
        $endpoint = normalizeText($subscription['endpoint'] ?? '');
        if ($endpoint === '') continue;
        $result = friendsSendWebPush($endpoint, $publicKey, $privatePem);
        $status = (int)($result['status'] ?? 0);
        $isGone = $status === 404 || $status === 410;
        if (!$isGone) {
            $aliveSubscriptions[] = $subscription;
        }
    }
    $store['push_subscriptions'][$targetKey] = $aliveSubscriptions;
}

function pairKey($first, $second) {
    $a = normalizeId($first);
    $b = normalizeId($second);
    if ($a === '' || $b === '') return '';
    return strcmp($a, $b) <= 0 ? ($a . '::' . $b) : ($b . '::' . $a);
}

function findUser($store, $userId) {
    $id = normalizeId($userId);
    if ($id === '') return null;
    foreach ($store['users'] as $user) {
        if (normalizeId($user['id'] ?? '') === $id) {
            return $user;
        }
    }
    return null;
}

function touchUserPresence(&$store, $userId, $isActiveTab) {
    $id = normalizeId($userId);
    if ($id === '') return false;
    $activeTab = !!$isActiveTab;
    $updatedAt = nowTs();
    foreach ($store['users'] as $index => $user) {
        if (normalizeId($user['id'] ?? '') !== $id) continue;
        $prevActive = normalizeBoolFlag($store['users'][$index]['active_tab'] ?? false, false);
        $prevTs = (int)($store['users'][$index]['presence_updated_at'] ?? 0);
        $store['users'][$index]['active_tab'] = $activeTab;
        $store['users'][$index]['presence_updated_at'] = $updatedAt;
        return $prevActive !== $activeTab || $prevTs !== $updatedAt;
    }
    return false;
}

function isUserActiveOnSite($store, $userId) {
    $id = normalizeId($userId);
    if ($id === '') return false;
    $now = nowTs();
    foreach ($store['users'] as $user) {
        if (normalizeId($user['id'] ?? '') !== $id) continue;
        $activeTab = normalizeBoolFlag($user['active_tab'] ?? false, false);
        if (!$activeTab) return false;
        $presenceTs = (int)($user['presence_updated_at'] ?? 0);
        if ($presenceTs <= 0) return false;
        return ($now - $presenceTs) <= 12;
    }
    return false;
}

function findUserIndexById($store, $userId) {
    $id = normalizeId($userId);
    if ($id === '') return -1;
    foreach ($store['users'] as $index => $user) {
        if (normalizeId($user['id'] ?? '') === $id) {
            return (int)$index;
        }
    }
    return -1;
}

function findUserIndexByExternalKey($store, $externalKey) {
    $key = normalizeExternalKey($externalKey);
    if ($key === '') return -1;
    foreach ($store['users'] as $index => $user) {
        if (normalizeExternalKey($user['external_key'] ?? '') === $key) {
            return (int)$index;
        }
    }
    return -1;
}

function mergePushSubscriptions($targetList, $sourceList) {
    $target = is_array($targetList) ? $targetList : [];
    $source = is_array($sourceList) ? $sourceList : [];
    $byEndpoint = [];
    foreach ($target as $row) {
        $endpoint = normalizeText($row['endpoint'] ?? '');
        if ($endpoint === '') continue;
        $byEndpoint[$endpoint] = $row;
    }
    foreach ($source as $row) {
        $endpoint = normalizeText($row['endpoint'] ?? '');
        if ($endpoint === '') continue;
        if (!isset($byEndpoint[$endpoint])) {
            $byEndpoint[$endpoint] = $row;
            continue;
        }
        $existing = $byEndpoint[$endpoint];
        if (normalizeText($existing['updated_at'] ?? '') === '' && normalizeText($row['updated_at'] ?? '') !== '') {
            $existing['updated_at'] = $row['updated_at'];
        }
        if (!is_array($existing['keys'] ?? null) && is_array($row['keys'] ?? null)) {
            $existing['keys'] = $row['keys'];
        }
        if (normalizeText($existing['contentEncoding'] ?? '') === '' && normalizeText($row['contentEncoding'] ?? '') !== '') {
            $existing['contentEncoding'] = $row['contentEncoding'];
        }
        $byEndpoint[$endpoint] = $existing;
    }
    return array_values($byEndpoint);
}

function remapUserIdInStore(&$store, $fromUserId, $toUserId) {
    $from = normalizeId($fromUserId);
    $to = normalizeId($toUserId);
    if ($from === '' || $to === '' || $from === $to) return;

    $fromIndex = findUserIndexById($store, $from);
    if ($fromIndex < 0) return;
    $toIndex = findUserIndexById($store, $to);

    if ($toIndex < 0) {
        $store['users'][$fromIndex]['id'] = $to;
    } else {
        $fromUser = $store['users'][$fromIndex];
        $toUser = $store['users'][$toIndex];
        $toUser['name'] = normalizeText($toUser['name'] ?? '') !== '' ? normalizeText($toUser['name'] ?? '') : normalizeText($fromUser['name'] ?? '');
        $toUser['avatar'] = normalizeText($toUser['avatar'] ?? '') !== '' ? normalizeText($toUser['avatar'] ?? '') : normalizeText($fromUser['avatar'] ?? '');
        $toUser['last_seen'] = max((int)($toUser['last_seen'] ?? 0), (int)($fromUser['last_seen'] ?? 0));
        $toUserExternal = normalizeExternalKey($toUser['external_key'] ?? '');
        $fromUserExternal = normalizeExternalKey($fromUser['external_key'] ?? '');
        if ($toUserExternal === '' && $fromUserExternal !== '') {
            $toUser['external_key'] = $fromUserExternal;
        }
        $store['users'][$toIndex] = $toUser;
        unset($store['users'][$fromIndex]);
        $store['users'] = array_values($store['users']);
    }

    foreach ($store['friends'] as $index => $friendship) {
        $a = normalizeId($friendship['a'] ?? '');
        $b = normalizeId($friendship['b'] ?? '');
        if ($a === $from) $a = $to;
        if ($b === $from) $b = $to;
        if ($a === '' || $b === '' || $a === $b) {
            unset($store['friends'][$index]);
            continue;
        }
        $store['friends'][$index]['a'] = $a;
        $store['friends'][$index]['b'] = $b;
        $store['friends'][$index]['key'] = pairKey($a, $b);
    }
    $dedupFriends = [];
    foreach (array_values($store['friends']) as $friendship) {
        $key = normalizeText($friendship['key'] ?? '');
        if ($key === '' || isset($dedupFriends[$key])) continue;
        $dedupFriends[$key] = $friendship;
    }
    $store['friends'] = array_values($dedupFriends);

    foreach ($store['requests'] as $index => $request) {
        $fromId = normalizeId($request['from'] ?? '');
        $toId = normalizeId($request['to'] ?? '');
        if ($fromId === $from) $fromId = $to;
        if ($toId === $from) $toId = $to;
        if ($fromId === '' || $toId === '' || $fromId === $toId) {
            unset($store['requests'][$index]);
            continue;
        }
        $store['requests'][$index]['from'] = $fromId;
        $store['requests'][$index]['to'] = $toId;
    }
    $store['requests'] = array_values($store['requests']);

    foreach ($store['calls'] as $index => $call) {
        $fromId = normalizeId($call['from'] ?? '');
        $toId = normalizeId($call['to'] ?? '');
        if ($fromId === $from) $fromId = $to;
        if ($toId === $from) $toId = $to;
        if ($fromId === '' || $toId === '' || $fromId === $toId) {
            unset($store['calls'][$index]);
            continue;
        }
        $store['calls'][$index]['from'] = $fromId;
        $store['calls'][$index]['to'] = $toId;
    }
    $store['calls'] = array_values($store['calls']);

    $fromSubs = $store['push_subscriptions'][$from] ?? [];
    $toSubs = $store['push_subscriptions'][$to] ?? [];
    if ($fromSubs || $toSubs) {
        $store['push_subscriptions'][$to] = mergePushSubscriptions($toSubs, $fromSubs);
    }
    if (isset($store['push_subscriptions'][$from])) {
        unset($store['push_subscriptions'][$from]);
    }
    if (isset($store['account_links']) && is_array($store['account_links'])) {
        foreach ($store['account_links'] as $identityKey => $mappedId) {
            if (normalizeId($mappedId) !== $from) continue;
            $store['account_links'][$identityKey] = $to;
        }
    }
}

function upsertUser(&$store, $userId, $name, $avatar = '', $externalKey = '', $username = '') {
    $id = normalizeId($userId);
    if ($id === '') return null;
    $safeName = normalizeText($name);
    if ($safeName === '') {
        $safeName = 'Пользователь';
    }
    $safeAvatar = normalizeText($avatar);
    $safeExternalKey = normalizeExternalKey($externalKey);
    $safeUsername = normalizeUsernameValue($username);
    if ($safeUsername === '') {
        $safeUsername = buildGeneratedUsername($id);
    }
    if ($safeExternalKey !== '') {
        $byExternal = findUserIndexByExternalKey($store, $safeExternalKey);
        if ($byExternal >= 0) {
            $externalId = normalizeId($store['users'][$byExternal]['id'] ?? '');
            if ($externalId !== '' && $externalId !== $id) {
                remapUserIdInStore($store, $id, $externalId);
                $id = $externalId;
            }
        }
    }
    $updatedAt = nowTs();
    foreach ($store['users'] as $index => $user) {
        if (normalizeId($user['id'] ?? '') !== $id) continue;
        $store['users'][$index]['name'] = $safeName;
        $store['users'][$index]['avatar'] = $safeAvatar;
        if ($safeExternalKey !== '') {
            $store['users'][$index]['external_key'] = $safeExternalKey;
        }
        if ($safeUsername !== '') {
            $store['users'][$index]['username'] = $safeUsername;
        } elseif (!isset($store['users'][$index]['username'])) {
            $store['users'][$index]['username'] = '';
        }
        $store['users'][$index]['last_seen'] = $updatedAt;
        if (!isset($store['users'][$index]['active_tab'])) $store['users'][$index]['active_tab'] = false;
        if (!isset($store['users'][$index]['presence_updated_at'])) $store['users'][$index]['presence_updated_at'] = 0;
        return $store['users'][$index];
    }
    $created = [
        'id' => $id,
        'name' => $safeName,
        'avatar' => $safeAvatar,
        'username' => $safeUsername,
        'external_key' => $safeExternalKey,
        'active_tab' => false,
        'presence_updated_at' => 0,
        'last_seen' => $updatedAt
    ];
    $store['users'][] = $created;
    return $created;
}

function isFriends($store, $firstId, $secondId) {
    $key = pairKey($firstId, $secondId);
    if ($key === '') return false;
    foreach ($store['friends'] as $friendship) {
        if (($friendship['key'] ?? '') === $key) {
            return true;
        }
    }
    return false;
}

function createFriendship(&$store, $firstId, $secondId) {
    $key = pairKey($firstId, $secondId);
    if ($key === '') return false;
    if (isFriends($store, $firstId, $secondId)) {
        return true;
    }
    $store['friends'][] = [
        'key' => $key,
        'a' => normalizeId($firstId),
        'b' => normalizeId($secondId),
        'created_at' => nowTs()
    ];
    return true;
}

function removeFriendship(&$store, $firstId, $secondId) {
    $key = pairKey($firstId, $secondId);
    if ($key === '') return;
    $store['friends'] = array_values(array_filter($store['friends'], function ($friendship) use ($key) {
        return ($friendship['key'] ?? '') !== $key;
    }));
}

function buildState($store, $appUserId) {
    $appUserId = normalizeId($appUserId);
    $usersById = [];
    foreach ($store['users'] as $user) {
        $uid = normalizeId($user['id'] ?? '');
        if ($uid === '') continue;
        $usersById[$uid] = $user;
    }

    $friends = [];
    foreach ($store['friends'] as $friendship) {
        $a = normalizeId($friendship['a'] ?? '');
        $b = normalizeId($friendship['b'] ?? '');
        if ($a === $appUserId) {
            $friendUser = $usersById[$b] ?? ['id' => $b, 'name' => 'Пользователь', 'avatar' => ''];
            $friends[] = [
                'id' => normalizeId($friendUser['id'] ?? ''),
                'name' => normalizeText($friendUser['name'] ?? 'Пользователь'),
                'avatar' => normalizeText($friendUser['avatar'] ?? '')
            ];
        } elseif ($b === $appUserId) {
            $friendUser = $usersById[$a] ?? ['id' => $a, 'name' => 'Пользователь', 'avatar' => ''];
            $friends[] = [
                'id' => normalizeId($friendUser['id'] ?? ''),
                'name' => normalizeText($friendUser['name'] ?? 'Пользователь'),
                'avatar' => normalizeText($friendUser['avatar'] ?? '')
            ];
        }
    }

    usort($friends, function ($left, $right) {
        return mb_strtolower($left['name']) <=> mb_strtolower($right['name']);
    });

    $incomingRequests = [];
    $outgoingRequests = [];
    foreach ($store['requests'] as $request) {
        $status = normalizeText($request['status'] ?? '');
        if ($status !== 'pending') continue;
        $from = normalizeId($request['from'] ?? '');
        $to = normalizeId($request['to'] ?? '');
        if ($to === $appUserId) {
            $fromUser = $usersById[$from] ?? ['id' => $from, 'name' => 'Пользователь', 'avatar' => ''];
            $incomingRequests[] = [
                'requestId' => normalizeId($request['id'] ?? ''),
                'fromId' => $from,
                'name' => normalizeText($fromUser['name'] ?? 'Пользователь'),
                'avatar' => normalizeText($fromUser['avatar'] ?? ''),
                'createdAt' => (int)($request['created_at'] ?? nowTs())
            ];
        } elseif ($from === $appUserId) {
            $toUser = $usersById[$to] ?? ['id' => $to, 'name' => 'Пользователь', 'avatar' => ''];
            $outgoingRequests[] = [
                'requestId' => normalizeId($request['id'] ?? ''),
                'toId' => $to,
                'name' => normalizeText($toUser['name'] ?? 'Пользователь'),
                'avatar' => normalizeText($toUser['avatar'] ?? ''),
                'createdAt' => (int)($request['created_at'] ?? nowTs())
            ];
        }
    }

    $incomingCalls = [];
    $outgoingCalls = [];
    foreach ($store['calls'] as $invite) {
        $from = normalizeId($invite['from'] ?? '');
        $to = normalizeId($invite['to'] ?? '');
        $status = normalizeText($invite['status'] ?? '');
        if ($to === $appUserId && $status === 'pending') {
            $fromUser = $usersById[$from] ?? ['id' => $from, 'name' => 'Пользователь', 'avatar' => ''];
            $incomingCalls[] = [
                'inviteId' => normalizeId($invite['id'] ?? ''),
                'fromId' => $from,
                'fromName' => normalizeText($fromUser['name'] ?? 'Пользователь'),
                'fromAvatar' => normalizeText($fromUser['avatar'] ?? ''),
                'roomId' => normalizeText($invite['room_id'] ?? ''),
                'createdAt' => (int)($invite['created_at'] ?? nowTs())
            ];
        }
        if ($from === $appUserId && $status !== 'pending') {
            $toUser = $usersById[$to] ?? ['id' => $to, 'name' => 'Пользователь', 'avatar' => ''];
            $outgoingCalls[] = [
                'inviteId' => normalizeId($invite['id'] ?? ''),
                'toId' => $to,
                'toName' => normalizeText($toUser['name'] ?? 'Пользователь'),
                'status' => $status,
                'roomId' => normalizeText($invite['room_id'] ?? ''),
                'updatedAt' => (int)($invite['updated_at'] ?? nowTs())
            ];
        }
    }

    usort($incomingRequests, function ($left, $right) {
        return ($right['createdAt'] ?? 0) <=> ($left['createdAt'] ?? 0);
    });
    usort($outgoingRequests, function ($left, $right) {
        return ($right['createdAt'] ?? 0) <=> ($left['createdAt'] ?? 0);
    });
    usort($incomingCalls, function ($left, $right) {
        return ($right['createdAt'] ?? 0) <=> ($left['createdAt'] ?? 0);
    });
    usort($outgoingCalls, function ($left, $right) {
        return ($right['updatedAt'] ?? 0) <=> ($left['updatedAt'] ?? 0);
    });
    $outgoingCalls = array_slice($outgoingCalls, 0, 30);

    return [
        'self' => findUser($store, $appUserId),
        'friends' => $friends,
        'incomingRequests' => $incomingRequests,
        'outgoingRequests' => $outgoingRequests,
        'incomingCalls' => $incomingCalls,
        'outgoingCalls' => $outgoingCalls
    ];
}

$method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
$body = [];
if ($method === 'POST') {
    $body = json_decode(file_get_contents('php://input'), true);
    if (!is_array($body)) $body = [];
}
$source = $method === 'POST' ? $body : $_GET;
$action = normalizeText($source['action'] ?? '');

if ($action === '') {
    friendsResponse(false, null, 'Action required');
}

$store = friendsLoadStore();

if ($action === 'register') {
    $appUserId = normalizeId($source['app_user_id'] ?? '');
    $name = normalizeText($source['name'] ?? '');
    $avatar = normalizeText($source['avatar'] ?? '');
    $username = normalizeText($source['username'] ?? '');
    $externalKey = normalizeExternalKey($source['external_key'] ?? '');
    $identityKeys = normalizeIdentityKeys($source['identity_keys'] ?? [], $externalKey);
    $previousAppUserId = normalizeId($source['previous_app_user_id'] ?? '');
    $activeTab = normalizeBoolFlag($source['active_tab'] ?? false, false);
    $storeChanged = false;
    if ($appUserId === '') {
        friendsResponse(false, null, 'app_user_id required');
    }
    $mappedIdentityId = resolveMappedUserIdByIdentity($store, $identityKeys);
    if ($mappedIdentityId !== '' && $mappedIdentityId !== $appUserId) {
        remapUserIdInStore($store, $appUserId, $mappedIdentityId);
        $storeChanged = true;
        if ($previousAppUserId !== '' && $previousAppUserId !== $mappedIdentityId) {
            remapUserIdInStore($store, $previousAppUserId, $mappedIdentityId);
            $storeChanged = true;
        }
        $appUserId = $mappedIdentityId;
    } elseif ($mappedIdentityId === '') {
        $primaryIdentityKey = pickPrimaryIdentityKey($identityKeys);
        $generatedCanonicalId = buildCanonicalUserIdFromIdentityKey($primaryIdentityKey);
        if ($generatedCanonicalId !== '' && $generatedCanonicalId !== $appUserId) {
            remapUserIdInStore($store, $appUserId, $generatedCanonicalId);
            $storeChanged = true;
            if ($previousAppUserId !== '' && $previousAppUserId !== $generatedCanonicalId) {
                remapUserIdInStore($store, $previousAppUserId, $generatedCanonicalId);
                $storeChanged = true;
            }
            $appUserId = $generatedCanonicalId;
        }
    }
    if ($externalKey !== '') {
        $existingByExternal = findUserIndexByExternalKey($store, $externalKey);
        if ($existingByExternal >= 0) {
            $existingId = normalizeId($store['users'][$existingByExternal]['id'] ?? '');
            if ($existingId !== '' && $existingId !== $appUserId) {
                remapUserIdInStore($store, $appUserId, $existingId);
                $storeChanged = true;
                if ($previousAppUserId !== '' && $previousAppUserId !== $existingId) {
                    remapUserIdInStore($store, $previousAppUserId, $existingId);
                    $storeChanged = true;
                }
                $appUserId = $existingId;
            }
        }
    } elseif ($previousAppUserId !== '' && $previousAppUserId !== $appUserId) {
        remapUserIdInStore($store, $previousAppUserId, $appUserId);
        $storeChanged = true;
    }
    $user = upsertUser($store, $appUserId, $name, $avatar, $externalKey, $username);
    $effectiveAppUserId = normalizeId($user['id'] ?? $appUserId);
    if (bindIdentityKeysToUser($store, $effectiveAppUserId, $identityKeys)) {
        $storeChanged = true;
    }
    $presenceChanged = touchUserPresence($store, $effectiveAppUserId, $activeTab);
    if ($presenceChanged) {
        $storeChanged = true;
    }
    if ($storeChanged && !friendsSaveStore($store)) {
        friendsResponse(false, null, 'Cannot write store');
    }
    friendsResponse(true, ['user' => $user, 'appUserId' => $effectiveAppUserId]);
}

$appUserId = normalizeId($source['app_user_id'] ?? '');
$externalKey = normalizeExternalKey($source['external_key'] ?? '');
$identityKeys = normalizeIdentityKeys($source['identity_keys'] ?? [], $externalKey);
$storeChanged = false;
if ($appUserId === '') {
    friendsResponse(false, null, 'app_user_id required');
}
$mappedIdentityId = resolveMappedUserIdByIdentity($store, $identityKeys);
if ($mappedIdentityId !== '' && $mappedIdentityId !== $appUserId) {
    remapUserIdInStore($store, $appUserId, $mappedIdentityId);
    $storeChanged = true;
    $appUserId = $mappedIdentityId;
} elseif ($mappedIdentityId === '') {
    $primaryIdentityKey = pickPrimaryIdentityKey($identityKeys);
    $generatedCanonicalId = buildCanonicalUserIdFromIdentityKey($primaryIdentityKey);
    if ($generatedCanonicalId !== '' && $generatedCanonicalId !== $appUserId) {
        remapUserIdInStore($store, $appUserId, $generatedCanonicalId);
        $storeChanged = true;
        $appUserId = $generatedCanonicalId;
    }
}
if ($externalKey !== '') {
    $existingByExternal = findUserIndexByExternalKey($store, $externalKey);
    if ($existingByExternal >= 0) {
        $existingId = normalizeId($store['users'][$existingByExternal]['id'] ?? '');
        if ($existingId !== '' && $existingId !== $appUserId) {
            remapUserIdInStore($store, $appUserId, $existingId);
            $storeChanged = true;
            $appUserId = $existingId;
        }
    }
}
if (!findUser($store, $appUserId)) {
    upsertUser(
        $store,
        $appUserId,
        normalizeText($source['name'] ?? 'Пользователь'),
        normalizeText($source['avatar'] ?? ''),
        $externalKey,
        normalizeText($source['username'] ?? '')
    );
    $storeChanged = true;
}
if (bindIdentityKeysToUser($store, $appUserId, $identityKeys)) {
    $storeChanged = true;
}
$activeTab = normalizeBoolFlag($source['active_tab'] ?? false, false);
$presenceChanged = touchUserPresence($store, $appUserId, $activeTab);
if ($presenceChanged || $storeChanged) {
    friendsSaveStore($store);
}

if ($action === 'state') {
    friendsResponse(true, buildState($store, $appUserId));
}

if ($action === 'push_config') {
    $keys = friendsGetVapidKeys($store);
    if (normalizeText($keys['public_key'] ?? '') === '') {
        friendsResponse(false, null, 'Push keys unavailable');
    }
    friendsSaveStore($store);
    friendsResponse(true, ['publicKey' => $keys['public_key']]);
}

if ($action === 'save_push_subscription') {
    $subscription = $source['subscription'] ?? null;
    $saved = friendsStorePushSubscription($store, $appUserId, is_array($subscription) ? $subscription : []);
    if (!$saved) {
        friendsResponse(false, null, 'Некорректная push подписка');
    }
    if (!friendsSaveStore($store)) {
        friendsResponse(false, null, 'Cannot write store');
    }
    friendsResponse(true, ['saved' => true]);
}

if ($action === 'search') {
    $query = mb_strtolower(normalizeText($source['query'] ?? ''));
    if ($query === '') {
        friendsResponse(true, ['results' => []]);
    }
    $results = [];
    $seenIdentity = [];
    foreach ($store['users'] as $user) {
        $candidateId = normalizeId($user['id'] ?? '');
        if ($candidateId === '' || $candidateId === $appUserId) continue;
        $identityKey = normalizeExternalKey($user['external_key'] ?? '');
        $dedupeKey = $identityKey !== '' ? $identityKey : ('id:' . mb_strtolower($candidateId));
        if (isset($seenIdentity[$dedupeKey])) continue;
        $candidateName = normalizeText($user['name'] ?? '');
        $candidateUsername = normalizeText($user['username'] ?? '');
        $idMatch = mb_strpos(mb_strtolower($candidateId), $query) !== false;
        $nameMatch = mb_strpos(mb_strtolower($candidateName), $query) !== false;
        $usernameMatch = $candidateUsername !== '' && mb_strpos(mb_strtolower($candidateUsername), $query) !== false;
        if (!$idMatch && !$nameMatch && !$usernameMatch) continue;
        $seenIdentity[$dedupeKey] = true;
        $incomingPending = false;
        $outgoingPending = false;
        foreach ($store['requests'] as $request) {
            if (($request['status'] ?? '') !== 'pending') continue;
            $from = normalizeId($request['from'] ?? '');
            $to = normalizeId($request['to'] ?? '');
            if ($from === $appUserId && $to === $candidateId) {
                $outgoingPending = true;
            }
            if ($from === $candidateId && $to === $appUserId) {
                $incomingPending = true;
            }
        }
        $results[] = [
            'id' => $candidateId,
            'name' => $candidateName !== '' ? $candidateName : 'Пользователь',
            'username' => $candidateUsername,
            'avatar' => normalizeText($user['avatar'] ?? ''),
            'isFriend' => isFriends($store, $appUserId, $candidateId),
            'incomingPending' => $incomingPending,
            'outgoingPending' => $outgoingPending
        ];
        if (count($results) >= 40) break;
    }
    friendsResponse(true, ['results' => $results]);
}

if ($action === 'send_request') {
    $targetId = normalizeId($source['target_id'] ?? '');
    if ($targetId === '' || $targetId === $appUserId) {
        friendsResponse(false, null, 'Некорректный target_id');
    }
    if (!findUser($store, $targetId)) {
        friendsResponse(false, null, 'Пользователь не найден');
    }
    if (isFriends($store, $appUserId, $targetId)) {
        friendsResponse(false, null, 'Уже в друзьях');
    }

    foreach ($store['requests'] as $index => $request) {
        $from = normalizeId($request['from'] ?? '');
        $to = normalizeId($request['to'] ?? '');
        $status = normalizeText($request['status'] ?? '');
        if ($status !== 'pending') continue;
        if ($from === $appUserId && $to === $targetId) {
            friendsResponse(true, ['status' => 'already_pending']);
        }
        if ($from === $targetId && $to === $appUserId) {
            $store['requests'][$index]['status'] = 'accepted';
            $store['requests'][$index]['updated_at'] = nowTs();
            createFriendship($store, $appUserId, $targetId);
            if (!friendsSaveStore($store)) {
                friendsResponse(false, null, 'Cannot write store');
            }
            friendsResponse(true, ['status' => 'auto_accepted']);
        }
    }

    $store['requests'][] = [
        'id' => 'fr_' . substr(md5($appUserId . '|' . $targetId . '|' . microtime(true)), 0, 16),
        'from' => $appUserId,
        'to' => $targetId,
        'status' => 'pending',
        'created_at' => nowTs(),
        'updated_at' => nowTs()
    ];
    if (!friendsSaveStore($store)) {
        friendsResponse(false, null, 'Cannot write store');
    }
    friendsResponse(true, ['status' => 'sent']);
}

if ($action === 'respond_request') {
    $requestId = normalizeId($source['request_id'] ?? '');
    $decision = normalizeText($source['decision'] ?? '');
    if ($requestId === '' || !in_array($decision, ['accept', 'decline'], true)) {
        friendsResponse(false, null, 'Некорректные параметры');
    }
    $updated = false;
    foreach ($store['requests'] as $index => $request) {
        if (normalizeId($request['id'] ?? '') !== $requestId) continue;
        if (normalizeId($request['to'] ?? '') !== $appUserId) {
            friendsResponse(false, null, 'Нет прав на обработку заявки');
        }
        if (normalizeText($request['status'] ?? '') !== 'pending') {
            friendsResponse(true, ['status' => 'already_processed']);
        }
        $store['requests'][$index]['status'] = $decision === 'accept' ? 'accepted' : 'declined';
        $store['requests'][$index]['updated_at'] = nowTs();
        if ($decision === 'accept') {
            createFriendship($store, normalizeId($request['from'] ?? ''), $appUserId);
        }
        $updated = true;
        break;
    }
    if (!$updated) {
        friendsResponse(false, null, 'Заявка не найдена');
    }
    if (!friendsSaveStore($store)) {
        friendsResponse(false, null, 'Cannot write store');
    }
    friendsResponse(true, ['status' => $decision === 'accept' ? 'accepted' : 'declined']);
}

if ($action === 'remove_friend') {
    $friendId = normalizeId($source['friend_id'] ?? '');
    if ($friendId === '') {
        friendsResponse(false, null, 'friend_id required');
    }
    removeFriendship($store, $appUserId, $friendId);
    $store['requests'] = array_values(array_filter($store['requests'], function ($request) use ($appUserId, $friendId) {
        $from = normalizeId($request['from'] ?? '');
        $to = normalizeId($request['to'] ?? '');
        $isPair = ($from === $appUserId && $to === $friendId) || ($from === $friendId && $to === $appUserId);
        if (!$isPair) return true;
        return normalizeText($request['status'] ?? '') !== 'pending';
    }));
    if (!friendsSaveStore($store)) {
        friendsResponse(false, null, 'Cannot write store');
    }
    friendsResponse(true, ['removed' => true]);
}

if ($action === 'send_call_invite') {
    $targetId = normalizeId($source['target_id'] ?? '');
    $roomId = normalizeText($source['room_id'] ?? '');
    if ($targetId === '' || $roomId === '') {
        friendsResponse(false, null, 'Некорректные параметры звонка');
    }
    if (!isFriends($store, $appUserId, $targetId)) {
        friendsResponse(false, null, 'Звонок доступен только друзьям');
    }
    foreach ($store['calls'] as $index => $call) {
        if (normalizeId($call['from'] ?? '') !== $appUserId) continue;
        if (normalizeId($call['to'] ?? '') !== $targetId) continue;
        if (normalizeText($call['status'] ?? '') !== 'pending') continue;
        $store['calls'][$index]['status'] = 'cancelled';
        $store['calls'][$index]['updated_at'] = nowTs();
    }
    $inviteId = 'call_' . substr(md5($appUserId . '|' . $targetId . '|' . microtime(true)), 0, 16);
    $store['calls'][] = [
        'id' => $inviteId,
        'from' => $appUserId,
        'to' => $targetId,
        'room_id' => $roomId,
        'status' => 'pending',
        'created_at' => nowTs(),
        'updated_at' => nowTs()
    ];
    if (!friendsSaveStore($store)) {
        friendsResponse(false, null, 'Cannot write store');
    }
    friendsNotifyIncomingCall($store, $targetId);
    friendsSaveStore($store);
    friendsResponse(true, ['inviteId' => $inviteId]);
}

if ($action === 'respond_call_invite') {
    $inviteId = normalizeId($source['invite_id'] ?? '');
    $decision = normalizeText($source['decision'] ?? '');
    if ($inviteId === '' || !in_array($decision, ['answer', 'decline'], true)) {
        friendsResponse(false, null, 'Некорректные параметры');
    }
    foreach ($store['calls'] as $index => $call) {
        if (normalizeId($call['id'] ?? '') !== $inviteId) continue;
        if (normalizeId($call['to'] ?? '') !== $appUserId) {
            friendsResponse(false, null, 'Нет прав на обработку звонка');
        }
        if (normalizeText($call['status'] ?? '') !== 'pending') {
            friendsResponse(true, ['status' => 'already_processed', 'roomId' => normalizeText($call['room_id'] ?? '')]);
        }
        $nextStatus = $decision === 'answer' ? 'accepted' : 'declined';
        $store['calls'][$index]['status'] = $nextStatus;
        $store['calls'][$index]['updated_at'] = nowTs();
        if (!friendsSaveStore($store)) {
            friendsResponse(false, null, 'Cannot write store');
        }
        friendsResponse(true, ['status' => $nextStatus, 'roomId' => normalizeText($call['room_id'] ?? '')]);
    }
    friendsResponse(false, null, 'Приглашение не найдено');
}

if ($action === 'cancel_call_invite') {
    $inviteId = normalizeId($source['invite_id'] ?? '');
    if ($inviteId === '') {
        friendsResponse(false, null, 'invite_id required');
    }
    foreach ($store['calls'] as $index => $call) {
        if (normalizeId($call['id'] ?? '') !== $inviteId) continue;
        if (normalizeId($call['from'] ?? '') !== $appUserId) {
            friendsResponse(false, null, 'Нет прав на отмену звонка');
        }
        if (normalizeText($call['status'] ?? '') !== 'pending') {
            friendsResponse(true, ['status' => normalizeText($call['status'] ?? '')]);
        }
        $store['calls'][$index]['status'] = 'cancelled';
        $store['calls'][$index]['updated_at'] = nowTs();
        if (!friendsSaveStore($store)) {
            friendsResponse(false, null, 'Cannot write store');
        }
        friendsResponse(true, ['status' => 'cancelled']);
    }
    friendsResponse(false, null, 'Приглашение не найдено');
}

friendsResponse(false, null, 'Unknown action');
?>
