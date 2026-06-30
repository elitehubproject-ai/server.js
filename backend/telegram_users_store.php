<?php
const TELEGRAM_USERS_STORE = __DIR__ . '/telegram_users.json';

function tgLoadUsers() {
    if (!file_exists(TELEGRAM_USERS_STORE)) {
        return [];
    }
    $raw = file_get_contents(TELEGRAM_USERS_STORE);
    if ($raw === false) return [];
    if (!preg_match('//u', $raw)) {
        $raw = mb_convert_encoding($raw, 'UTF-8', 'Windows-1251');
    }
    
    $decoded = json_decode($raw, true);
    return is_array($decoded) ? $decoded : [];
}

function tgSaveUsers($users) {
    $payload = json_encode(array_values($users), JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    $result = @file_put_contents(TELEGRAM_USERS_STORE, $payload, LOCK_EX);
    if ($result === false) {
        if (file_exists(TELEGRAM_USERS_STORE)) {
            @chmod(TELEGRAM_USERS_STORE, 0666);
        } else {
            @chmod(__DIR__, 0777);
        }
        $result = @file_put_contents(TELEGRAM_USERS_STORE, $payload, LOCK_EX);
    }
    return $result !== false;
}

function tgUpsertUser($from) {
    $id = isset($from['id']) ? (string)$from['id'] : '';
    if ($id === '') {
        return;
    }
    $users = tgLoadUsers();
    $updated = false;
    foreach ($users as &$user) {
        if ((string)($user['id'] ?? '') === $id) {
            $user['username'] = isset($from['username']) ? ('@' . ltrim((string)$from['username'], '@')) : ($user['username'] ?? '');
            $nameParts = array_filter([(string)($from['first_name'] ?? ''), (string)($from['last_name'] ?? '')]);
            $user['name'] = trim(implode(' ', $nameParts)) ?: ($user['name'] ?? 'Telegram User');
            $user['last_seen'] = time();
            $updated = true;
            break;
        }
    }
    unset($user);
    if (!$updated) {
        $nameParts = array_filter([(string)($from['first_name'] ?? ''), (string)($from['last_name'] ?? '')]);
        $users[] = [
            'id' => $id,
            'username' => isset($from['username']) ? ('@' . ltrim((string)$from['username'], '@')) : '',
            'name' => trim(implode(' ', $nameParts)) ?: 'Telegram User',
            'last_seen' => time()
        ];
    }
    tgSaveUsers($users);
}

function tgUsernameAvatar($username) {
    $clean = ltrim((string)$username, '@');
    if ($clean === '') return '';
    return 'https://t.me/i/userpic/320/' . rawurlencode($clean) . '.jpg';
}

function tgFindUserIdByUsername($username) {
    $found = tgFindUserByUsername($username);
    return (string)($found['id'] ?? '');
}

function tgFindUserByUsername($username) {
    $needle = strtolower(ltrim((string)$username, '@'));
    if ($needle === '') return null;
    $users = tgLoadUsers();
    foreach ($users as $user) {
        $existing = strtolower(ltrim((string)($user['username'] ?? ''), '@'));
        if ($existing !== '' && $existing === $needle) {
            return $user;
        }
    }
    return null;
}

function tgNormalizeUsername($username) {
    $normalized = trim((string)$username);
    if ($normalized === '') return '';
    $normalized = preg_replace('#^https?://t\.me/#i', '', $normalized);
    $normalized = '@' . ltrim($normalized, '@');
    if (!preg_match('/^@[a-zA-Z0-9_]{4,32}$/', $normalized)) {
        return '';
    }
    return $normalized;
}

function tgFindUserIndexById($users, $telegramId) {
    foreach ($users as $i => $user) {
        if ((string)($user['id'] ?? '') === (string)$telegramId) {
            return $i;
        }
    }
    return -1;
}

function tgAddContactFor($telegramId, $contactData) {
    $ownerId = trim((string)$telegramId);
    $contactName = trim((string)($contactData['name'] ?? ''));
    $contactUsername = tgNormalizeUsername((string)($contactData['username'] ?? ''));
    $linkedId = trim((string)($contactData['linkedTelegramId'] ?? ''));
    $providedAvatar = trim((string)($contactData['avatar'] ?? ''));
    if ($ownerId === '' || !preg_match('/^\d+$/', $ownerId)) {
        return ['success' => false, 'error' => 'Invalid telegram_id'];
    }
    if ($contactName === '') {
        return ['success' => false, 'error' => 'Empty contact name'];
    }
    if ($contactUsername === '') {
        return ['success' => false, 'error' => 'Invalid username'];
    }

    $users = tgLoadUsers();
    $ownerIndex = tgFindUserIndexById($users, $ownerId);
    if ($ownerIndex < 0) {
        $users[] = [
            'id' => $ownerId,
            'username' => '',
            'name' => 'Telegram User',
            'last_seen' => time(),
            'custom_contacts' => []
        ];
        $ownerIndex = count($users) - 1;
    }

    $owner = $users[$ownerIndex];
    $customContacts = is_array($owner['custom_contacts'] ?? null) ? $owner['custom_contacts'] : [];
    $hiddenContactIds = is_array($owner['hidden_contact_ids'] ?? null) ? $owner['hidden_contact_ids'] : [];
    if ($linkedId === '') {
        $linkedId = tgFindUserIdByUsername($contactUsername);
    }
    $avatar = $providedAvatar !== '' ? $providedAvatar : tgUsernameAvatar($contactUsername);
    $contactKey = strtolower($contactUsername);
    $updated = false;

    foreach ($customContacts as &$contact) {
        $existingUsername = strtolower((string)($contact['username'] ?? ''));
        if ($existingUsername !== '' && $existingUsername === $contactKey) {
            $contact['name'] = $contactName;
            $contact['username'] = $contactUsername;
            $contact['target'] = $linkedId !== '' ? $linkedId : $contactUsername;
            $contact['avatar'] = $avatar;
            $contact['updated_at'] = time();
            $contact['linkedTelegramId'] = $linkedId;
            $updated = true;
            break;
        }
    }
    unset($contact);

    if (!$updated) {
        $customContacts[] = [
            'id' => 'manual_' . substr(md5($contactKey), 0, 12),
            'name' => $contactName,
            'username' => $contactUsername,
            'target' => $linkedId !== '' ? $linkedId : $contactUsername,
            'avatar' => $avatar,
            'linkedTelegramId' => $linkedId,
            'updated_at' => time()
        ];
    }

    $owner['custom_contacts'] = $customContacts;
    $manualId = 'manual_' . substr(md5($contactKey), 0, 12);
    $linkedIdLower = strtolower($linkedId);
    $owner['hidden_contact_ids'] = array_values(array_filter($hiddenContactIds, function ($v) use ($contactKey, $manualId, $linkedIdLower) {
        $value = strtolower((string)$v);
        if ($value === $contactKey) return false;
        if ($value === strtolower($manualId)) return false;
        if ($linkedIdLower !== '' && $value === $linkedIdLower) return false;
        return true;
    }));
    $users[$ownerIndex] = $owner;
    if (!tgSaveUsers($users)) {
        return ['success' => false, 'error' => 'Cannot write contacts'];
    }
    return ['success' => true];
}

function tgDeleteContactFor($telegramId, $contactId) {
    $ownerId = trim((string)$telegramId);
    $targetId = trim((string)$contactId);
    if ($ownerId === '' || !preg_match('/^\d+$/', $ownerId)) {
        return ['success' => false, 'error' => 'Invalid telegram_id'];
    }
    if ($targetId === '') {
        return ['success' => false, 'error' => 'Invalid contact id'];
    }

    $users = tgLoadUsers();
    $ownerIndex = tgFindUserIndexById($users, $ownerId);
    if ($ownerIndex < 0) {
        return ['success' => true];
    }

    $owner = $users[$ownerIndex];
    $customContacts = is_array($owner['custom_contacts'] ?? null) ? $owner['custom_contacts'] : [];
    $hiddenContactIds = is_array($owner['hidden_contact_ids'] ?? null) ? $owner['hidden_contact_ids'] : [];
    $targetIdLower = strtolower($targetId);
    $customContacts = array_values(array_filter($customContacts, function ($contact) use ($targetId, $targetIdLower) {
        $id = (string)($contact['id'] ?? '');
        $username = strtolower((string)($contact['username'] ?? ''));
        $target = strtolower((string)($contact['target'] ?? ''));
        if ($id === $targetId) return false;
        if ($username !== '' && $username === $targetIdLower) return false;
        if ($target !== '' && $target === $targetIdLower) return false;
        return true;
    }));

    $owner['custom_contacts'] = $customContacts;
    if (!in_array($targetIdLower, array_map('strtolower', $hiddenContactIds), true)) {
        $hiddenContactIds[] = $targetIdLower;
    }
    $owner['hidden_contact_ids'] = array_values($hiddenContactIds);
    $users[$ownerIndex] = $owner;
    if (!tgSaveUsers($users)) {
        return ['success' => false, 'error' => 'Cannot write contacts'];
    }
    return ['success' => true];
}

function tgGetContactsFor($telegramId) {
    $users = tgLoadUsers();
    $contacts = [];
    $seen = [];
    $ownerIndex = tgFindUserIndexById($users, $telegramId);
    $hiddenLookup = [];
    if ($ownerIndex >= 0) {
        $owner = $users[$ownerIndex];
        $customContacts = is_array($owner['custom_contacts'] ?? null) ? $owner['custom_contacts'] : [];
        $hiddenContactIds = is_array($owner['hidden_contact_ids'] ?? null) ? $owner['hidden_contact_ids'] : [];
        foreach ($hiddenContactIds as $hiddenId) {
            $hiddenLookup[strtolower((string)$hiddenId)] = true;
        }
        foreach ($customContacts as $contact) {
            $username = tgNormalizeUsername((string)($contact['username'] ?? ''));
            $target = (string)($contact['target'] ?? '');
            $key = strtolower((string)($contact['id'] ?? $username ?: $target));
            if ($key === '' || isset($seen[$key])) {
                continue;
            }
            if (isset($hiddenLookup[$key]) || ($username !== '' && isset($hiddenLookup[strtolower($username)]))) {
                continue;
            }
            $seen[$key] = true;
            $contacts[] = [
                'id' => (string)($contact['id'] ?? $key),
                'name' => (string)($contact['name'] ?? 'Контакт'),
                'avatar' => (string)($contact['avatar'] ?? tgUsernameAvatar($username)),
                'username' => $username,
                'target' => $target !== '' ? $target : ($username !== '' ? $username : '')
            ];
        }
    }

    foreach ($users as $user) {
        $id = (string)($user['id'] ?? '');
        if ($id === '' || $id === (string)$telegramId) {
            continue;
        }
        $username = (string)($user['username'] ?? '');
        $key = strtolower($id);
        if (isset($seen[$key])) {
            continue;
        }
        $usernameKey = strtolower((string)$username);
        if (isset($hiddenLookup[$key]) || ($usernameKey !== '' && isset($hiddenLookup[$usernameKey]))) {
            continue;
        }
        $seen[$key] = true;
        $contacts[] = [
            'id' => $id,
            'name' => (string)($user['name'] ?? 'Telegram User'),
            'avatar' => tgUsernameAvatar($username),
            'username' => $username,
            'target' => $id
        ];
    }
    return $contacts;
}
?>
