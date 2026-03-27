'use strict';

const http = require('http');
const https = require('https');
const { URL } = require('url');

console.log('[messenger_http] process.env keys:', Object.keys(process.env).filter(k => k.includes('MESSENGER') || k.includes('API')));

let apiUrl = '';
let apiKey = '';
let enabled = false;

function env(name, def = '') {
  const v = process.env[name];
  console.log(`[messenger_http] env("${name}") =`, v ? `"${v.substring(0, 5)}..."` : 'undefined/empty');
  if (v == null) return def;
  const t = String(v).trim();
  return t.length ? t : def;
}

function sortedPair(a, b) {
  const x = String(a);
  const y = String(b);
  return x < y ? [x, y] : [y, x];
}

function directChatId(a, b) {
  const [u1, u2] = sortedPair(a, b);
  if (!u1 || !u2) return '';
  return `dm:${u1}::${u2}`;
}

// HTTP запрос к PHP API
async function apiRequest(action, payload = {}, isInit = false) {
  if (!apiUrl) {
    throw new Error('API URL not set');
  }

  const url = new URL(apiUrl);
  const postData = JSON.stringify({ action, ...payload });

  const options = {
    hostname: url.hostname,
    port: url.port || (url.protocol === 'https:' ? 443 : 80),
    path: url.pathname,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(postData),
      'X-API-Key': apiKey
    },
    timeout: 30000
  };

  return new Promise((resolve, reject) => {
    const client = url.protocol === 'https:' ? https : http;

    const req = client.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            reject(new Error(parsed.error));
          } else {
            resolve(parsed.data);
          }
        } catch (e) {
          reject(new Error('Invalid JSON response: ' + data.substring(0, 200)));
        }
      });
    });

    req.on('error', (err) => {
      reject(err);
    });

    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });

    req.write(postData);
    req.end();
  });
}

async function initMessengerMysql() {
  apiUrl = env('MESSENGER_API_URL', '');
  apiKey = env('MESSENGER_API_KEY', '');

  console.log('[messenger_http] MESSENGER_API_URL:', apiUrl ? 'set' : 'NOT SET');
  console.log('[messenger_http] MESSENGER_API_KEY:', apiKey ? 'set (length: ' + apiKey.length + ')' : 'NOT SET');

  if (!apiUrl || !apiKey) {
    console.warn('[messenger_http] MESSENGER_API_URL or MESSENGER_API_KEY not set');
    return false;
  }

  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const maxRetries = 10;
  const delayMs = 1500;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`[messenger_http] init attempt ${attempt}/${maxRetries}...`);
      await apiRequest('listAllUserIds', {}, true);
      enabled = true;
      console.log('[messenger_http] connected via HTTP API');
      return true;
    } catch (err) {
      console.error(`[messenger_http] attempt ${attempt} failed:`, err.message.substring(0, 100));
      if (attempt < maxRetries) {
        console.log(`[messenger_http] waiting ${delayMs}ms before retry...`);
        await sleep(delayMs);
      } else {
        console.error('[messenger_http] all retries exhausted');
        enabled = false;
        return false;
      }
    }
  }
  return false;
}

function isEnabled() {
  return enabled;
}

async function getProfile(userId) {
  return apiRequest('getProfile', { userId });
}

async function upsertProfile(userId, patch) {
  return apiRequest('upsertProfile', { userId, patch });
}

async function upsertSettings(userId, privacy) {
  return apiRequest('upsertSettings', { userId, privacy });
}

async function listAllUserIds() {
  return apiRequest('listAllUserIds');
}

async function setUserOnlineFlags(userId, online) {
  return apiRequest('setUserOnlineFlags', { userId, online });
}

async function findDirectChat(a, b) {
  return apiRequest('findDirectChat', { a, b });
}

async function getOrCreateChat(a, b) {
  return apiRequest('getOrCreateChat', { a, b });
}

async function getChatById(chatId) {
  return apiRequest('getChatById', { chatId });
}

async function loadChatMeta(chatId) {
  const chat = await getChatById(chatId);
  return chat?.meta || null;
}

async function listChatsForUser(userId) {
  return apiRequest('listChatsForUser', { userId });
}

async function updateChatMeta(chatId, meta) {
  return apiRequest('updateChatMeta', { chatId, meta });
}

async function updateLastMessagePreview(chatId, lastMessageObj, updatedAt) {
  return apiRequest('updateLastMessagePreview', { chatId, lastMessageObj, updatedAt });
}

async function deleteChatRow(chatId) {
  return apiRequest('deleteChatRow', { chatId });
}

async function insertMessage(msg) {
  return apiRequest('insertMessage', { message: msg });
}

async function listMessagesForChat(chatId, clearedAfterTs = 0, limit = 500) {
  return apiRequest('listMessagesForChat', { chatId, clearedAfterTs, limit });
}

async function getLatestMessageInChatAfter(chatId, clearedAfterTs = 0) {
  return apiRequest('getLatestMessageInChatAfter', { chatId, clearedAfterTs });
}

async function getMessageById(id) {
  return apiRequest('getMessageById', { id });
}

async function deleteMessageByIdHard(id) {
  return apiRequest('deleteMessageByIdHard', { id });
}

async function updateMessageFields(id, patch) {
  return apiRequest('updateMessageFields', { id, patch });
}

async function addMessageReadBy(messageId, readerUserId) {
  return apiRequest('addMessageReadBy', { messageId, readerUserId });
}

module.exports = {
  initMessengerMysql,
  isEnabled,
  getProfile,
  upsertProfile,
  upsertSettings,
  listAllUserIds,
  findDirectChat,
  getOrCreateChat,
  getChatById,
  loadChatMeta,
  updateChatMeta,
  updateLastMessagePreview,
  insertMessage,
  listMessagesForChat,
  getLatestMessageInChatAfter,
  getMessageById,
  deleteMessageByIdHard,
  updateMessageFields,
  listChatsForUser,
  deleteChatRow,
  setUserOnlineFlags,
  directChatId,
  addMessageReadBy
};
