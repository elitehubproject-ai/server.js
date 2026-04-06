'use strict';

/**
 * Подкидной / переводной дурак, колода 36 (6…A × 4 масти).
 * Формат карты: "6s", "Th", "As" …
 * Масти: s пики, h черви, d бубны, c трефы
 */

const SUITS = ['s', 'h', 'd', 'c'];
const RANKS = ['6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A'];
const RANK_VALUE = Object.fromEntries(RANKS.map((r, i) => [r, i]));

function parseCard(id) {
  let s = String(id || '').trim().toLowerCase();
  if (!s) return null;
  s = s.replace(/~\d+$/, '');
  if (s.length < 2) return null;
  const suit = s.slice(-1);
  if (!SUITS.includes(suit)) return null;
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
  if (!RANKS.includes(rank)) return null;
  return { rank, suit, id: `${rank}${suit}` };
}

function makeDeck36() {
  const d = [];
  let uid = 0;
  for (const suit of SUITS) {
    for (const rank of RANKS) {
      d.push(`${rank}${suit}~${uid++}`);
    }
  }
  return d;
}

function shuffle(arr, rng = Math.random) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function sortHand(ids, trumpSuit) {
  return [...ids].sort((x, y) => {
    const px = parseCard(x);
    const py = parseCard(y);
    if (!px || !py) return 0;
    const tx = px.suit === trumpSuit ? 1 : 0;
    const ty = py.suit === trumpSuit ? 1 : 0;
    if (tx !== ty) return ty - tx;
    if (px.suit !== py.suit) return SUITS.indexOf(px.suit) - SUITS.indexOf(py.suit);
    return RANK_VALUE[px.rank] - RANK_VALUE[py.rank];
  });
}

/** Обычные правила: та же масть старше, иначе козырь; козырь на козырь — старше по рангу. */
function canBeat(attackId, defendId, trumpSuit) {
  const a = parseCard(attackId);
  const d = parseCard(defendId);
  if (!a || !d) return false;
  const aTrump = a.suit === trumpSuit;
  const dTrump = d.suit === trumpSuit;
  if (aTrump && dTrump) return RANK_VALUE[d.rank] > RANK_VALUE[a.rank];
  if (!aTrump && dTrump) return true;
  if (aTrump && !dTrump) return false;
  if (a.suit === d.suit) return RANK_VALUE[d.rank] > RANK_VALUE[a.rank];
  return false;
}

function lowestTrumpFirstAttacker(players, hands, trumpSuit, rng = Math.random) {
  let best = null;
  let bestPid = null;
  for (const pid of players) {
    for (const cid of hands.get(pid) || []) {
      const c = parseCard(cid);
      if (!c || c.suit !== trumpSuit) continue;
      const val = RANK_VALUE[c.rank];
      if (best === null || val < best || (val === best && players.indexOf(pid) < players.indexOf(bestPid))) {
        best = val;
        bestPid = pid;
      }
    }
  }
  if (bestPid) return bestPid;
  return players[Math.floor(rng() * players.length)] || players[0];
}

function createEmptyGame(mode) {
  return {
    mode: mode === 'perevodnoy' ? 'perevodnoy' : 'podkidnoy',
    phase: 'lobby',
    players: [],
    playerNames: {},
    hands: new Map(),
    deck: [],
    trump: null,
    trumpCard: null,
    discard: [],
    initiatorId: null,
    battle: null,
    attackerIndex: 0,
    defenderIndex: 1,
    handTarget: 6,
    stockEmpty: false,
    firstDealRules: true,
    battlesFinished: 0,
    winnerId: null,
    turnDeadline: 0,
    turnSeconds: 60,
    lobbyDeadline: 0,
    lobbyAutoSec: 60,
    version: 0,
    playSeq: 0,
    lastPlay: null
  };
}

function bumpLastPlay(game, byPid, cardIds, kind) {
  const cards = (cardIds || []).map((c) => String(c || '').trim()).filter(Boolean);
  if (!byPid || !cards.length) return;
  game.playSeq = (game.playSeq || 0) + 1;
  game.lastPlay = { by: byPid, cards, kind: kind || 'play', seq: game.playSeq };
}

function nextIndex(n, len) {
  return (n + 1) % len;
}

/** Цепочка переводов: [{ card, defense }]. Совместимость со старым transferCard. */
function ensureTransferStack(row) {
  if (!row || typeof row !== 'object') return;
  if (!Array.isArray(row.transferStack)) {
    row.transferStack = [];
    if (row.transferCard) {
      row.transferStack.push({ card: row.transferCard, defense: row.transferDefense || null });
    }
  }
}

function getTransfers(row) {
  ensureTransferStack(row);
  return row.transferStack;
}

/** Сколько карт в сумме в неотбитой «куче» на столе, если на targetRow добавить ещё один перевод. */
function totalCardsInAttackPileAfterOneMoreTransfer(battle, targetRow) {
  let n = 0;
  for (const row of battle.table) {
    ensureTransferStack(row);
    const ts = row.transferStack;
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

function removeCardFromHand(hands, pid, cardId) {
  const h = hands.get(pid);
  if (!h) return false;
  const i = h.indexOf(cardId);
  if (i < 0) return false;
  h.splice(i, 1);
  return true;
}

/** Следующий непустой индекс руки, начиная с fromIdx. */
function nextNonEmptyHandIndex(game, fromIdx) {
  const n = game.players.length;
  if (n === 0) return 0;
  let i = fromIdx % n;
  let steps = 0;
  while (steps < n) {
    const pid = game.players[i];
    if ((game.hands.get(pid) || []).length > 0) return i;
    i = nextIndex(i, n);
    steps++;
  }
  return fromIdx % n;
}

function dealInitial(game) {
  const n = game.players.length;
  if (n < 2 || n > 6) return { ok: false, error: 'Игроков от 2 до 6' };
  game.deck = shuffle(makeDeck36());
  game.hands = new Map();
  game.players.forEach((pid) => game.hands.set(pid, []));
  const perHand = n === 6 ? 5 : 6;
  game.handTarget = perHand;
  for (let round = 0; round < perHand; round++) {
    for (let p = 0; p < n; p++) {
      if (game.deck.length === 0) break;
      const pid = game.players[p];
      game.hands.get(pid).push(game.deck.pop());
    }
  }
  if (game.deck.length === 0) return { ok: false, error: 'Не удалось раздать' };
  game.trumpCard = game.deck[0];
  const tc = parseCard(game.trumpCard);
  game.trump = tc ? tc.suit : 's';
  game.stockEmpty = false;
  game.firstDealRules = true;
  game.battlesFinished = 0;
  const firstPid = lowestTrumpFirstAttacker(game.players, game.hands, game.trump);
  game.attackerIndex = Math.max(0, game.players.indexOf(firstPid));
  game.defenderIndex = nextNonEmptyHandIndex(game, nextIndex(game.attackerIndex, n));
  game.phase = 'playing';
  game.battle = {
    table: [],
    subPhase: 'attack',
    attackerPid: game.players[game.attackerIndex],
    defenderPid: game.players[game.defenderIndex],
    maxAttackCards: battleAttackCardsCap(game, game.defenderIndex),
    tosserQueue: [],
    leadingRank: null
  };
  game.lastPlay = null;
  game.players.forEach((pid) => {
    game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
  });
  game.turnDeadline = Date.now() + game.turnSeconds * 1000;
  game.version++;
  return { ok: true };
}

function refillHands(game) {
  const n = game.players.length;
  const limit = game.handTarget || 6;
  const order = [];
  let idx = game.attackerIndex;
  for (let k = 0; k < n; k++) {
    order.push(game.players[idx]);
    idx = nextIndex(idx, n);
  }
  for (const pid of order) {
    const h = game.hands.get(pid) || [];
    while (h.length < limit && game.deck.length > 0) {
      h.push(game.deck.pop());
    }
    game.hands.set(pid, sortHand(h, game.trump));
  }
  if (game.deck.length === 0) game.stockEmpty = true;
}

/** Победитель — выложивший все карты; оставшийся с картами — дурак (для 2 игроков). */
function checkGameEnd(game) {
  const n = game.players.length;
  if (n < 2) return false;
  const active = game.players.filter((pid) => (game.hands.get(pid) || []).length > 0);
  const empty = game.players.filter((pid) => (game.hands.get(pid) || []).length === 0);
  if (active.length <= 1) {
    game.winnerId = empty.length ? empty[empty.length - 1] : active[0] || null;
    game.phase = 'ended';
    game.turnDeadline = 0;
    game.version++;
    return true;
  }
  return false;
}

/**
 * После «беру»: атакует тот, кто вёл атаку в этой схватке; защищается взявший стол.
 * Если переводной перевели на атакующего (в т.ч. 2 игрока), индекс атакующего и защитника совпадают —
 * тогда взявший не ходит первым: атакует другой игрок, взявший снова защищается.
 */
function startNextBattleAfterTake(game, prevAttackerPid, prevDefenderPid) {
  const n = game.players.length;
  if (!game.stockEmpty) refillHands(game);
  game.battlesFinished++;
  if (game.battlesFinished >= 1) game.firstDealRules = false;
  if (checkGameEnd(game)) return;

  let ai;
  let di;
  const takerIdx = Math.max(0, game.players.indexOf(prevDefenderPid));
  if (n <= 2) {
    ai = game.players.indexOf(prevAttackerPid);
    if (ai < 0) ai = 0;
    di = game.players.indexOf(prevDefenderPid);
    if (di < 0) di = nextIndex(ai, n);
  } else {
    ai = nextNonEmptyHandIndex(game, nextIndex(takerIdx, n));
    let guardA = 0;
    while (guardA < n && ai === takerIdx) {
      ai = nextNonEmptyHandIndex(game, nextIndex(ai, n));
      guardA++;
    }
    di = nextNonEmptyHandIndex(game, nextIndex(ai, n));
  }

  if ((game.hands.get(game.players[ai]) || []).length === 0) {
    ai = nextNonEmptyHandIndex(game, ai);
  }
  if ((game.hands.get(game.players[di]) || []).length === 0) {
    di = nextNonEmptyHandIndex(game, nextIndex(ai, n));
  }
  if (di === ai) {
    di = nextNonEmptyHandIndex(game, nextIndex(ai, n));
  }

  game.attackerIndex = ai;
  game.defenderIndex = di;

  game.battle = {
    table: [],
    subPhase: 'attack',
    attackerPid: game.players[game.attackerIndex],
    defenderPid: game.players[game.defenderIndex],
    maxAttackCards: battleAttackCardsCap(game, game.defenderIndex),
    tosserQueue: [],
    leadingRank: null
  };
  game.turnDeadline = Date.now() + game.turnSeconds * 1000;
  game.version++;
}

/** После «бито»: бывший защитник отбивается и становится атакующим. */
function startNextBattleAfterBeat(game, prevDefenderPid) {
  game.lastPlay = null;
  const n = game.players.length;
  if (!game.stockEmpty) refillHands(game);
  game.battlesFinished++;
  if (game.battlesFinished >= 1) game.firstDealRules = false;
  if (checkGameEnd(game)) return;

  let ai = game.players.indexOf(prevDefenderPid);
  if (ai < 0) ai = 0;
  ai = nextNonEmptyHandIndex(game, ai);
  game.attackerIndex = ai;

  let dIdx = nextIndex(game.attackerIndex, n);
  dIdx = nextNonEmptyHandIndex(game, dIdx);
  if (dIdx === game.attackerIndex) {
    dIdx = nextNonEmptyHandIndex(game, nextIndex(dIdx, n));
  }
  game.defenderIndex = dIdx;

  game.battle = {
    table: [],
    subPhase: 'attack',
    attackerPid: game.players[game.attackerIndex],
    defenderPid: game.players[game.defenderIndex],
    maxAttackCards: battleAttackCardsCap(game, game.defenderIndex),
    tosserQueue: [],
    leadingRank: null
  };
  game.turnDeadline = Date.now() + game.turnSeconds * 1000;
  game.version++;
}

/** Кто нажимает «бито»: обычно атакующий по индексу; если после перевода совпал с защитником — другой игрок (не защитник). */
function bitoAuthorityPid(game) {
  const n = game.players.length;
  const atkIdx = game.attackerIndex;
  const defIdx = game.defenderIndex;
  if (atkIdx !== defIdx) return game.players[atkIdx];
  let i = nextNonEmptyHandIndex(game, nextIndex(defIdx, n));
  let guard = 0;
  while (guard < n && i === defIdx) {
    i = nextNonEmptyHandIndex(game, nextIndex(i, n));
    guard++;
  }
  return game.players[i];
}

function ranksOnTable(battle) {
  const s = new Set();
  for (const row of battle.table) {
    const a = parseCard(row.attack);
    if (a) s.add(a.rank);
    if (row.defense) {
      const d = parseCard(row.defense);
      if (d) s.add(d.rank);
    }
    for (const t of getTransfers(row)) {
      const x = parseCard(t.card);
      if (x) s.add(x.rank);
      if (t.defense) {
        const d2 = parseCard(t.defense);
        if (d2) s.add(d2.rank);
      }
    }
  }
  return s;
}

function defenderCardCount(game) {
  const dpid = game.players[game.defenderIndex];
  return (game.hands.get(dpid) || []).length;
}

function battleAttackCardsCap(game, defenderIdx) {
  const pid = game.players[defenderIdx];
  const handN = (game.hands.get(pid) || []).length;
  const hardCap = game.firstDealRules ? 5 : 6;
  return Math.max(1, Math.min(hardCap, handN));
}

function maxTossAllowed(game) {
  const cap = game.battle?.maxAttackCards || Number.MAX_SAFE_INTEGER;
  return Math.max(0, cap - game.battle.table.length);
}

function canToss(game, pid) {
  const sub = game.battle.subPhase;
  if (sub !== 'toss' && sub !== 'take_toss') return false;
  if (sub === 'take_toss') {
    if (pid !== game.players[game.attackerIndex]) return false;
  } else if (pid === game.players[game.defenderIndex]) return false;
  if (maxTossAllowed(game) <= 0) return false;
  return true;
}

function canAnyToss(game) {
  const ranks = ranksOnTable(game.battle);
  for (const pid of game.players) {
    if (!canToss(game, pid)) continue;
    for (const c of game.hands.get(pid) || []) {
      const p = parseCard(c);
      if (p && ranks.has(p.rank)) return true;
    }
  }
  return false;
}

/** Есть ли на столе неотбитые атака или карты перевода (порядок отбоя — любой). */
function hasUnbeatenDefenseTargets(battle) {
  for (const row of battle.table) {
    ensureTransferStack(row);
    if (!row.defense) return true;
    for (const t of row.transferStack) {
      if (!t.defense) return true;
    }
  }
  return false;
}

function hasAnyDefenseOnTable(battle) {
  if (!battle || !Array.isArray(battle.table)) return false;
  for (const row of battle.table) {
    ensureTransferStack(row);
    if (row.defense) return true;
    for (const t of row.transferStack) {
      if (t.defense) return true;
    }
  }
  return false;
}

/** Все слоты, куда можно положить отбой данной картой (атака и переводы — в любом порядке). */
function enumerateBeatTargets(battle, defendCardId, trumpSuit) {
  const out = [];
  for (const row of battle.table) {
    ensureTransferStack(row);
    if (!row.defense && canBeat(row.attack, defendCardId, trumpSuit)) {
      out.push({ row, beatAttack: true, anchor: row.attack, transferIndex: null });
    }
    const ts = row.transferStack;
    for (let i = 0; i < ts.length; i++) {
      const t = ts[i];
      if (!t.defense && canBeat(t.card, defendCardId, trumpSuit)) {
        out.push({ row, beatAttack: false, anchor: t.card, transferIndex: i });
      }
    }
  }
  return out;
}

/** Перевод по непобитой атаке (цепочка: можно снова перевести тем же достоинством). */
function canTransferOnRow(game, row, cardId) {
  if (game.mode !== 'perevodnoy') return false;
  if (hasAnyDefenseOnTable(game.battle)) return false;
  if (row.defense) return false;
  if (row.beatType === 'toss') return false;
  const tr = parseCard(cardId);
  const lead = parseCard(row.attack);
  return !!(tr && lead && tr.rank === lead.rank);
}

function resolveNextDefenderIndex(game) {
  const n = game.players.length;
  let ni = nextIndex(game.defenderIndex, n);
  let steps = 0;
  while (steps < n && (game.hands.get(game.players[ni]) || []).length === 0) {
    ni = nextIndex(ni, n);
    steps++;
  }
  return ni;
}

/**
 * Логика защиты: against = id карты на столе (атака или карта перевода), которую бьём/переводим.
 * target 'beat' | 'transfer' при неоднозначности (та же достоинство + можно и побить).
 */
function tryDefendPlay(game, cardId, action) {
  const b = game.battle;
  const against = String(action.against || action.attackCard || '').trim();
  const playTarget =
    action.target === 'beat' ? 'beat' : action.target === 'transfer' ? 'transfer' : action.transfer ? 'transfer' : null;

  const beats = enumerateBeatTargets(b, cardId, game.trump);
  const xferRows = b.table.filter((row) => canTransferOnRow(game, row, cardId));

  const pickBeat = () => {
    if (against) {
      return beats.find((t) => t.anchor === against) || null;
    }
    if (beats.length === 1) return beats[0];
    return null;
  };

  const pickTransferRow = () => {
    if (against) {
      const row = b.table.find((r) => r.attack === against);
      if (row && canTransferOnRow(game, row, cardId)) return row;
      return null;
    }
    if (xferRows.length === 1) return xferRows[0];
    return null;
  };

  const transferAllowedForRow = (row) => {
    const ni = resolveNextDefenderIndex(game);
    const need = totalCardsInAttackPileAfterOneMoreTransfer(b, row);
    const cap = b.maxAttackCards || Number.MAX_SAFE_INTEGER;
    if (need > cap) {
      return { ok: false, error: 'Лимит карт в этом отбое исчерпан' };
    }
    if ((game.hands.get(game.players[ni]) || []).length < need) {
      return { ok: false, error: 'У защитника мало карт — перевод невозможен' };
    }
    return { ok: true, ni };
  };

  if (playTarget === 'transfer') {
    let row = pickTransferRow();
    if (!row && against) {
      const tr = parseCard(cardId);
      const byRank = xferRows.filter((r) => {
        const lead = parseCard(r.attack);
        return tr && lead && tr.rank === lead.rank;
      });
      if (byRank.length === 1) row = byRank[0];
    }
    if (!row && xferRows.length === 1) row = xferRows[0];
    if (!row && !against) {
      const tr = parseCard(cardId);
      const byRank = xferRows.filter((r) => {
        const lead = parseCard(r.attack);
        return tr && lead && tr.rank === lead.rank;
      });
      if (byRank.length === 1) row = byRank[0];
    }
    if (!row) {
      return { ok: false, error: against ? 'Нельзя перевести эту атаку' : 'Укажите карту для перевода' };
    }
    /* Явный target=transfer (слот перевода): разрешаем, даже если той же картой можно было бы побить (напр. козырь). */
    const chk = transferAllowedForRow(row);
    if (!chk.ok) return chk;
    return { ok: true, kind: 'transfer', row };
  }

  if (playTarget === 'beat') {
    const hit = pickBeat();
    if (!hit) {
      return { ok: false, error: against ? 'Нельзя побить эту карту' : 'Укажите, какую карту бьёте' };
    }
    return {
      ok: true,
      kind: 'beat',
      row: hit.row,
      beatAttack: hit.beatAttack,
      transferIndex: hit.transferIndex == null ? null : hit.transferIndex
    };
  }

  const xfer = pickTransferRow();
  const hit = pickBeat();

  if (xfer && hit && xfer === hit.row) {
    const amb =
      parseCard(cardId) &&
      parseCard(xfer.attack) &&
      parseCard(cardId).rank === parseCard(xfer.attack).rank &&
      canBeat(xfer.attack, cardId, game.trump);
    if (amb) return { ok: false, error: 'Перетащите в «Побить» или «Перевод»' };
  }

  if (xfer && hit && xfer !== hit.row && !against) {
    return { ok: false, error: 'Укажите карту на столе' };
  }

  if (against) {
    const rowByAttack = b.table.find((r) => r.attack === against);
    if (rowByAttack && canTransferOnRow(game, rowByAttack, cardId)) {
      const hitA = beats.find((t) => t.anchor === against);
      if (hitA && playTarget !== 'transfer' && !action.transfer) {
        return { ok: false, error: 'Выберите «Побить» или «Перевод»' };
      }
      const chk = transferAllowedForRow(rowByAttack);
      if (!chk.ok) return chk;
      return { ok: true, kind: 'transfer', row: rowByAttack };
    }
    const hitA = beats.find((t) => t.anchor === against);
    if (hitA) {
      return {
        ok: true,
        kind: 'beat',
        row: hitA.row,
        beatAttack: hitA.beatAttack,
        transferIndex: hitA.transferIndex == null ? null : hitA.transferIndex
      };
    }
    return { ok: false, error: 'Нельзя сходить на эту карту' };
  }

  if (xfer && !hit) {
    const chk = transferAllowedForRow(xfer);
    if (!chk.ok) return chk;
    return { ok: true, kind: 'transfer', row: xfer };
  }

  if (hit && !xfer) {
    return {
      ok: true,
      kind: 'beat',
      row: hit.row,
      beatAttack: hit.beatAttack,
      transferIndex: hit.transferIndex == null ? null : hit.transferIndex
    };
  }

  if (!xfer && !hit) {
    if (beats.length > 1) return { ok: false, error: 'Укажите, какую карту бьёте' };
    if (xferRows.length > 1) return { ok: false, error: 'Укажите карту для перевода' };
    return { ok: false, error: 'Невозможный ход' };
  }

  return { ok: false, error: 'Укажите карту на столе' };
}

function pushTableCardsToHand(hand, row) {
  ensureTransferStack(row);
  hand.push(row.attack);
  if (row.defense) hand.push(row.defense);
  for (const t of row.transferStack) {
    hand.push(t.card);
    if (t.defense) hand.push(t.defense);
  }
}

function pushTableCardsToDiscard(game, row) {
  ensureTransferStack(row);
  game.discard.push(row.attack);
  if (row.defense) game.discard.push(row.defense);
  for (const t of row.transferStack) {
    game.discard.push(t.card);
    if (t.defense) game.discard.push(t.defense);
  }
}

function exportGamePublic(game, viewerId) {
  const out = {
    mode: game.mode,
    phase: game.phase,
    initiatorId: game.initiatorId || '',
    players: game.players.map((id) => ({
      id,
      name: game.playerNames[id] || id,
      cardCount: (game.hands.get(id) || []).length
    })),
    trump: game.trump,
    trumpCard: game.trumpCard,
    deckCount: game.deck.length,
    handTarget: game.handTarget || 6,
    battle: game.battle,
    attackerIndex: game.attackerIndex,
    defenderIndex: game.defenderIndex,
    firstDealRules: game.firstDealRules,
    battlesFinished: game.battlesFinished,
    stockEmpty: game.stockEmpty,
    winnerId: game.winnerId,
    turnDeadline: game.turnDeadline,
    turnSeconds: game.turnSeconds,
    lobbyDeadline: game.lobbyDeadline,
    version: game.version,
    names: { ...(game.playerNames || {}) },
    playSeq: game.playSeq || 0,
    lastPlay: game.lastPlay
      ? {
          by: game.lastPlay.by,
          cards: [...game.lastPlay.cards],
          kind: game.lastPlay.kind,
          seq: game.lastPlay.seq
        }
      : null
  };
  if (viewerId && game.hands.has(viewerId)) {
    out.myHand = [...(game.hands.get(viewerId) || [])];
  }
  return out;
}

function createLobby(initiatorId, initiatorName, mode) {
  const g = createEmptyGame(mode);
  g.phase = 'lobby';
  g.initiatorId = initiatorId;
  g.playerNames[initiatorId] = String(initiatorName || 'Игрок').slice(0, 40);
  g.players = [initiatorId];
  g.lobbyDeadline = Date.now() + g.lobbyAutoSec * 1000;
  g.version = 1;
  return g;
}

function lobbyJoin(game, pid, name) {
  if (game.phase !== 'lobby') return { ok: false, error: 'Не лобби' };
  if (game.players.length > 6) return { ok: false, error: 'Максимум 6 игроков' };
  if (!game.players.includes(pid)) {
    game.players.push(pid);
    game.playerNames[pid] = String(name || 'Игрок').slice(0, 40);
  }
  if (game.players.length >= 2 && !game.lobbyDeadline) {
    game.lobbyDeadline = Date.now() + game.lobbyAutoSec * 1000;
  }
  game.version++;
  return { ok: true };
}

function lobbyLeave(game, pid) {
  if (game.phase !== 'lobby') return { ok: false, error: 'Не лобби' };
  game.players = game.players.filter((x) => x !== pid);
  delete game.playerNames[pid];
  if (game.players.length === 0) return { ok: true, empty: true };
  if (game.initiatorId === pid) game.initiatorId = game.players[0];
  game.version++;
  return { ok: true };
}

function tryStartGame(game, pid, force, canForceStart) {
  if (game.phase !== 'lobby') return { ok: false, error: 'Уже не лобби' };
  if (!game.players.includes(pid)) return { ok: false, error: 'Не в лобби' };
  if (game.players.length < 2) return { ok: false, error: 'Нужно минимум 2 игрока' };
  if (force && !canForceStart) return { ok: false, error: 'Нет прав на принудительный старт' };
  return dealInitial(game);
}

function cancelLobby(game, pid, isRoomOwner, isAdmin) {
  if (game.phase !== 'lobby') return { ok: false, error: 'Не лобби' };
  const ok = pid === game.initiatorId || isRoomOwner || isAdmin;
  if (!ok) return { ok: false, error: 'Нет прав' };
  return { ok: true, cancelled: true };
}

function endGameByModerator(game) {
  game.phase = 'ended';
  game.winnerId = null;
  game.turnDeadline = 0;
  game.version++;
  return { ok: true };
}

function collectTableCardIds(battle) {
  const out = [];
  if (!battle || !Array.isArray(battle.table)) return out;
  for (const row of battle.table) {
    ensureTransferStack(row);
    if (row.attack) out.push(row.attack);
    if (row.defense) out.push(row.defense);
    for (const t of row.transferStack) {
      if (t.card) out.push(t.card);
      if (t.defense) out.push(t.defense);
    }
  }
  return out;
}

function beginTakeToss(game) {
  const b = game.battle;
  const dpid = game.players[game.defenderIndex];
  b.subPhase = 'take_toss';
  b.takePid = dpid;
  b.attackerPid = game.players[game.attackerIndex];
  b.defenderPid = dpid;
  if (maxTossAllowed(game) <= 0 || !canAnyToss(game)) {
    return applyTake(game);
  }
  game.turnDeadline = Date.now() + 10000;
  game.version++;
  return { ok: true };
}

function applyTake(game) {
  const b = game.battle;
  const dpid = b.takePid || game.players[game.defenderIndex];
  const prevAttackerPid = game.players[game.attackerIndex];
  const prevDefenderPid = dpid;
  const takenIds = collectTableCardIds(b);
  const hand = game.hands.get(dpid) || [];
  for (const row of b.table) {
    pushTableCardsToHand(hand, row);
  }
  game.hands.set(dpid, sortHand(hand, game.trump));
  b.takePid = null;
  startNextBattleAfterTake(game, prevAttackerPid, prevDefenderPid);
  if (takenIds.length) {
    bumpLastPlay(game, dpid, takenIds, 'take');
  }
  return { ok: true };
}

function rowFullyDefendedForBito(row) {
  ensureTransferStack(row);
  if (!row.defense) return false;
  return row.transferStack.every((t) => t.defense);
}

function applyBito(game) {
  const b = game.battle;
  if (b.table.some((r) => !rowFullyDefendedForBito(r))) return { ok: false, error: 'Не всё отбито' };
  const prevDefenderPid = game.players[game.defenderIndex];
  for (const row of b.table) {
    pushTableCardsToDiscard(game, row);
  }
  startNextBattleAfterBeat(game, prevDefenderPid);
  return { ok: true };
}

/** Меньше двух игроков в партии — нельзя продолжать (избегаем «игры с собой»). */
function endGameTooFewPlayers(game) {
  game.phase = 'ended';
  game.winnerId = game.players.length === 1 ? game.players[0] : null;
  game.turnDeadline = 0;
  game.lastPlay = null;
  if (game.battle) {
    game.battle.table = [];
    game.battle.subPhase = 'attack';
  }
  game.version++;
}

/** Выход из партии без завершения для всех. */
function playingLeave(game, pid) {
  if (game.phase !== 'playing') return { ok: false, error: 'Не идёт игра' };
  if (!game.players.includes(pid)) return { ok: false, error: 'Не в игре' };

  const hand = game.hands.get(pid) || [];
  for (const c of hand) game.discard.push(c);
  game.hands.delete(pid);
  delete game.playerNames[pid];

  const oldAttPid = game.players[game.attackerIndex];
  const oldDefPid = game.players[game.defenderIndex];

  game.players = game.players.filter((x) => x !== pid);

  if (game.players.length === 0) {
    game.phase = 'ended';
    game.winnerId = null;
    game.turnDeadline = 0;
    game.version++;
    return { ok: true, empty: true };
  }

  if (game.players.length < 2) {
    endGameTooFewPlayers(game);
    return { ok: true };
  }

  if (game.initiatorId === pid) game.initiatorId = game.players[0];

  if (game.battle && game.battle.table.length) {
    for (const row of game.battle.table) {
      pushTableCardsToDiscard(game, row);
    }
    game.battle.table = [];
    game.battle.subPhase = 'attack';
    game.lastPlay = null;
  }

  let ai = game.players.indexOf(oldAttPid);
  if (ai < 0) ai = 0;
  ai = nextNonEmptyHandIndex(game, ai);
  game.attackerIndex = ai;

  let di = game.players.indexOf(oldDefPid);
  if (di < 0 || di === ai) di = nextIndex(ai, game.players.length);
  di = nextNonEmptyHandIndex(game, di);
  if (di === ai) di = nextNonEmptyHandIndex(game, nextIndex(di, game.players.length));
  game.defenderIndex = di;

  game.battle.attackerPid = game.players[game.attackerIndex];
  game.battle.defenderPid = game.players[game.defenderIndex];
  game.turnDeadline = Date.now() + game.turnSeconds * 1000;
  checkGameEnd(game);
  game.version++;
  return { ok: true };
}

function processAction(game, pid, action) {
  const type = action?.type;
  if (game.phase === 'lobby') return { ok: false, error: 'Игра не началась' };
  if (game.phase === 'ended') return { ok: false, error: 'Игра окончена' };
  if (game.phase === 'playing' && game.players.length < 2) {
    endGameTooFewPlayers(game);
    return { ok: false, error: 'Игра завершена: недостаточно игроков' };
  }

  if (type === 'take') {
    if (game.battle.subPhase !== 'defend') {
      return { ok: false, error: 'Брать можно только пока отбиваетесь' };
    }
    if (pid !== game.players[game.defenderIndex]) return { ok: false, error: 'Только защитник' };
    return beginTakeToss(game);
  }

  if (type === 'done') {
    if (game.battle.subPhase === 'take_toss') {
      if (pid !== game.players[game.attackerIndex]) return { ok: false, error: 'Только атакующий нажимает бито' };
      return applyTake(game);
    }
    if (pid !== bitoAuthorityPid(game)) return { ok: false, error: 'Только атакующий нажимает бито' };
    if (game.battle.subPhase !== 'toss') return { ok: false, error: 'Сначала отбейтесь' };
    if (game.battle.table.some((r) => !rowFullyDefendedForBito(r))) return { ok: false, error: 'Есть неотбитые' };
    return applyBito(game);
  }

  if (type === 'play') {
    const rawCards = Array.isArray(action.cards) && action.cards.length ? action.cards : action.card ? [action.card] : [];
    if (!rawCards.length) return { ok: false, error: 'Нет карт' };

    const b = game.battle;
    const n = game.players.length;
    const defPid = game.players[game.defenderIndex];
    const attPid = game.players[game.attackerIndex];

    if (b.subPhase === 'attack') {
      if (pid !== attPid) return { ok: false, error: 'Ход атакующего' };
      const cardIds = rawCards.map((c) => String(c || '').trim()).filter(Boolean);
      const cap = b.maxAttackCards || Number.MAX_SAFE_INTEGER;
      if (cardIds.length > cap) return { ok: false, error: 'Превышен лимит карт в отбое' };
      const parsed = cardIds.map((id) => parseCard(id)).filter(Boolean);
      if (parsed.length !== cardIds.length) return { ok: false, error: 'Неверная карта' };
      const rank0 = parsed[0].rank;
      if (!parsed.every((p) => p.rank === rank0)) return { ok: false, error: 'Одинаковый ранг' };
      const removed = [];
      for (const cid of cardIds) {
        if (!removeCardFromHand(game.hands, pid, cid)) {
          for (const r of removed) game.hands.get(pid).push(r);
          game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
          return { ok: false, error: 'Нет карты' };
        }
        removed.push(cid);
      }
      b.leadingRank = rank0;
      for (const cid of cardIds) {
        b.table.push({ attack: cid, defense: null, beatType: 'attack', transferStack: [] });
      }
      b.subPhase = 'defend';
      game.turnDeadline = Date.now() + game.turnSeconds * 1000;
      game.version++;
      bumpLastPlay(game, pid, removed, 'attack');
      checkGameEnd(game);
      return { ok: true };
    }

    const cardIds = rawCards.map((c) => String(c || '').trim()).filter(Boolean);
    if (!cardIds.length) return { ok: false, error: 'Нет карт' };
    const cardId = cardIds[0];
    if (!parseCard(cardId)) return { ok: false, error: 'Неверная карта' };

    if (b.subPhase === 'defend') {
      if (pid !== defPid) return { ok: false, error: 'Ход защитника' };

      const plan = tryDefendPlay(game, cardId, action);
      if (!plan.ok) return { ok: false, error: plan.error };

      if (!removeCardFromHand(game.hands, pid, cardId)) return { ok: false, error: 'Нет карты' };

      const applyTransfer = (row) => {
        ensureTransferStack(row);
        row.transferStack.push({ card: cardId, defense: null });
        row.beatType = 'transfer';
        let ni = nextIndex(game.defenderIndex, n);
        let steps = 0;
        while (steps < n && (game.hands.get(game.players[ni]) || []).length === 0) {
          ni = nextIndex(ni, n);
          steps++;
        }
        game.defenderIndex = ni;
        b.defenderPid = game.players[game.defenderIndex];
        b.attackerPid = game.players[game.attackerIndex];
        b.subPhase = 'defend';
        game.turnDeadline = Date.now() + game.turnSeconds * 1000;
        game.version++;
      };

      const applyBeat = (row, beatAttack, transferIdx) => {
        ensureTransferStack(row);
        const attackCard = beatAttack ? row.attack : row.transferStack[transferIdx].card;
        if (!canBeat(attackCard, cardId, game.trump)) {
          game.hands.get(pid).push(cardId);
          game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
          return { ok: false, error: 'Этой картой нельзя побить' };
        }
        if (beatAttack) {
          row.defense = cardId;
          row.beatType = row.transferStack.length > 0 ? 'transfer' : 'beat';
        } else {
          row.transferStack[transferIdx].defense = cardId;
          row.beatType = 'beat';
        }
        const still = hasUnbeatenDefenseTargets(b);
        b.attackerPid = game.players[game.attackerIndex];
        if (still) {
          game.turnDeadline = Date.now() + game.turnSeconds * 1000;
          game.version++;
          return { ok: true };
        }
        b.subPhase = 'toss';
        b.attackerPid = bitoAuthorityPid(game);
        game.turnDeadline = Date.now() + game.turnSeconds * 1000;
        game.version++;
        return { ok: true };
      };

      if (plan.kind === 'transfer') {
        applyTransfer(plan.row);
        bumpLastPlay(game, pid, [cardId], 'transfer');
        checkGameEnd(game);
        return { ok: true };
      }
      const tidx = plan.beatAttack ? 0 : plan.transferIndex;
      const br = applyBeat(plan.row, plan.beatAttack, tidx);
      if (br && br.ok) {
        bumpLastPlay(game, pid, [cardId], 'beat');
        checkGameEnd(game);
      }
      return br;
    }

    if (b.subPhase === 'toss' || b.subPhase === 'take_toss') {
      if (cardIds.some((cid) => !parseCard(cid))) return { ok: false, error: 'Неверная карта' };
      if (!canToss(game, pid)) {
        return { ok: false, error: 'Сейчас нельзя подкинуть' };
      }
      const allowedRanks = ranksOnTable(b);
      for (const cid of cardIds) {
        const pr = parseCard(cid);
        if (!pr || !allowedRanks.has(pr.rank)) return { ok: false, error: 'Такой ранг нельзя' };
      }
      const cap = b.maxAttackCards || Number.MAX_SAFE_INTEGER;
      if (b.table.length + cardIds.length > cap) {
        return { ok: false, error: 'Превышен лимит карт в отбое' };
      }
      const removed = [];
      for (const cid of cardIds) {
        if (!removeCardFromHand(game.hands, pid, cid)) {
          for (const r of removed) game.hands.get(pid).push(r);
          game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
          return { ok: false, error: 'Нет карты' };
        }
        removed.push(cid);
      }
      for (const cid of cardIds) {
        b.table.push({
          attack: cid,
          defense: null,
          beatType: 'toss',
          transferStack: []
        });
      }
      if (b.subPhase === 'take_toss') {
        b.subPhase = 'take_toss';
        b.attackerPid = game.players[game.attackerIndex];
        b.defenderPid = b.takePid || game.players[game.defenderIndex];
        game.turnDeadline = Date.now() + 10000;
      } else {
        b.subPhase = 'defend';
        game.turnDeadline = Date.now() + game.turnSeconds * 1000;
      }
      game.version++;
      bumpLastPlay(game, pid, removed, b.subPhase === 'take_toss' ? 'take_toss' : 'toss');
      checkGameEnd(game);
      return { ok: true };
    }

    if (!removeCardFromHand(game.hands, pid, cardId)) return { ok: false, error: 'Нет карты' };
  }

  return { ok: false, error: 'Неизвестное действие' };
}

function tickTurnTimer(game) {
  if (game.phase !== 'playing') return null;
  if (game.players.length < 2) {
    endGameTooFewPlayers(game);
    return { ok: true, ended: true };
  }
  if (!game.turnDeadline) return null;
  if (Date.now() < game.turnDeadline) return null;
  const b = game.battle;
  if (b.subPhase === 'defend') {
    return beginTakeToss(game);
  }
  if (b.subPhase === 'take_toss') {
    return applyTake(game);
  }
  if (b.subPhase === 'toss') {
    return applyBito(game);
  }
  game.turnDeadline = Date.now() + game.turnSeconds * 1000;
  game.version++;
  return { ok: true, auto: true };
}

function lobbyTick(game) {
  if (game.phase !== 'lobby') return null;
  if (game.players.length < 2) return null;
  if (!game.lobbyDeadline || Date.now() < game.lobbyDeadline) return null;
  return dealInitial(game);
}

module.exports = {
  createLobby,
  lobbyJoin,
  lobbyLeave,
  tryStartGame,
  cancelLobby,
  endGameByModerator,
  playingLeave,
  exportGamePublic,
  processAction,
  tickTurnTimer,
  lobbyTick,
  parseCard,
  sortHand,
  canAnyToss
};
