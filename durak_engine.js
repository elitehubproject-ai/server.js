'use strict';

/**
 * Подкидной / переводной дурак, колода 52 карты (как типичный спрайт 13×4).
 * Формат карты: ранг + масть, напр. "2s", "Ah", "Ts" (десятка).
 * Масти: s пики, h червы, d бубны, c трефы
 */

const SUITS = ['s', 'h', 'd', 'c'];
const RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A'];
const RANK_VALUE = Object.fromEntries(RANKS.map((r, i) => [r, i]));

function parseCard(id) {
  const s = String(id || '').trim().toLowerCase();
  if (s.length < 2) return null;
  const suit = s.slice(-1);
  if (!SUITS.includes(suit)) return null;
  const head = s.slice(0, -1);
  let rank;
  if (head === '10' || head === 't') rank = 'T';
  else if (head.length === 1) {
    const c = head;
    if (c === 't') rank = 'T';
    else if (/^[2-9]$/.test(c)) rank = c;
    else if (/^[jqka]$/i.test(c)) rank = c.toUpperCase();
    else return null;
  } else return null;
  if (!RANKS.includes(rank)) return null;
  return { rank, suit, id: `${rank}${suit}` };
}

function makeDeck52() {
  const d = [];
  for (const suit of SUITS) {
    for (const rank of RANKS) {
      d.push(`${rank}${suit}`);
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

function lowestTrumpFirstAttacker(players, hands, trumpSuit) {
  let best = null;
  let bestPid = null;
  for (const pid of players) {
    const hand = hands.get(pid) || [];
    for (const cid of hand) {
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
  let low = null;
  let lowPid = players[0];
  for (const pid of players) {
    for (const cid of hands.get(pid) || []) {
      const c = parseCard(cid);
      if (!c) continue;
      const key = RANK_VALUE[c.rank] * 10 + SUITS.indexOf(c.suit);
      if (low === null || key < low) {
        low = key;
        lowPid = pid;
      }
    }
  }
  return lowPid;
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
    stockEmpty: false,
    firstDealRules: true,
    battlesFinished: 0,
    winnerId: null,
    turnDeadline: 0,
    turnSeconds: 60,
    lobbyDeadline: 0,
    lobbyAutoSec: 60,
    version: 0
  };
}

function nextIndex(n, len) {
  return (n + 1) % len;
}

function prevIndex(n, len) {
  return (n - 1 + len) % len;
}

function removeCardFromHand(hands, pid, cardId) {
  const h = hands.get(pid);
  if (!h) return false;
  const i = h.indexOf(cardId);
  if (i < 0) return false;
  h.splice(i, 1);
  return true;
}

function dealInitial(game) {
  const n = game.players.length;
  if (n < 2 || n > 8) return { ok: false, error: 'Игроков должно быть от 2 до 8' };
  game.deck = shuffle(makeDeck52());
  game.hands = new Map();
  game.players.forEach((pid) => game.hands.set(pid, []));
  for (let round = 0; round < 6; round++) {
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
  const ai = game.players.indexOf(
    lowestTrumpFirstAttacker(game.players, game.hands, game.trump)
  );
  game.attackerIndex = ai >= 0 ? ai : 0;
  game.defenderIndex = nextIndex(game.attackerIndex, n);
  game.phase = 'playing';
  game.battle = {
    table: [],
    subPhase: 'attack',
    attackerPid: game.players[game.attackerIndex],
    defenderPid: game.players[game.defenderIndex],
    tosserQueue: [],
    leadingRank: null
  };
  game.players.forEach((pid) => {
    game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
  });
  game.turnDeadline = Date.now() + game.turnSeconds * 1000;
  game.version++;
  return { ok: true };
}

function refillHands(game) {
  const n = game.players.length;
  const order = [];
  let idx = game.attackerIndex;
  for (let k = 0; k < n; k++) {
    order.push(game.players[idx]);
    idx = nextIndex(idx, n);
  }
  for (const pid of order) {
    const h = game.hands.get(pid) || [];
    while (h.length < 6 && game.deck.length > 0) {
      h.push(game.deck.pop());
    }
    game.hands.set(pid, sortHand(h, game.trump));
  }
  if (game.deck.length === 0) game.stockEmpty = true;
}

function checkGameEnd(game) {
  const active = game.players.filter((pid) => (game.hands.get(pid) || []).length > 0);
  if (active.length <= 1) {
    game.winnerId = active[0] || null;
    game.phase = 'ended';
    game.turnDeadline = 0;
    game.version++;
    return true;
  }
  return false;
}

function startNextBattle(game) {
  const n = game.players.length;
  const prevDef = game.defenderIndex;
  if (!game.stockEmpty) refillHands(game);
  game.battlesFinished++;
  if (game.battlesFinished >= 1) game.firstDealRules = false;
  if (checkGameEnd(game)) return;
  game.attackerIndex = prevDef;
  let dIdx = nextIndex(game.attackerIndex, n);
  while ((game.hands.get(game.players[dIdx]) || []).length === 0) {
    dIdx = nextIndex(dIdx, n);
    if (dIdx === game.attackerIndex) break;
  }
  game.defenderIndex = dIdx;
  game.battle = {
    table: [],
    subPhase: 'attack',
    attackerPid: game.players[game.attackerIndex],
    defenderPid: game.players[game.defenderIndex],
    tosserQueue: [],
    leadingRank: null
  };
  game.turnDeadline = Date.now() + game.turnSeconds * 1000;
  game.version++;
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
    if (row.transferCard) {
      const t = parseCard(row.transferCard);
      if (t) s.add(t.rank);
    }
  }
  return s;
}

function defenderCardCount(game) {
  const dpid = game.players[game.defenderIndex];
  return (game.hands.get(dpid) || []).length;
}

function maxTossAllowed(game) {
  const defCount = defenderCardCount(game);
  return Math.max(0, defCount - game.battle.table.length);
}

function canToss(game, pid) {
  if (game.battle.subPhase !== 'toss') return false;
  if (pid === game.players[game.defenderIndex]) return false;
  if (maxTossAllowed(game) <= 0) return false;
  if (game.firstDealRules && defenderCardCount(game) < 3) return false;
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
    battle: game.battle,
    attackerIndex: game.attackerIndex,
    defenderIndex: game.defenderIndex,
    firstDealRules: game.firstDealRules,
    stockEmpty: game.stockEmpty,
    winnerId: game.winnerId,
    turnDeadline: game.turnDeadline,
    turnSeconds: game.turnSeconds,
    lobbyDeadline: game.lobbyDeadline,
    version: game.version,
    names: { ...(game.playerNames || {}) }
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
  if (game.players.length > 8) return { ok: false, error: 'Максимум 8 игроков (колода 52)' };
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

function applyTake(game) {
  const b = game.battle;
  const dpid = game.players[game.defenderIndex];
  const hand = game.hands.get(dpid) || [];
  for (const row of b.table) {
    hand.push(row.attack);
    if (row.transferCard) hand.push(row.transferCard);
    if (row.defense) hand.push(row.defense);
  }
  game.hands.set(dpid, sortHand(hand, game.trump));
  game.discard.push(...b.table.map((r) => [r.attack, r.defense].filter(Boolean)).flat());
  startNextBattle(game);
  return { ok: true };
}

function applyBito(game) {
  const b = game.battle;
  if (b.table.some((r) => !r.defense)) return { ok: false, error: 'Не всё отбито' };
  for (const row of b.table) {
    game.discard.push(row.attack);
    if (row.transferCard) game.discard.push(row.transferCard);
    if (row.defense) game.discard.push(row.defense);
  }
  startNextBattle(game);
  return { ok: true };
}

function processAction(game, pid, action) {
  const type = action?.type;
  if (game.phase === 'lobby') return { ok: false, error: 'Игра не началась' };
  if (game.phase === 'ended') return { ok: false, error: 'Игра окончена' };

  if (type === 'take') {
    if (game.battle.subPhase !== 'defend' && game.battle.subPhase !== 'toss') {
      return { ok: false, error: 'Сейчас нельзя взять' };
    }
    if (pid !== game.players[game.defenderIndex]) return { ok: false, error: 'Только защитник' };
    return applyTake(game);
  }

  if (type === 'done') {
    if (pid !== game.players[game.defenderIndex]) return { ok: false, error: 'Только защитник' };
    if (game.battle.table.some((r) => !r.defense)) return { ok: false, error: 'Есть неотбитые' };
    if (game.battle.subPhase === 'toss' && canAnyToss(game)) {
      return { ok: false, error: 'Ещё можно подкинуть' };
    }
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
        b.table.push({ attack: cid, defense: null, beatType: 'attack' });
      }
      b.subPhase = 'defend';
      game.turnDeadline = Date.now() + game.turnSeconds * 1000;
      game.version++;
      return { ok: true };
    }

    const cardId = String(rawCards[0] || '').trim();
    if (!parseCard(cardId)) return { ok: false, error: 'Неверная карта' };
    if (!removeCardFromHand(game.hands, pid, cardId)) return { ok: false, error: 'Нет карты' };

    if (b.subPhase === 'defend') {
      if (pid !== defPid) {
        game.hands.get(pid).push(cardId);
        game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
        return { ok: false, error: 'Ход защитника' };
      }
      const undef = b.table.filter((r) => !r.defense);
      if (undef.length === 0) {
        game.hands.get(pid).push(cardId);
        game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
        return { ok: false, error: 'Нечего бить' };
      }
      const target = undef[0];

      if (game.mode === 'perevodnoy' && action.transfer) {
        const tr = parseCard(cardId);
        const lead = parseCard(target.attack);
        if (tr && lead && tr.rank === lead.rank) {
          let ni = nextIndex(game.defenderIndex, n);
          let steps = 0;
          while (steps < n && (game.hands.get(game.players[ni]) || []).length === 0) {
            ni = nextIndex(ni, n);
            steps++;
          }
          const ti = b.table.indexOf(target);
          if (ti >= 0) {
            b.table[ti].transferCard = cardId;
            b.table[ti].beatType = 'transfer';
          }
          game.defenderIndex = ni;
          b.defenderPid = game.players[game.defenderIndex];
          b.subPhase = 'defend';
          game.turnDeadline = Date.now() + game.turnSeconds * 1000;
          game.version++;
          return { ok: true };
        }
      }

      if (!canBeat(target.attack, cardId, game.trump)) {
        game.hands.get(pid).push(cardId);
        game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
        return { ok: false, error: 'Этой картой нельзя побить' };
      }
      target.defense = cardId;
      target.beatType = 'beat';
      const moreUndef = b.table.some((r) => !r.defense);
      if (moreUndef) {
        game.turnDeadline = Date.now() + game.turnSeconds * 1000;
        game.version++;
        return { ok: true };
      }
      b.subPhase = 'toss';
      game.turnDeadline = Date.now() + game.turnSeconds * 1000;
      game.version++;
      return { ok: true };
    }

    if (b.subPhase === 'toss') {
      if (!canToss(game, pid)) {
        game.hands.get(pid).push(cardId);
        game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
        return { ok: false, error: 'Сейчас нельзя подкинуть' };
      }
      const pr = parseCard(cardId);
      const allowedRanks = ranksOnTable(b);
      if (!allowedRanks.has(pr.rank)) {
        game.hands.get(pid).push(cardId);
        game.hands.set(pid, sortHand(game.hands.get(pid), game.trump));
        return { ok: false, error: 'Такой ранг нельзя' };
      }
      b.table.push({ attack: cardId, defense: null, beatType: 'toss' });
      b.subPhase = 'defend';
      game.turnDeadline = Date.now() + game.turnSeconds * 1000;
      game.version++;
      return { ok: true };
    }
  }

  return { ok: false, error: 'Неизвестное действие' };
}

function tickTurnTimer(game) {
  if (game.phase !== 'playing' || !game.turnDeadline) return null;
  if (Date.now() < game.turnDeadline) return null;
  const defPid = game.players[game.defenderIndex];
  if (game.battle.subPhase === 'defend' || game.battle.subPhase === 'toss') {
    return applyTake(game);
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
  exportGamePublic,
  processAction,
  tickTurnTimer,
  lobbyTick,
  parseCard,
  sortHand,
  canAnyToss
};
