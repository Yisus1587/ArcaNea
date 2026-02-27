export const normalizeSearchText = (input: string): string => {
  try {
    let s = String(input || '').toLowerCase().trim();
    if (!s) return '';
    try {
      s = s.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    } catch {
      // ignore normalize failure
    }
    s = s.replace(/[^a-z0-9\s]/g, ' ');
    s = s.replace(/\s+/g, ' ').trim();
    return s;
  } catch {
    return '';
  }
};

export const fuzzyScore = (query: string, target: string): number => {
  try {
    const q = normalizeSearchText(query);
    const t = normalizeSearchText(target);
    if (!q || !t) return 0;
    if (q === t) return 1;
    if (t.startsWith(q)) return 0.9;
    if (t.includes(q)) return 0.8;
    const qTokens = q.split(' ').filter(Boolean);
    const tTokens = t.split(' ').filter(Boolean);
    if (!qTokens.length || !tTokens.length) return 0;
    let hit = 0;
    for (const qt of qTokens) {
      if (tTokens.some((tt) => tt.startsWith(qt) || qt.startsWith(tt))) hit += 1;
    }
    let score = hit / Math.max(qTokens.length, tTokens.length);
    if (score < 0.45) {
      let i = 0;
      for (const ch of t) {
        if (i < q.length && ch === q[i]) i += 1;
      }
      score = Math.max(score, Math.min(0.6, i / q.length));
    }
    return score;
  } catch {
    return 0;
  }
};
