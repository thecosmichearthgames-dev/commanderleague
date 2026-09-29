import { put, list, del } from '@vercel/blob';

// Season-namespaced storage. Season 4 data stays untouched under its old prefix,
// so starting a new season never requires deleting anything.
const SEASON = 's5';
const PREFIX = `seasons/${SEASON}/players/`;
const keyFor = slug => `${PREFIX}${slug}.json`;

// Bounded parallelism so a large league doesn't fire 100+ simultaneous fetches
// from a single serverless function.
async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const idx = next++;
      if (idx >= items.length) return;
      out[idx] = await fn(items[idx]);
    }
  });
  await Promise.all(workers);
  return out;
}

// A transient network blip shouldn't surface to a player as a failed save.
async function withRetry(fn, tries = 3) {
  let lastErr;
  for (let attempt = 0; attempt < tries; attempt++) {
    try { return await fn(); }
    catch (e) {
      lastErr = e;
      if (attempt === tries - 1) break;
      await new Promise(r => setTimeout(r, 200 * Math.pow(2, attempt)));
    }
  }
  throw lastErr;
}

async function listAll(prefix) {
  const blobs = [];
  let cursor;
  do {
    const res = await withRetry(() => list({ prefix, cursor, limit: 1000 }));
    blobs.push(...res.blobs);
    cursor = res.hasMore ? res.cursor : undefined;
  } while (cursor);
  return blobs;
}

async function readAll() {
  const players = {};
  const submissions = {};
  const blobs = await listAll(PREFIX);
  const records = await mapLimit(blobs, 12, async b => {
    try {
      const r = await withRetry(() => fetch(b.url, { cache: 'no-store' }), 2);
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  });
  for (const rec of records) {
    if (!rec || !rec.slug) continue;
    players[rec.slug] = { name: rec.name, location: rec.location, deck: rec.deck };
    submissions[rec.slug] = rec.weeks || {};
  }
  return { players, submissions };
}

async function readOne(slug) {
  const { blobs } = await withRetry(() => list({ prefix: keyFor(slug), limit: 1 }));
  if (!blobs.length) return null;
  try {
    const r = await withRetry(() => fetch(blobs[0].url, { cache: 'no-store' }), 2);
    if (!r.ok) return null;
    return await r.json();
  } catch (e) { return null; }
}

async function writeOne(rec) {
  return withRetry(() => put(keyFor(rec.slug), JSON.stringify(rec), {
    access: 'public',
    contentType: 'application/json',
    allowOverwrite: true,
    addRandomSuffix: false
  }));
}

export default async function handler(req, res) {
  try {
    if (req.method === 'GET') {
      // Brief edge cache: a room full of players opening the page at once
      // collapses into a couple of origin reads instead of one per person.
      res.setHeader('Cache-Control', 'public, s-maxage=20, stale-while-revalidate=120');
      return res.status(200).json(await readAll());
    }

    if (req.method === 'POST') {
      res.setHeader('Cache-Control', 'no-store');
      const { action, payload } = req.body || {};

      if (action === 'submit') {
        const { slug, week } = payload;
        if (!slug || !week) return res.status(400).json({ error: 'missing player or week' });
        // Read-modify-write touches ONLY this player's own file, so two players
        // saving at the same instant can never overwrite each other.
        const rec = (await readOne(slug)) || { slug, weeks: {} };
        rec.name = payload.name;
        rec.location = payload.location;
        rec.deck = payload.deck;
        rec.weeks = rec.weeks || {};
        rec.weeks[week] = payload;
        await writeOne(rec);
        return res.status(200).json({ ok: true });
      }

      if (action === 'delete') {
        const { slug, week } = payload;
        const rec = await readOne(slug);
        if (rec && rec.weeks) { delete rec.weeks[week]; await writeOne(rec); }
        return res.status(200).json({ ok: true });
      }

      if (action === 'rename') {
        const { oldSlug, newSlug, newName } = payload;
        const oldRec = await readOne(oldSlug);
        if (!oldRec) return res.status(200).json({ ok: true, migrated: 0 });
        const newRec = (await readOne(newSlug)) || { slug: newSlug, weeks: {} };
        newRec.name = newName;
        newRec.location = newRec.location || oldRec.location;
        newRec.deck = newRec.deck || oldRec.deck;
        newRec.weeks = newRec.weeks || {};
        let migrated = 0;
        for (const w of Object.keys(oldRec.weeks || {})) {
          const e = oldRec.weeks[w];
          e.name = newName;
          newRec.weeks[w] = e;
          migrated++;
        }
        await writeOne(newRec);
        if (newSlug !== oldSlug) {
          const { blobs } = await list({ prefix: keyFor(oldSlug), limit: 1 });
          if (blobs.length) await del(blobs[0].url);
        }
        return res.status(200).json({ ok: true, migrated });
      }

      if (action === 'reset') {
        // Scoped to this season's prefix only.
        const blobs = await listAll(PREFIX);
        for (let i = 0; i < blobs.length; i += 100) {
          await del(blobs.slice(i, i + 100).map(b => b.url));
        }
        return res.status(200).json({ ok: true, cleared: blobs.length });
      }

      return res.status(400).json({ error: 'unknown action' });
    }

    return res.status(405).json({ error: 'method not allowed' });
  } catch (e) {
    console.error('[league api]', e);
    return res.status(500).json({ error: String(e && e.message ? e.message : e) });
  }
}
