// Rate-limit-aware GitHub REST client shared by every probe and scan that
// talks to api.github.com with GITHUB_TOKEN.
//
// Why it exists: the probes used to fire at api.github.com as fast as the
// event loop allowed. probe-readmes alone issued ~60 requests a second and
// its own header admitted that trips GitHub's secondary limit; probe-stars
// ran 10-wide with no 403 handling at all. The quota belongs to the GitHub
// App installation, is shared by every workflow in the repository, and an
// Actions token is capped near 1000 requests/hour/repository — one nightly
// sweep cost 5-8x that. From 2026-09-22 the deploy step, the run's only
// Pages write, was refused every night with "API rate limit exceeded for
// installation" (403), and because a failed job skips its cache post-step
// the stale cache made the next run re-burn the same requests. Seven
// deployments were lost before this file existed.
//
// The shape of the fix: every request passes through one shared token
// bucket, so a sweep spans minutes instead of seconds; 403/429 sleeps on
// retry-after or until x-ratelimit-reset, within a cap; and 404 — a genuine
// answer — never retries. Callers stay dumb: they ask for a path and get
// either data or a status.
//
//   import { ghGet, ghRaw } from './lib/gh-client.mjs'
//
//   ghGet('/repos/o/r')       -> parsed JSON, or throws GhError with .status
//   ghRaw('/repos/o/r', opts) -> { status, body } after any needed retries
//   quotaState()              -> { remaining, resetInMs } last seen values
//
// Knobs: GH_MIN_INTERVAL_MS, GH_CONCURRENCY, GH_MAX_WAIT_MS,
// GH_MAX_ATTEMPTS, GH_TIMEOUT_MS. GITHUB_TOKEN is read at import.

// GH_API_BASE exists so the retry paths can be exercised against a local mock;
// CI and local runs both talk to the real thing.
const API = process.env.GH_API_BASE ?? 'https://api.github.com'
const MIN_INTERVAL_MS = Number(process.env.GH_MIN_INTERVAL_MS ?? 125) // ~8 req/s
const MAX_INFLIGHT = Number(process.env.GH_CONCURRENCY ?? 4)
const MAX_WAIT_MS = Number(process.env.GH_MAX_WAIT_MS ?? 15 * 60 * 1000)
const MAX_ATTEMPTS = Number(process.env.GH_MAX_ATTEMPTS ?? 8)
const TIMEOUT_MS = Number(process.env.GH_TIMEOUT_MS ?? 15000)
const BACKOFF_BASE_MS = Number(process.env.GH_BACKOFF_MS ?? 2000)
const TOKEN = process.env.GITHUB_TOKEN
const USER_AGENT = 'awesome-dsh-plugin-probe'

export class GhError extends Error {
  constructor(status, message = `HTTP ${status}`) {
    super(message)
    this.name = 'GhError'
    this.status = status
  }
}

// 400/404/410/422 are answers: a malformed path or a missing README will not
// become readable by trying harder, and every retry spends the quota the
// pacing exists to protect. Everything else is at least a maybe.
const TERMINAL = new Set([400, 404, 410, 422])

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

// --- pacing: one token bucket for the whole process --------------------------

let nextSlotAt = 0
let inflight = 0
const waiters = []

async function acquire() {
  if (inflight >= MAX_INFLIGHT) {
    await new Promise((resolve) => waiters.push(resolve))
  } else {
    inflight++
  }
  const gap = nextSlotAt - Date.now()
  if (gap > 0) await sleep(gap)
  nextSlotAt = Date.now() + MIN_INTERVAL_MS
}

function release() {
  const next = waiters.shift()
  if (next) next() // a waiter takes this slot; the in-flight count stays put
  else inflight--
}

// --- quota bookkeeping --------------------------------------------------------

let remaining = null
let resetAtMs = 0

function noteHeaders(res) {
  const rem = Number(res.headers.get('x-ratelimit-remaining'))
  if (Number.isFinite(rem)) remaining = rem
  const reset = Number(res.headers.get('x-ratelimit-reset'))
  if (Number.isFinite(reset)) resetAtMs = reset * 1000
}

/** Last quota values GitHub told us, or nulls before the first response. */
export function quotaState() {
  return { remaining, resetInMs: resetAtMs ? Math.max(0, resetAtMs - Date.now()) : null }
}

/** How long to sleep before retrying a throttled response, 0 when unknown. */
function throttleDelayMs(res) {
  // retry-after is what secondary limits send; honor it before guessing.
  const after = Number(res.headers.get('retry-after'))
  if (Number.isFinite(after) && after > 0) return after * 1000
  if (remaining === 0 && resetAtMs) return Math.max(0, resetAtMs - Date.now()) + 1000
  return 0
}

// --- the request path ---------------------------------------------------------

async function send(path, opts) {
  await acquire()
  try {
    return await fetch(`${API}${path}`, {
      ...opts,
      headers: {
        accept: 'application/vnd.github+json',
        authorization: `Bearer ${TOKEN}`,
        'user-agent': USER_AGENT,
        ...opts?.headers,
      },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })
  } finally {
    release()
  }
}

async function ghAttempt(path, opts) {
  const normalized = path.startsWith('/') ? path : `/${path}`
  let attempt = 0
  let backoff = BACKOFF_BASE_MS
  let lastStatus = 0
  while (attempt < MAX_ATTEMPTS) {
    attempt++
    let res
    try {
      res = await send(normalized, opts)
    } catch {
      lastStatus = 0
      if (attempt >= MAX_ATTEMPTS) break
      await sleep(backoff * (0.5 + Math.random()))
      backoff = Math.min(backoff * 2, 60000)
      continue
    }
    noteHeaders(res)
    if (res.ok) return { status: res.status, body: await res.json().catch(() => null) }
    lastStatus = res.status
    if (retryable(res)) {
      const wait = throttleDelayMs(res) || backoff * (0.5 + Math.random())
      // A reset hours away means this job should fail fast and let the next
      // one try — not hold a runner hostage until the window reopens.
      if (wait > MAX_WAIT_MS) break
      await sleep(wait)
      backoff = Math.min(backoff * 2, 60000)
    }
  }
  return { status: lastStatus, body: null }
}

function retryable(res) {
  return !TERMINAL.has(res.status)
}

export async function ghGet(path) {
  if (!TOKEN) throw new GhError(0, 'GITHUB_TOKEN is not set')
  const { status, body } = await ghAttempt(path, { method: 'GET' })
  if (status !== 200) throw new GhError(status)
  return body
}

export async function ghRaw(path, opts = {}) {
  if (!TOKEN) return { status: 0, body: null }
  return ghAttempt(path, opts)
}
