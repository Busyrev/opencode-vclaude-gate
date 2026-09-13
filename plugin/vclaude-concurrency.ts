import type { Plugin } from "@opencode-ai/plugin"

/**
 * vclaude.ru держит жёсткий лимит: не больше 5 одновременных запросов
 * на модель на ключ. Слот занят всю длительность стрима, шестой запрос
 * получает 429 с безликим "Сервис временно недоступен".
 *
 * Плагин подменяет fetch у провайдера, чей baseURL указывает на vclaude.ru,
 * и ставит перед ним семафор: лишние запросы ждут очереди вместо 429.
 * Слот освобождается не на заголовках ответа, а на конце тела —
 * именно так его считает сервер.
 */

const LIMIT = (() => {
  const raw = Number(process.env.VCLAUDE_MAX_CONCURRENCY)
  return Number.isInteger(raw) && raw > 0 ? raw : 5
})()

/** Страховка от слота, зависшего на недочитанном стриме. */
const HOLD_TIMEOUT_MS = 10 * 60_000

const TARGET_BASE_URL = /vclaude\.ru/i

const MODEL_IN_BODY = /"model"\s*:\s*"([^"]+)"/

type Bucket = { active: number; queue: Array<() => void> }

const buckets = new Map<string, Bucket>()
const gated = new WeakSet<object>()

function bucketFor(key: string): Bucket {
  let bucket = buckets.get(key)
  if (!bucket) {
    bucket = { active: 0, queue: [] }
    buckets.set(key, bucket)
  }
  return bucket
}

function modelOf(init?: RequestInit): string {
  const body = init?.body
  if (typeof body === "string") return MODEL_IN_BODY.exec(body)?.[1] ?? "unknown"
  return "unknown"
}

/** Обычный режим молчит, чтобы не сорить в вывод `opencode run`. */
const DEBUG = Boolean(process.env.VCLAUDE_GATE_DEBUG)

function log(message: string) {
  if (DEBUG) console.log(`[vclaude-gate] ${message}`)
}

function warn(message: string) {
  console.log(`[vclaude-gate] ${message}`)
}

function releaser(key: string, bucket: Bucket): () => void {
  let released = false
  const finish = () => {
    released = true
    clearTimeout(timer)
    bucket.active--
    bucket.queue.shift()?.()
  }
  const timer = setTimeout(() => {
    if (released) return
    warn(`слот ${key} удерживался дольше ${HOLD_TIMEOUT_MS / 1000} с, возвращаю в пул`)
    finish()
  }, HOLD_TIMEOUT_MS)
  timer.unref?.()
  return () => {
    if (!released) finish()
  }
}

function acquire(key: string, signal?: AbortSignal | null): Promise<() => void> {
  const bucket = bucketFor(key)
  if (bucket.active < LIMIT) {
    bucket.active++
    return Promise.resolve(releaser(key, bucket))
  }
  if (signal?.aborted) return Promise.reject(abortError(signal))

  const waitStarted = Date.now()
  return new Promise<() => void>((resolve, reject) => {
    const cleanup = () => signal?.removeEventListener("abort", onAbort)
    const enter = () => {
      cleanup()
      bucket.active++
      log(`${key}: ждал слот ${Date.now() - waitStarted} мс, в очереди ещё ${bucket.queue.length}`)
      resolve(releaser(key, bucket))
    }
    const onAbort = () => {
      const index = bucket.queue.indexOf(enter)
      if (index !== -1) bucket.queue.splice(index, 1)
      cleanup()
      reject(abortError(signal))
    }
    signal?.addEventListener("abort", onAbort, { once: true })
    bucket.queue.push(enter)
    log(`${key}: занято ${bucket.active}/${LIMIT}, встаю в очередь (${bucket.queue.length})`)
  })
}

function abortError(signal?: AbortSignal | null): unknown {
  return signal?.reason ?? new DOMException("The operation was aborted.", "AbortError")
}

/** Ответы, которым по спецификации нельзя приделать тело. */
const NULL_BODY_STATUS = new Set([101, 103, 204, 205, 304])

function gate(upstream: typeof fetch): typeof fetch {
  return async (input: any, init?: any) => {
    const release = await acquire(modelOf(init), init?.signal)
    let response: Response
    try {
      response = await upstream(input, init)
    } catch (error) {
      release()
      throw error
    }
    if (!response.body || NULL_BODY_STATUS.has(response.status)) {
      release()
      return response
    }

    const reader = response.body.getReader()
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const { done, value } = await reader.read()
          if (done) {
            release()
            controller.close()
            return
          }
          controller.enqueue(value)
        } catch (error) {
          release()
          controller.error(error)
        }
      },
      cancel(reason) {
        release()
        return reader.cancel(reason)
      },
    })

    return new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    })
  }
}

export default (async () => ({
  config: async (config: any) => {
    for (const [id, provider] of Object.entries<any>(config.provider ?? {})) {
      const options = provider?.options
      if (!options || typeof options !== "object") continue
      if (!TARGET_BASE_URL.test(String(options.baseURL ?? ""))) continue
      if (gated.has(options)) continue
      const upstream: typeof fetch = typeof options.fetch === "function" ? options.fetch : globalThis.fetch
      options.fetch = gate(upstream)
      gated.add(options)
      log(`провайдер ${id}: лимит ${LIMIT} одновременных запросов на модель`)
    }
  },
})) satisfies Plugin
