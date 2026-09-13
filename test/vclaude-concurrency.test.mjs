import assert from "node:assert/strict"
import plugin from "../plugin/vclaude-concurrency.ts"

const LIMIT = 5
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const body = (model) => JSON.stringify({ model, max_tokens: 64, messages: [] })

/**
 * Прогоняем настоящий хук config: он должен найти провайдера по baseURL
 * и подменить его fetch. Дальше тестируем именно то, что получил opencode.
 */
async function gatedFetch(upstream) {
  const hooks = await plugin({}, {})
  const config = {
    provider: {
      anthropic: { options: { baseURL: "https://vclaude.ru/v1", fetch: upstream } },
      other: { options: { baseURL: "https://api.anthropic.com" } },
    },
  }
  await hooks.config(config)
  assert.notEqual(config.provider.anthropic.options.fetch, upstream, "fetch провайдера не подменён")
  assert.equal(config.provider.other.options.fetch, undefined, "чужой провайдер трогать нельзя")
  return config.provider.anthropic.options.fetch
}

/** upstream, у которого тело закрывается только по команде close(). */
function makeUpstream() {
  const state = { started: 0, active: 0, peak: 0, order: [], handles: [] }
  const upstream = async (_input, init) => {
    state.started++
    state.active++
    state.peak = Math.max(state.peak, state.active)
    state.order.push(init.tag)
    let close
    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("chunk"))
        close = () => {
          state.active--
          controller.close()
        }
      },
    })
    state.handles.push({ tag: init.tag, close: () => close() })
    return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } })
  }
  return { upstream, state }
}

async function drain(response) {
  const reader = response.body.getReader()
  while (true) {
    const { done } = await reader.read()
    if (done) return
  }
}

// 1. Пик конкурентности и FIFO
{
  const { upstream, state } = makeUpstream()
  const fetched = await gatedFetch(upstream)
  const pending = []
  for (let i = 0; i < 12; i++) pending.push(fetched("https://x", { body: body("t1"), tag: i }))
  await sleep(50)
  assert.equal(state.started, LIMIT, `в upstream ушло ${state.started}, ожидалось ${LIMIT}`)

  // дочитываем ответы по одному — каждый должен пускать ровно одного следующего
  for (let i = 0; i < 12; i++) {
    const response = await pending[i]
    state.handles.find((h) => h.tag === i).close()
    await drain(response)
    await sleep(20)
    const expected = Math.min(12, LIMIT + i + 1)
    assert.equal(state.started, expected, `после закрытия #${i} стартовало ${state.started}, ждали ${expected}`)
  }
  assert.equal(state.peak, LIMIT, `пик ${state.peak}`)
  assert.deepEqual(state.order, [...Array(12).keys()], `порядок нарушен: ${state.order}`)
  console.log("ok  пик = 5, очередь FIFO, слоты возвращаются")
}

// 2. Слот держится до конца тела, а не до заголовков
{
  const { upstream, state } = makeUpstream()
  const fetched = await gatedFetch(upstream)
  const responses = await Promise.all(
    [...Array(LIMIT).keys()].map((i) => fetched("https://x", { body: body("t2"), tag: i })),
  )
  const extra = fetched("https://x", { body: body("t2"), tag: 99 })
  await sleep(50)
  assert.equal(state.started, LIMIT, "заголовки получены, но слоты обязаны оставаться занятыми")
  state.handles[0].close()
  await drain(responses[0])
  await sleep(20)
  assert.equal(state.started, LIMIT + 1, "после конца тела должен стартовать следующий")
  await extra
  console.log("ok  слот освобождается по концу тела, не по заголовкам")
}

// 3. Отмена ожидающего запроса не тратит слот
{
  const { upstream, state } = makeUpstream()
  const fetched = await gatedFetch(upstream)
  const responses = await Promise.all(
    [...Array(LIMIT).keys()].map((i) => fetched("https://x", { body: body("t3"), tag: i })),
  )
  const controller = new AbortController()
  const cancelled = fetched("https://x", { body: body("t3"), tag: 98, signal: controller.signal })
  const queued = fetched("https://x", { body: body("t3"), tag: 97 })
  await sleep(20)
  controller.abort()
  await assert.rejects(cancelled, (error) => error.name === "AbortError")
  state.handles[0].close()
  await drain(responses[0])
  await sleep(20)
  assert.equal(state.started, LIMIT + 1, "освободившийся слот должен уйти следующему живому запросу")
  assert.equal(state.order.at(-1), 97, "слот ушёл не тому запросу")
  await queued
  console.log("ok  abort в очереди не съедает слот")
}

// 4. Счётчики по моделям независимы
{
  const { upstream, state } = makeUpstream()
  const fetched = await gatedFetch(upstream)
  for (let i = 0; i < LIMIT; i++) void fetched("https://x", { body: body("t4-opus"), tag: i })
  await sleep(20)
  void fetched("https://x", { body: body("t4-haiku"), tag: 100 })
  await sleep(20)
  assert.equal(state.started, LIMIT + 1, "haiku не должен ждать очередь opus")
  console.log("ok  лимит считается на модель")
}

// 5. Ошибка upstream возвращает слот
{
  let calls = 0
  const failing = async () => {
    calls++
    throw new Error("boom")
  }
  const fetched = await gatedFetch(failing)
  for (let i = 0; i < LIMIT + 3; i++) {
    await assert.rejects(fetched("https://x", { body: body("t5") }), /boom/)
  }
  assert.equal(calls, LIMIT + 3, "после ошибки слот должен возвращаться в пул")
  console.log("ok  ошибка upstream возвращает слот")
}

// 6. Ответ без тела возвращает слот
{
  let calls = 0
  const fetched = await gatedFetch(async () => {
    calls++
    return new Response(null, { status: 204 })
  })
  for (let i = 0; i < LIMIT + 3; i++) await fetched("https://x", { body: body("t6") })
  assert.equal(calls, LIMIT + 3, "ответ 204 не должен удерживать слот")
  console.log("ok  ответ без тела возвращает слот")
}

// 7. Отмена во время чтения тела возвращает слот
{
  const { upstream, state } = makeUpstream()
  const fetched = await gatedFetch(upstream)
  const responses = await Promise.all(
    [...Array(LIMIT).keys()].map((i) => fetched("https://x", { body: body("t7"), tag: i })),
  )
  await responses[0].body.cancel("stop")
  await sleep(20)
  void fetched("https://x", { body: body("t7"), tag: 90 })
  await sleep(20)
  assert.equal(state.started, LIMIT + 1, "cancel() тела должен освобождать слот")
  console.log("ok  cancel тела освобождает слот")
}

console.log("\nвсе проверки прошли")
