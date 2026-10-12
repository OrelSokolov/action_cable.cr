# action_cable.cr — порт ActionCable

> ⚠️ EXPERIMENTAL FRAMEWORK, DO NOT USE IN PRODUCTION YET.

Неймспейс: `ActionCable` — 1:1. Оценка: ~90%. На Crystal ложится
естественнее, чем на Ruby: fiber-per-connection поверх `HTTP::WebSocket`
(stdlib) вместо nio4r + thread pool.

## API 1:1

### Connection (`app/channels/application_cable/connection.cr`)
- `class Connection < ActionCable::Connection::Base`
- `identified_by :current_user` (N раз) — макрос: геттер/сеттер + регистр
- `def connect` / `def disconnect`; `reject_unauthorized_connection`
- `cookies` (через cookie_jar запроса на апгрейд) — аутентификация
  как в Rails
- `before_command/after_command/around_command`

### Channel (`< ApplicationCable::Channel < ActionCable::Channel::Base`)
- `def subscribed` / `def unsubscribed` (+ `before_subscribe/
  after_subscribe/before_unsubscribe/after_unsubscribe` с `if:/unless:`)
- `stream_from "room_#{id}" do |msg| end` (+ coder:), `stream_for @post`,
  `stop_stream_from/stop_stream_for/stop_all_streams`,
  `stream_or_reject_for`, `transmit(data, via:)`, `reject`, `params`,
  `connection`
- `periodically :transmit_progress, every: 5.seconds` / `periodically
  every: 3.minutes do ... end`
- Публичный метод канала = RPC-экшен для клиента: в Crystal —
  макрос-таблица диспетчеризации (разработчик пишет `def speak(data)` —
  работает так же, как в Rails)
- Class: `CommentsChannel.broadcast_to(@post, message)`,
  `broadcasting_for(target)`

### Сервер
- `ActionCable.server.broadcast(name, message)`,
  `ActionCable.server.remote_connections.where(current_user: u).disconnect`
- Монтирование на `/cable` (`mount_path`)
- Протокол: тот же JSON-протокол Rails (`command: subscribe/message`,
  ответы `welcome/subscription_confirmation/ping/disconnect`) —
  JS-клиент @rails/actioncable работает без изменений

### Config
`config/cable.yml`: `adapter:` async (in-process) / postgresql
(LISTEN/NOTIFY) / redis (pubsub) + `channel_prefix`;
`config.action_cable.*`: mount_path, allowed_request_origins,
worker_pool_size (fiber-пул), logger, log_tags.

## Реализация

- WebSocket-апгрейд + fiber-per-connection; сердце сервера — event loop
  не нужен, блокирующий IO на fiber'ах дёшев.
- Pubsub-адаптеры: Async (in-process канал), PG LISTEN/NOTIFY (crystal-pg),
  Redis (кристаллический redis-клиент). Протокол адаптера — как в Rails.
- Remote disconnect: внутренние per-connection каналы, как в Rails.
- Identifier'ы → per-connection fiber state.

## Статус (Iteration 8 — periodically + remote disconnect готовы)

- `periodically` (порт Channel::PeriodicTimers): DSL-макрос в классе канала —
  `periodically :tick, every: 5.seconds` / `periodically every: 3.minutes do … end`;
  таймеры стартуют после `subscribed` (after_subscribe) и останавливаются на
  `unsubscribe_from_channel` (after_unsubscribe), fiber-цикл `select` по
  sleep/stop-каналу — стоп мгновенный, без долёта интервала. Реестр таймеров
  наследуется подклассами (`self.periodic_timers`, спека зеркалит gem).
  Отклонение: DSL — макрос, callable = имя метода или блок (Proc в class-body
  невыговариваем); `every:` — Time::Span или число секунд; ArgumentError на
  every <= 0.
- `identified_by :current_user` на `ActionCable::Connection` (порт
  Connection::Identification): макрос-регистрация, значения String,
  `connection_identifier` — сортированный colon-join gid как в геме.
  Добавлены перезаписываемые коллбэки `connect`/`disconnect` (connect — до
  welcome, disconnect — после отписок).
- Remote disconnect (порт RemoteConnections + Connection::InternalChannel):
  `server.remote_connections.where(current_user: "1", …).disconnect(reconnect: true)`;
  каждый Connection подписан на свой внутренний канал `action_cable/<gid>`
  через pubsub-адаптер → кросс-процессно на Redis/PG; сообщение
  `{"type":"disconnect","reconnect":…}` → фрейм disconnect (reason "remote")
  → WS close handshake (`Connection#close_connection`). Реестр соединений в
  Server (`connections`/register/unregister) — интроспекция; доставка идёт
  через внутренний канал, как в геме. `CorkWriter#flush` гарантирует порядок
  disconnect-фрейм до close-фрейма.
- Оракул: +6 кейсов (rc_internal_channel ×2, rc_disconnect_payload ×2,
  rc_invalid_identifiers, periodic_timers_registered) — сняты с 8.1.4,
  `ruby oracle/cases.rb > oracle/expected.yml` (диф — только добавления).
  Runtime-поведение покрыто unit/интеграционными спеками:
  `spec/periodically_spec.cr` (5), `spec/remote_disconnect_spec.cr` (4:
  disconnect по идентификатору, reconnect: false, несколько коннектов одного
  пользователя, InvalidIdentifiersError). Сюит 40/40.
- Отклонения: значения идентификаторов — только String (gid-строки);
  `unsubscribe` внутреннего канала дропает и коллбэк соседнего коннекта с тем
  же gid (Proc без identity, как в остальных адаптерах); отписка
  where-сообщением проверена на Async-адаптере (Redis/PG — тот же pubsub-путь,
  вручную не прогонялась).

## Статус (Iteration 7 — кросс-процессные pubsub-адаптеры готовы)

- Порт `PubsubAdapter` включён в `SubscriptionAdapter::Base`; 1:1
  Rails-API (broadcast/subscribe/unsubscribe/shutdown) остался на Base.
- `src/adapters/redis_pubsub.cr` — `SubscriptionAdapter::Redis`:
  broadcast → PUBLISH, подписки — одно выделенное SUBSCRIBE-соединение
  (новые каналы дописываются в него под Mutex).
- `src/adapters/pg_pubsub.cr` — `SubscriptionAdapter::PostgreSQL`:
  LISTEN/NOTIFY (аналог solid_cable без таблицы поллинга): стримы →
  безопасные идентификаторы (`ac_` + hex-SHA1), broadcast →
  `pg_notify`; одна listen-связка на все стримы. Ограничение PG:
  payload ≤ ~8000 байт на NOTIFY.
- Оракул: `oracle/pubsub_cases.rb` → `pubsub_expected.yml`, снят с
  настоящего ActionCable 8.1.4 Redis-адаптера (5 кейсов). Зеркало
  `spec/pubsub_spec.cr`: одни и те же кейсы для Redis и PostgreSQL.
  Сюит 25/25.
- Отклонения: `unsubscribe` дропает коллбэки канала в процессе, но не
  redis-подписку (Proc без identity — как в Test/Async); broadcast
  возвращает nil, а не счётчик получателей PUBLISH.
- Бенч: `bench/crails_pubsub` → `bench/results/202-pubsub.md`.

## Статус (Iteration 5 — P1.5 fast broadcast готов, бенч снят)

- Oracle: `oracle/cases.rb` → `oracle/expected.yml`, 14 кейсов сняты с
  actioncable 8.1.4 (канальная семантика через FakeConnection-харнесс +
  протокольные константы). Зеркало: `spec/oracle_spec.cr` — 14/14 зелёные.
- WS-слой: `src/connection.cr` + `src/server.cr` — Rails-JSON-протокол
  (welcome/subscribe/confirm_subscription/message/ping), fiber-per-connection,
  ручной апгрейд в `Server::CableHandler` (прямой доступ к io для cork).
  Покрыт интеграционным спеком `spec/ws_spec.cr` (реальный сервер + клиент).
- P1.5 fast broadcast (Bun/uWS publish-аналог, см. секцию ниже):
  `src/cork_writer.cr` (cork-буфер + writer-fiber, 1 write на батч,
  frame-once API, backpressure cap 1 MiB + drop), `src/async_adapter.cr`
  (Async-адаптер, fast-реестр, encode-once конверт — дефолт сервера),
  `stream_from` без блока → fast-подписка. Бенчи: `results/310-ws-bun-matrix.md`
  (2.72M msg/s ≈ bun-publish 2.52M, ~6.5× bun-send), `results/300-websockets.md`
  (397k msg/s @ 300 коннектов, было 261k). Спеки 15/15.
- AsyncAdapter active_job: `bench/crails_jobs` — см. `bench/results/200-jobs.md`
  (~1M jobs/s no-op). WS-бенч: `bench/crails_ws` + `bench/loadgen/ws_load.cr`.
- Реализовано (`src/`): `protocol.cr` (INTERNAL-константы 1:1 + envelope
  builder), `subscription_adapter.cr` (Base + Test), `channel.cr`
  (Channel::Base: subscribed/unsubscribed/stream_from/stop_all_streams/
  transmit/reject/params + perform_action-диспетчер), `connection.cr`,
  `cork_writer.cr`, `async_adapter.cr`, `server.cr`, `ruby_inspect.cr`.
- Зафиксированные отклонения от Ruby API:
  - `transmit` публичный (в Ruby приватный; в Crystal нет `send`);
  - `streams` → публичный `stream_names` (для тестов);
  - `unsubscribe(channel, callback)` → `unsubscribe(channel)` — у Proc
    в Crystal нет identity;
  - params — `Hash(String, String)` вместо HashWithIndifferentAccess;
  - INTERNAL — именованные константы вместо гетерогенного хеша;
  - cookies/stream_for/broadcast_to — P1 (identified_by и periodically —
    готовы, Iteration 8).
- Семантика, снятая оракулом: transmit оборачивает в
  `{identifier:, message:}`; unsubscribe чистит streams до вызова
  `unsubscribed`; dispatch по публичным 0/1-арг методам подкласса
  (кроме lifecycle).

## Fast broadcast — перенос идей Bun/uWS publish (P1.5)

Контекст: `bench/bun_ws` — оригинальный бенч oven-sh (32 клиента, closed
loop). Bun `publishText` 4.17M msg/s против Bun `send` 159K (26×) — и это
не «быстрый WebSocket», а другой API fan-out'а. Разбор исходников
(`~/bun/packages/bun-uws/src/`):

| идея | Bun (publish) | наш текущий путь (P0.5) |
|---|---|---|
| кодирование | конверт сериализуется 1 раз — «палитра» `outgoingMessages`, подписчик получает `uint16`-индекс (`TopicTree.h:301`) | на каждого подписчика: `JSON.parse` + `to_json` + String-аллокации |
| fan-out | целиком нативный цикл по `drainableSubscribers`, JS не участвует | N вызовов `Proc(String, Nil)` |
| запись | cork: фреймы копятся в буфере лупа, drain в pre/post-handler → 1 `write` на сокет на итерацию (`App.h:514`, `LoopData.h`) | `@ws.send` на подписчика = syscall на сообщение на сокет |
| backpressure | `publishText` → -1 (queued) / 0 (dropped); drain останавливается на DROPPED | нет: блокирующая запись, медленный клиент стопит весь sweep |

Итого на 1 broadcast × N подписчиков сейчас: N парсингов, N сериализаций,
N Proc-вызовов, N syscall'ов, 0 батчинга. Замер P0.5: ~261k msg/s @ 300
коннектов (`bench/results/300-websockets.md`).

### Целевой дизайн (4 механизма)

1. **Fast-stream подписка (encode-once).** `stream_from` без блока
   регистрирует `(connection, identifier)` в реестре адаптера вместо Proc.
   Broadcast: конверт `{"identifier":…,"message":…}` строится один раз на
   уникальный identifier — message уже валидный JSON, встраивается как есть
   (raw-embedding, без parse→dump). Затем те же байты уходят всем
   подписчикам этого identifier.
2. **Per-connection cork.** Исходящие фреймы копятся в буфере соединения
   (двухбуферный swap под Mutex), writer-fiber соединения выгребает батч
   одним `io.write` — 1 syscall на сокет на батч. Аналог drain+cork: пока
   писатель пишет, новые сообщения накапливаются в следующий батч.
3. **Один писатель сокета.** Авто-понги stdlib (из fiber-читателя) и батчи
   writer-fiber сериализуются lock-обёрткой IO → нет интерливинга фреймов
   (заодно чинит существующую гонку ping-fiber vs broadcast-fiber).
4. **Backpressure.** Буфер капится (1 MiB); переполнение → drop сообщения +
   счётчик (семантика Bun DROPPED). Медленный клиент не стопит sweep.

Отличие от Bun: у нас fiber-модель, «итерация event loop» = батч
writer-fiber (пробуждение по каналу-флагу, порог — размер буфера), а не
pre/post-handler epoll. Кастомные коллбэки `stream_from(name) { |msg| }`
остаются на Proc-пути — семантика 1:1 с Rails.

### Совместимость и риски

- Протокол не меняется: oracle 14/14 и ws_spec должны остаться зелёными
  без правок тестов (меняется только путь доставки). Добавляется ws_spec
  на Async-адаптере (fast path) — тот же протокол end-to-end.
- Raw-embedding: сообщение с неканоничными пробелами не нормализуется,
  как после parse→dump (остаётся валидным JSON с тем же значением).
  Oracle сидит на Test-адаптере (Proc-путь) — не затронут.
- Монтаж `/cable` переходит с `HTTP::WebSocketHandler` на ручной upgrade
  (нужен прямой доступ к io для cork) — поведенчески то же, проверено
  standalone-вариантом `bench/bun_ws/chat-server.cr`.

### Бенчмарк-критерий (сравнение с Bun на их же клиенте)

- `bench/bun_ws/chat-server.crails.cr` — chat-сервер на `action_cable_cr`
  (`stream_from` fast path) в матрице run_all.sh против bun-publish /
  bun-send / node-ws. Цель: ряд bun-send и выше; syscalls/доставку меряем
  `strace -c -e write,writev`.
- `bench/crails_ws` (300-websockets): регресса быть не должно; цель —
  заметный рост с ~261k msg/s. Результаты → `bench/results/`.

### Этапы

- T1: Async-адаптер + fast-реестр + encode-once конверт.
- T2: cork-буфер + writer-fiber + 1 write на батч + IO-lock (п.2–3).
- T3: backpressure (cap + drop + счётчики, `buffered_amount`).
- T4: бенчи (`bench/bun_ws`, 300-websockets), отчёт в `bench/results/`.

## P1.6: Оптимизации WS-транспорта (backlog — не реализовано)

Обоснование: CPU, сожжённый на горячем пути, не исчезает — он украден у
соседей по процессу (scheduler-очередь других fiber'ов, GC-паузы,
вытеснение кэша). Поэтому дешёвый fan-out — это не «цифра в бенче», а
чужая p99-латентность под пиковой нагрузкой. Мера эффекта — тот же бенч
(`bench/bun_ws/run_all.sh`, база в `results/310-ws-bun-matrix.md`).

Память 95 MB против 42 MB у bun-publish — принятый размен: cork-буферы
мелкие на фоне реального Rails-приложения, не проблема.

### 1. Буферизованное чтение (главный кандидат)

Stdlib `WebSocket::Protocol` читает каждый входящий фрейм напрямую из
сокета: `read_fully` заголовка (2 байта) + `read_fully` payload = 2–3
`read`-сисколла на сообщение. Запись мы закрыли cork'ом (P1.5), чтение
осталось с P0.5. Аналог uWS: один большой `read()` на пробуждение, из
буфера парсится сколько влезло фреймов. Реализация: обёртка буферизованного
чтения под `Protocol` (по образцу `LockedImageIO` в `cork_writer.cr`).
Ожидание: минус 1–2 сисколла на каждое входящее сообщение.

### 2. JSON-фастпас на горячем пути

`handle_command` парсит каждое сообщение дважды (`JSON.parse(raw)` +
`JSON.parse(data["data"])`) — две аллокации дерева `JSON::Any` + хэши,
GC-давление растёт с throughput, а GC останавливает ВСЕ fiber'ы процесса.
Для broadcast-паттерна (`command == "message"`) — диспетчеризация без
полного парса: проверка подстрок `"command":"message"` / identifier,
полный JSON только для data. Порядок полей в клиентах @rails/actioncable
стабилен; чужие клиенты с другим порядком — фолбэк на полный парс.

### 3. Мелочи

- Кэш конвертов: несколько broadcast'ов за тик с одним identifier
  строят одну и ту же строку — мемоизировать на тик.
- Пул фрейм-буферов вместо `Bytes.dup` на батч в `CorkWriter`.
- Стек fiber'ов: ~290 КБ резидентно на коннект — тюнинг ради цели
  «100k коннектов на процессе» (ёмкость, не скорость).

### Куда не лезть

- Свой парсер RFC 6455 вместо stdlib: фрагментация, интерливинг
  control-фреймов, close-handshake — uWS шлифует это годами.
- io_uring: нет в Crystal stdlib, переписывание планировщика ради
  последних процентов.
- Busy-polling writer-fiber'ов: обмен CPU на латентность, бьёт по
  соседям — противоречит смыслу всего раздела.

## Фазы

- P0: connection + channel DSL, stream_from/transmit/reject, async-адаптер,
  JSON-протокол (совместимость с @rails/actioncable).
- P1: PG/Redis pubsub (кросс-процессность), broadcast_to/stream_for,
  periodically, remote disconnect, identified_by полный. Готово, кроме
  broadcast_to/stream_for (Iteration 7–8).
- P1.5: fast broadcast — идеи Bun/uWS publish: encode-once + cork-буфер +
  writer-fiber + backpressure (см. секцию выше). Готово, Iteration 5.
- P1.6: оптимизации WS-транспорта — буферизованное чтение, JSON-фастпас,
  мелочи (backlog, см. секцию выше).
- P2: command-коллбэки, статистика, health-check.


## Готово, когда

два браузера с официальным `@rails/actioncable` JS-клиентом подключаются
к `/cable`, состоят в канале и получают broadcast'ы друг друга.

## Зависимости

active_support.cr, railties.cr (HTTP-сервер), crystal-pg / redis.

## Phase E — identified_by-коннект + stream_for/broadcast_to ✅ (2026-10-05)

Оракул: `oracle/stream_for_cases.rb` → `oracle/stream_for_expected.yml`
(8 кейсов, actioncable 8.1.4 + GlobalID::Identification, app
"crails_app"): broadcasting_for = `[channel_name, *Array(broadcastables)]
.join(":")`, элемент — `to_gid_param` (base64url("gid://<app>/<Class>/<id>")
без паддинга; unpersisted с id — тоже gid) или to_param/to_s.
channel_name = underscore(имя без Channel-суффикса), неймспейсы → ":".
Зеркало `spec/stream_for_spec.cr` 11/11; полный сюит 51/51.

Реализация:
- `channel.cr`: `ActionCable.serialize_broadcasting` (gid), class/instance
  `broadcasting_for`/`broadcast_to` (class-level — через новый
  `ActionCable.server` module-акцессор; приложение подменяет своим
  Server при монтировании), `stream_for`/`stop_stream_for`/
  `stream_or_reject_for`; `channel_name` генерится в `macro inherited`.
  Отклонение: app-имя — `ActionCable.gid_app` (ENV CRAILS_APP_NAME,
  default "app"), не GlobalID.app.
- `connection.cr`: upgrade-запрос передаётся в Connection
  (`server.cr` CableHandler), `request_cookies` (парсинг Cookie),
  `session` — верификация подписанной cookie фазы C (crails_session,
  SignedCookieJar action_pack, guarded по secret), доступно в
  connect-хуке; `reject_unauthorized_connection!` → Unauthorized
  ConnectionError → disconnect-фрейм {reason: "unauthorized",
  reconnect: false} без регистрации соединения.
- shard.yml += active_record_cr, action_pack_cr (gid-сериализация
  записей и cookie-джарбы; зеркалит зависимости гема actioncable →
  actionpack).
