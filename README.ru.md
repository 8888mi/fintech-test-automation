# Автотесты платёжной платформы

*[English version](README.md)*

Фреймворк автотестов для платёжного сервиса с асинхронной обработкой событий. Покрывает три
слоя — **REST API**, **контракты событий Kafka** и **браузерный UI** — одним набором, против
настоящего брокера, а не моков.

```bash
git clone <repo> && cd fintech-test-automation
make install && make up && make test
```

Три команды — и всё окружение (брокер Kafka, тестируемый сервис, браузер) поднимается, а
набор тестов отрабатывает. Без ручной настройки и без «спроси у меня креды».

[![tests](https://github.com/<user>/<repo>/actions/workflows/tests.yml/badge.svg)](https://github.com/<user>/<repo>/actions/workflows/tests.yml)

---

## Зачем это нужно

Большинство наборов тестов для платёжных систем останавливается на HTTP-ответе. Непроверенной
остаётся более сложная половина: сервис вернул `201 Created`, тест позеленел — а событие, от
которого зависят рецепт-леджер и сервис уведомлений, не было опубликовано вовсе или было
опубликовано дважды.

Этот фреймворк рассматривает вызов API и порождённое им событие как **одну проверяемую
транзакцию**.

```python
def test_replayed_request_does_not_emit_a_second_event(payments_api, kafka_consumer, account_id):
    key = payments_api.new_idempotency_key()
    payment = payments_api.create(account_id=account_id, amount_minor=11_000,
                                  idempotency_key=key).expect_status(201).json()

    kafka_consumer.wait_for_payment_event(payment["payment_id"], "payment.created")

    # Тот же ключ повторно — должно быть no-op, а не второе списание.
    payments_api.create(account_id=account_id, amount_minor=11_000,
                        idempotency_key=key).expect_status(200)

    kafka_consumer.assert_no_event(
        lambda e: e.value["payment_id"] == payment["payment_id"]
                  and e.value["event_type"] == "payment.created",
        within=5.0, description="duplicate payment.created",
    )
```

Дублирующее событие здесь означает списание с клиента дважды. Ни один тест, работающий только
с API, этого не поймает.

---

## Стек

| Слой | Инструменты |
|---|---|
| Язык | Python 3.11+, Poetry |
| Раннер | pytest, pytest-xdist, pytest-rerunfailures |
| API | requests + контрактные модели pydantic |
| События | confluent-kafka |
| UI | Playwright (синхронный API), Page Object |
| Отчётность | Allure — HTML-отчёт собирается локально; CI дополнительно публикует его на GitHub Pages |
| CI | GitHub Actions с сервисами Docker Compose |
| Качество кода | ruff, mypy |

---

## Что нужно установить

| Для чего | Требование |
|---|---|
| Для всего | Python 3.11+ и [Poetry](https://python-poetry.org/) |
| `make up`, integration- и UI-тесты | Docker с Compose v2, запущенный демон |
| `make report` | [Allure CLI](https://allurereport.org/docs/install/) и JRE — **не** ставится через `make install` |
| Цели `make` | GNU Make. На «голой» Windows отсутствует — пользуйтесь `run-tests.bat` или командами напрямую (см. ниже) |

`make install` ставит только Python-зависимости и сборку Chromium. Allure CLI — отдельная
Java-программа: `allure-pytest` пишет сырые результаты, а отчёт из них собирает уже CLI.

---

## Структура

```
demo_service/            Тестируемая система: FastAPI-сервис платежей + продюсер Kafka + статический UI
src/framework/
  config.py              Типизированные настройки — единственное место, читающее окружение
  api/
    client.py            HTTP-сессия, ретраи только на транзиентных ошибках, вложения в Allure
    payments_api.py      Service object: тесты выражают намерение, а не транспорт
    models.py            Контракты ответов, extra="forbid"
  kafka_client/
    consumer.py          Ограниченный по времени read-only консьюмер с ожиданием по предикату
  ui/
    base_page.py         Локаторы только по data-testid
    pages/               Page objects
  utils/
    money.py             Целые минорные единицы, Decimal на границах, никогда float
    waiters.py           Поллинг вместо sleep
tests/
  conftest.py            Фикстуры: настройки, HTTP-клиент, консьюмер Kafka, опции браузера
  api/                   Быстрые, без брокера и браузера — основная масса покрытия
  integration/           Контракт API → Kafka и идемпотентность
  ui/                    Мало, широко, сквозь слои
docs/
  ARCHITECTURE.md        Проектные решения и их обоснование
  TEST_STRATEGY.md       Что покрыто на каком уровне и что сознательно не покрыто
.dockerignore            Держит кеши и артефакты прогонов вне контекста сборки
run-tests.bat            Замена Makefile для Windows
```

---

## Проектные решения

Подробное обоснование — в [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Кратко:

**Деньги — целое число минорных единиц.** `float` не касается суммы нигде. `0.1 + 0.2` в
двоичной плавающей точке не равно `0.3`, и в платёжной системе этот зазор рано или поздно
всплывает как расхождение при сверке, которое никто не может воспроизвести.

**Набор тестов объявляет собственную копию схемы событий.** `src/framework/api/models.py`
намеренно не импортирует модели сервиса. Будь определение общим, ломающее изменение контракта
правило бы и тест вместе с реализацией — и набор остался бы зелёным прямо во время инцидента.
Дублирование здесь и есть защитный механизм, а `extra="forbid"` роняет тест ещё и на
необъявленном новом поле.

**Консьюмер Kafka — read-only и с дедлайном.** Одноразовый `group.id` вместе с
`enable.auto.commit=False` означает, что прогон против общего окружения никогда не сдвинет
оффсеты настоящей консьюмер-группы. У каждого ожидания есть предел, и падает оно со списком
реально пришедших событий — вместо того чтобы висеть, пока раннер CI не убьёт джобу без единой
диагностики.

**Консьюмер подписывается до действия, порождающего событие.** Назначение партиций
принудительно инициируется и подтверждается заранее — так закрывается окно, в котором быстрый
сервис успевает опубликовать событие, а тест его уже не увидит. Это и есть разница между
надёжным набором тестов и надёжным на 95% — а 95% хуже, чем бесполезно, потому что люди
начинают перезапускать падения на рефлексе.

**Ретраи только на транзиентных ошибках.** `429` и `5xx` повторяются, `4xx` доходит до
проверки нетронутым: отказ по бизнес-правилу — это результат, а не сетевой сбой.

**UI-тестов мало, и они широкие.** Всё, что доказуемо на уровне API, доказывается там, где это
стоит миллисекунды. Браузерные тесты существуют ради одного, чего нижние уровни доказать не
могут: что клик оборачивается событием в топике.

**Локаторы — только `data-testid`.** CSS-классы и тексты являются решениями дизайна; привязка
тестов к ним означает, что правка формулировки ломает набор.

---

## Запуск

| Команда | Что делает |
|---|---|
| `make install` | Python-зависимости + Chromium |
| `make up` | Kafka + сервис через Docker Compose, ждёт healthcheck |
| `make test` | Весь набор, параллельно, один ретрай на падение |
| `make test-api` | Только слой API — брокер и браузер не нужны |
| `make test-integration` | Контрактные тесты API → Kafka |
| `make test-ui` | Тесты Playwright |
| `make report` | Собрать и открыть отчёт Allure |
| `make lint` | ruff + mypy |
| `make down` | Погасить окружение и удалить тома |

Без `make` — например, на Windows — те же команды напрямую:

```bash
poetry install && poetry run playwright install chromium
docker compose up -d --wait
poetry run pytest -n auto --reruns 1 --reruns-delay 2
poetry run pytest tests/api          # либо tests/integration, tests/ui
poetry run ruff check src tests demo_service && poetry run mypy src
docker compose down -v
```

### Windows: `run-tests.bat`

Тот же сценарий одним скриптом. Если демон Docker не отвечает, скрипт запускает Docker Desktop
(ждёт до двух минут), затем выполняет `docker compose up -d --wait` и pytest и завершается с
кодом выхода pytest.

```bat
run-tests.bat                  :: весь набор, параллельно, один ретрай на падение (как в CI)
run-tests.bat api              :: только tests/api; также integration, ui
run-tests.bat ui --headed      :: всё после имени набора передаётся в pytest как есть
run-tests.bat install          :: poetry install + Chromium
run-tests.bat kafka-ui         :: открыть веб-интерфейс Kafka (см. ниже)
run-tests.bat down             :: погасить окружение вместе с kafka-ui и удалить тома
```

После прогона окружение остаётся запущенным, чтобы следующий запуск стартовал сразу;
останавливается оно через `run-tests.bat down`. Окружение нужно всем наборам, включая API:
API-тесты обращаются к сервису платежей на `localhost:8000`, а он работает в Compose.

Подмножества выбираются путём, как выше. Маркеры, объявленные в `pyproject.toml`
(`smoke`, `api`, `integration`, `ui`), **пока не проставлены ни на одном тесте**, поэтому `-m`
сейчас отсеивает всё — до появления маркеров пользуйтесь каталогами.

---

## Конфигурация

Все настройки живут в `src/framework/config.py` и читаются из окружения. Скопируйте шаблон и
правьте копию:

```bash
cp .env.example .env
```

**`.env.example` — только шаблон.** Классы настроек читают `.env`
(`SettingsConfigDict(env_file=".env")`), поэтому правка `.env.example` не даёт ничего. Без
файла `.env` действуют значения по умолчанию из `config.py` — именно поэтому набор тестов
работает против Docker Compose «из коробки».

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `API_BASE_URL` | `http://localhost:8000` | Тестируемый сервис |
| `API_TIMEOUT_SECONDS` / `API_RETRY_ATTEMPTS` | `10` / `3` | Таймаут HTTP и транзиентные ретраи |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Брокер, внешний листенер |
| `KAFKA_PAYMENTS_TOPIC` | `payments.events` | Топик, по которому идут проверки |
| `KAFKA_EVENT_TIMEOUT_SECONDS` | `15` | Дедлайн ожидания события |
| `UI_BASE_URL` | `http://localhost:8000` | Тестируемая страница |
| `UI_HEADLESS` | `true` | `false` показывает окно браузера |
| `UI_SLOW_MO_MS` | `0` | Пауза перед каждым действием Playwright, мс |
| `UI_DEFAULT_TIMEOUT_MS` | `10000` | Таймаут локаторов |

> **Коммитятся значения, безопасные для CI.** На раннере GitHub Actions `ubuntu-latest` нет
> X-сервера, поэтому `headless = False` в `config.py` упал бы там с
> *«launched a headed browser without having a XServer running»*, не оберни джобу в
> `xvfb-run`. Локальным предпочтениям для отладки место в `.env` — он в gitignore и до CI
> не доезжает.

Против защищённого брокера задайте `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT` с
соответствующим механизмом и учётными данными; правок в коде не требуется.

`--env` перекрывает `ENVIRONMENT` для одного прогона: `pytest --env=staging`. Если флаг не
передан, значение из окружения остаётся нетронутым.

### Как увидеть браузер

```bash
poetry run pytest tests/ui --headed --slowmo 500
```

`--headed` и `--slowmo` побеждают `UI_HEADLESS` / `UI_SLOW_MO_MS` — командная строка всегда
перекрывает `.env`. Учтите: `slow_mo` ставит паузу *между* действиями Playwright и не печатает
текст посимвольно, потому что page objects используют `fill()`.

---

## Отчёты и диагностика

### Allure

```bash
poetry run pytest                                    # пишет в allure-results/
allure serve allure-results                          # временный отчёт + браузер
allure generate allure-results -o allure-report --clean && allure open allure-report
```

**Не открывайте `allure-report/index.html` двойным кликом.** По `file://` страница будет
пустой: отчёт подгружает свои JSON через XHR, а браузеры блокируют такие запросы с файлового
протокола. Нужен именно локальный сервер — его и поднимают `allure serve` и `allure open`.

Каждый запуск pytest сначала очищает `allure-results/` (`--clean-alluredir` прописан в
`addopts`), поэтому отчёт всегда показывает только последний прогон. Чтобы увидеть несколько
наборов в одном отчёте, запускайте их одной командой — `pytest tests/api tests/ui`: второй
отдельный запуск сотрёт результаты первого. Для трендов нужно перед генерацией копировать
`allure-report/history` обратно в `allure-results/history` — именно это делает CI-джоба через
ветку `gh-pages`.

Набор тестов прикладывает к отчёту тела запросов и ответов каждого HTTP-вызова, найденное
событие Kafka, последние 25 наблюдённых событий при таймауте ожидания и полностраничный
скриншот из UI happy path.

### Kafka UI

Топики, сообщения, партиции и consumer-группы в браузере на **http://localhost:8080**:

```bash
docker compose --profile tools up -d kafka-ui      # или: run-tests.bat kafka-ui
```

Сервис вынесен в профиль Compose `tools`, поэтому обычные `docker compose up`, `make up` и CI
его не скачивают и не ждут. По той же причине простой `docker compose down -v` его не
останавливает — нужен `docker compose --profile tools down -v` (так и делает
`run-tests.bat down`). Образ — `kafbat/kafka-ui`, поддерживаемый форк
`provectuslabs/kafka-ui`; к брокеру он подключается по внутреннему листенеру `kafka:29092`.

### Посмотреть топик напрямую

```bash
# Живой хвост — только новые сообщения; оставьте запущенным на время прогона (Ctrl+C — выход)
docker exec -it qa-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payments.events --property print.key=true

# Всё, что опубликовано к этому моменту
docker exec -it qa-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payments.events --from-beginning --property print.key=true

# Первые 5 сообщений — команда завершится сама
docker exec qa-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payments.events --from-beginning --max-messages 5

# Список топиков, описание (партиции, лидер, реплики) и оффсеты
docker exec qa-kafka kafka-topics --bootstrap-server localhost:29092 --list
docker exec qa-kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic payments.events
docker exec qa-kafka kafka-get-offsets --bootstrap-server localhost:29092 --topic payments.events
```

Команды записаны одной строкой, чтобы их можно было вставить в любую оболочку — PowerShell
и cmd не понимают перенос строки через `\`, как в bash. Без `--from-beginning`
консьюмер показывает только сообщения, пришедшие после его запуска, поэтому на тихом топике
он выглядит зависшим — это нормально.

Порт 29092 — внутренний листенер брокера, доступный изнутри контейнера; тесты с хоста ходят на
9092. На этом образе привычный `kafka-run-class kafka.tools.GetOffsetShell` больше не работает:
в Kafka 3.7 класс переехал в `org.apache.kafka.tools`, поэтому нужен скрипт
`kafka-get-offsets`.

Топик `payments.events` заранее не создаётся: он появляется при первой записи сервиса
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`). Сообщения копятся между прогонами и исчезают
только после `docker compose down -v`, которая удаляет тома.

---

## Особенности платформ

**На Windows нет `make`.** Пользуйтесь `run-tests.bat`, прямыми командами выше либо поставьте GNU Make. Цель
`make clean` вдобавок опирается на `rm -rf` и `find`, так что ей нужна POSIX-оболочка вроде
Git Bash.

**Никогда не передавайте `TopicPartition` в логирование.** В confluent-kafka 2.15.0 под Windows
её `__repr__` собран на C через `PRId32`, который MSVC разворачивает в `%I32d` — директиву,
которую `PyUnicode_FromFormat` не принимает, — поэтому `repr()`, `str()` и `%`-форматирование
одинаково падают с `SystemError`. Именно поэтому `consumer.py` логирует обычные кортежи
`(topic, partition)`.

**Контекст сборки Docker — весь репозиторий** (`context: .` в Compose), тогда как образу нужен
только `demo_service/`. `.dockerignore` держит вне контекста кеши и артефакты прогонов; без
него туда уезжают мегабайты `.mypy_cache` и вывода Allure, растущие с каждым запуском.

---

## CI

`.github/workflows/tests.yml` прогоняет линтеры, поднимает Kafka и сервис через Docker Compose,
запускает набор на 4 воркерах и публикует отчёт Allure на GitHub Pages с историей за 30
прогонов. Логи сервиса сохраняются артефактом при падении, потому что красная сборка без логов
стоит дороже, чем отсутствие сборки вообще.

**Публикация происходит только в CI**, при локальном прогоне — никогда. Локально pytest пишет
сырые результаты в `allure-results/` (`--alluredir` прописан в `addopts` файла
`pyproject.toml`, поэтому это делает каждый запуск), а превращение их в отчёт — отдельный
ручной шаг `allure serve` из раздела выше. Джоба `publish-report` выгружает `allure-results`
артефактом, сливает его с историей, хранящейся в ветке `gh-pages`, и пушит собранный отчёт
обратно туда же. Она ограничена условием `github.ref == 'refs/heads/main'`, так что для работы
нужен репозиторий на GitHub с включённым Pages, у которого источником указана ветка
`gh-pages`, — и заполненные плейсхолдеры `<user>/<repo>` в бейдже выше.

---

## Границы решения

Сервис в `demo_service/` — специально написанная заглушка, достаточно маленькая, чтобы
прочитать её за один присест, но воспроизводящая те свойства, которые делают платёжные системы
неудобными для тестирования: идемпотентность, асинхронные побочные эффекты, переходы состояний
с недопустимыми путями и деньги, которые не должны терять точность.

Поставляемый результат — сам фреймворк. Нацелить его на другое платёжное API означает переписать
service objects в `src/framework/api/` и page objects; клиент, консьюмер, конфигурация, фикстуры
и отчётность переносятся без изменений.
