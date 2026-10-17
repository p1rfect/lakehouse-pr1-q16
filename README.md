# Практическая работа №1. Локальный Lakehouse

Баланов Артём. Вариант 4, TPC-H Q16 «Parts/Supplier Relationship».

Стек: Silo (форк MinIO) → Apache Iceberg / Lakekeeper → Trino. Дополнительный движок Apache Spark читает тот же Bronze через Iceberg REST и S3, без копирования данных. Каждый слой имеет отдельный бакет, warehouse и каталог: `bronze`, `silver`, `gold`. Бенчмарк хранится отдельно в `bench`.

Масштаб — `tpch.sf50`, то есть номинальный класс примерно 50 ГиБ в масштабе TPC-H до колоночного кодирования и сжатия. Это не побайтное измерение исходного набора и не размер сжатых Parquet/ORC на диске. Во всех слоях и сравнениях используется один масштаб. Фактические размеры файлов приведены в отчёте; для них ГБ = 10⁹ байт.

## Содержимое

- `отчет.md` — заполненная форма, методология, результаты и выводы.
- `compose.yaml`, `catalog/`, `trino/` — локальный стенд.
- `sql/` — SQL для просмотра и выполнения вручную.
- `scripts/lab.py` — загрузка, ELT, проверки, бенчмарк и инвентаризация.
- `scripts/spark_bronze.py` — чтение Bronze дополнительным движком.
- `results/` — реальные замеры, идентификаторы запросов, проверки и выгрузка Gold.

`results/reads.json` содержит итоговые сопоставимые серии чтения. `queries.jsonl` хранит также служебные запросы и предварительные проверки при настройке стенда; они не включаются в медианы отчёта. Готовность worker проверяется чтением одного ключа из маленькой `region` того же каталога после каждого перезапуска. Это не прогрев целевой таблицы Q16 или lineitem.

## Запуск на Windows

Нужны запущенный Docker Desktop в Linux-режиме, Docker Compose и Python 3.10+. Для `lab.py` внешние Python-библиотеки не требуются. Данные и образы занимают существенное место; большой прогон использует четыре представления одного исходного набора данных.

Все команды выполняются из корня проекта:

```powershell
docker compose up -d storage postgres catalog trino
python scripts/lab.py init
python scripts/lab.py core
python scripts/lab.py evolution
python scripts/lab.py benchmark
python scripts/lab.py audit
python scripts/lab.py inventory
```

При первом запуске дождаться `starting: false` по `http://localhost:18080/v1/info`. Скрипт PowerShell делает ожидание автоматически:

```powershell
.\Run-Lab.ps1 -Action all
.\Run-Lab.ps1 -Action spark
```

`core` и `evolution` предназначены для первого запуска в новом томе. `benchmark` пропускает уже материализованные таблицы и сохраняет записи измерений по мере выполнения. Для независимого повторного эксперимента используйте новый Compose project name и свободные порты. Простой повтор чтений измеряет повторное чтение существующих данных, а не новый независимый эксперимент записи.

Для анализа физических файлов и повторной сборки отчёта есть дополнительные скрипты. Основной стенд и ELT от этих Python-пакетов не зависят:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-analysis.txt
.\.venv\Scripts\python.exe scripts/inspect_storage.py
.\.venv\Scripts\python.exe scripts/build_report.py
```

## Подключения

| Сервис | Адрес |
|---|---|
| S3 API | `http://localhost:19000` |
| Консоль Silo | `http://localhost:19001` |
| Lakekeeper | `http://localhost:18181/ui/` |
| Trino | `http://localhost:18080` |
| JDBC Trino | `jdbc:trino://localhost:18080/bronze/lakehouse` |

Trino: пользователь `balanov`, пароль пустой. Silo: `minioadmin` / `pr1-local-storage-2026`. Это настройки воспроизводимого локального учебного стенда. Порты привязаны к `127.0.0.1`. PostgreSQL доступен внутри сети Compose и не занимает порт существующей БД пользователя.

```sql
SELECT * FROM gold.lakehouse.supplier_counts
ORDER BY supplier_cnt DESC, brand, type, size;
```

## Остановка с сохранением данных

```powershell
docker compose stop
```

Данные остаются в именованных томах `pr1-balanov_storage` и `pr1-balanov_postgres`. Архив проекта содержит код, отчёт и измерения; большие файлы данных восстанавливаются генератором TPC-H и в Git не входят.

## Замена образа из гайда

При выполнении 17.10.2026 запросы к `quay.io/minio/minio` и `quay.io/minio/mc` вернули HTTP 401; образ `minio/minio` из Docker Hub также не скачался. Использован S3-совместимый форк [PGSTY Silo](https://github.com/pgsty/silo), фиксированная версия `RELEASE.2026-09-16T00-00-00Z`. Это замена объектного хранилища, которую предусматривает поле «если поднимал вместо чего-то» в форме отчёта. Silo сохраняет MinIO-переменные окружения, S3 API и клиент `mcli`.

## Документация

- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html).
- [Trino TPC-H connector](https://trino.io/docs/current/connector/tpch.html) и [TPC-H specification, раздел 4.1.3](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf).
- [Apache Iceberg: configuration and compression](https://iceberg.apache.org/docs/latest/configuration/).
- [Lakekeeper](https://docs.lakekeeper.io/).
- [Silo](https://silo.pgsty.com/docs/).
- Учебные материалы преподавателя: «Гайд-лейкхаус-win», «Гайд-2-медальон», «Презентация к Р1. Лаб.», форма «отчет.md» и «варианты.md».
