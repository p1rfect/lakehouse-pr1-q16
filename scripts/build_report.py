"""Build the report from measured JSON. Optional: pip install matplotlib."""
import json
import statistics as st
from pathlib import Path
from collections import defaultdict
import os
os.environ.setdefault('MPLCONFIGDIR',str(Path.cwd()/'work'/'mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT=Path(__file__).resolve().parents[1]
R=ROOT/'results'
def load(name):
    return json.loads((R/name).read_text(encoding='utf-8-sig'))
def fmt(value,digits=2):
    return f'{value:,.{digits}f}'.replace(',',' ')
def integer(value):
    return f'{value:,}'.replace(',',' ')
def mb(value):
    return fmt(value/1e6,4 if value<1e6 else 2)
def table(headers,rows):
    return '| '+' | '.join(headers)+' |\n|'+ '|'.join(['---']*len(headers))+'|\n'+'\n'.join('| '+' | '.join(str(v).replace('|','\\|').replace('\n',' ') for v in row)+' |' for row in rows)+'\n'

writes=load('writes.json'); reads=load('reads.json'); layers=load('layers.json')
checks=load('checks.json'); physical=load('physical_files.json'); evolution=load('evolution.json')
spark=load('spark_bronze.json'); audit=load('audit.json'); system=load('system.json')
gold=load('gold.json')['rows']; benchmark_checks=load('benchmark_checks.json')
assert benchmark_checks.get('all_configs_equal') is True,'Benchmark has not completed successfully'
assert len(writes)==32 and len(reads)==32,'Expected all 32 writes and 32 final read measurements'
assert len({(w['config'],w['table']) for w in writes})==32,'Duplicate write measurements'
configs=['parquet_zstd','orc_gzip','parquet_gzip','orc_zstd']
names={'parquet_zstd':'Parquet / ZSTD','orc_gzip':'ORC / ZLIB (GZIP в Trino)','parquet_gzip':'Parquet / GZIP','orc_zstd':'ORC / ZSTD'}
summary=[]
for config in configs:
    group=[w for w in writes if w['config']==config]
    entry={'config':config,'bytes':sum(w['data_bytes'] for w in group),'write':sum(w['wall_seconds'] for w in group),'files':sum(w['files'] for w in group),'rows':sum(w['rows'] for w in group),'metadata':sum(p['iceberg_metadata_bytes'] for p in physical if p['config']==config)}
    for query in ['q16','lineitem_aggregation']:
        measurements=[r for r in reads if r['config']==config and r['query']==query]
        first=next(r for r in measurements if r['run']==0)
        repeat=[r for r in measurements if r['run']>0]
        entry[query]={'first':first['wall_seconds'],'median':st.median(r['wall_seconds'] for r in repeat),'min':min(r['wall_seconds'] for r in repeat),'max':max(r['wall_seconds'] for r in repeat),'input_bytes':st.median(r['stats']['physicalInputBytes'] for r in repeat)}
    summary.append(entry)
(R/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')

plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})
fig,axes=plt.subplots(2,2,figsize=(12,8),layout='constrained')
labels=['Parquet\nZSTD','ORC\nZLIB','Parquet\nGZIP','ORC\nZSTD']
colors=['#2264aa','#c17c22','#6594c7','#50a293']
for ax,values,title,ylabel in [
    (axes[0,0],[s['bytes']/1e9 for s in summary],'Размер данных восьми таблиц','ГБ'),
    (axes[0,1],[s['write'] for s in summary],'Материализация восьми таблиц','Секунды'),
]:
    ax.bar(labels,values,color=colors,width=.6)
    ax.set_title(title,loc='left',fontweight='bold');ax.set_ylabel(ylabel)
    for index,value in enumerate(values):ax.text(index,value,f'{value:.2f}',ha='center',va='bottom')
for ax,query,title in [(axes[1,0],'q16','Q16: соединения и COUNT DISTINCT'),(axes[1,1],'lineitem_aggregation','Агрегация всей lineitem')]:
    x=list(range(4))
    ax.bar([i-.18 for i in x],[s[query]['first'] for s in summary],width=.36,color='#c2d4e7',label='После перезапуска Trino')
    ax.bar([i+.18 for i in x],[s[query]['median'] for s in summary],width=.36,color='#2264aa',label='Медиана трёх повторов')
    ax.set_xticks(x,labels);ax.set_ylabel('Секунды');ax.set_title(title,loc='left',fontweight='bold');ax.legend(fontsize=8)
fig.suptitle('TPC-H sf50 · один узел Trino 476 · 8 vCPU · heap 6 ГиБ',fontsize=16,fontweight='bold')
fig.savefig(R/'benchmark.png',dpi=170)
plt.close(fig)

parts=[]
parts.append('# Отчёт по практической работе №1\n\nРазвёртывание локального Lakehouse, ELT и сравнение форматов и сжатия.\n')
parts.append('## 1. Вариант и масштаб\n')
parts.append(table(['Поле','Значение'],[
    ['ФИО','Баланов Артём'],['Вариант','4, TPC-H Q16 «Parts/Supplier Relationship»'],['Масштаб','tpch.sf50 во всех слоях и сравнениях'],['Объём','Полный TPC-H sf50: номинально примерно 50 ГиБ исходного набора в масштабе TPC-H до колоночного кодирования и сжатия. Это не побайтное измерение исходного набора; фактические размеры файлов измерены отдельно.'],['Почему sf50','Масштаб соответствует заданному большому набору и помещается на локальном диске. Для Q16 используются три таблицы, а оставшиеся пять загружены для сравнения форматов на полном наборе.'],['Дата выполнения','17 октября 2026 года']]))
parts.append('В [TPC-H specification, §4.1.3](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf) SF1 соответствует приблизительно 1 ГиБ. Для учебного эксперимента используется произвольный SF50, который допускает [генератор Trino](https://trino.io/docs/current/connector/tpch.html). Номинальный SF не является измерением физических файлов. Все восемь таблиц действительно материализованы в каждом из четырёх вариантов; полные таблицы не заменялись выборкой.\n')
parts.append('## 2. Поднятые сервисы и выбор технологий\n')
parts.append(table(['Сервис','Роль','Обоснование'],[
 ['Silo, форк MinIO','S3-совместимое объектное хранилище','Сохраняет S3 API и подход гайда. Образы MinIO из гайда вернули 401 при скачивании, поэтому использован фиксированный выпуск Silo. Отдельные бакеты физически разделяют слои.'],
 ['PostgreSQL 17','Служебная БД Lakekeeper','Хранит состояние каталога и поддерживает транзакции. Файлы таблиц сюда не загружаются. Использован отдельный именованный том.'],
 ['Lakekeeper','Iceberg REST Catalog','Один каталог таблиц доступен Trino и Spark через открытый REST-интерфейс. Состояние таблицы не привязано к одному движку.'],
 ['Trino 476','ELT, SQL и бенчмарк','Имеет детерминированный генератор tpch, чтение и запись Iceberg, Parquet и ORC. Позволяет провести эксперимент одним движком.'],
 ['Apache Spark 3.5.5','Дополнительный инструмент, часть 6.2','Читает существующие Iceberg-таблицы Bronze через тот же REST-каталог и S3. Проверяет совместимость и выполняет агрегацию без предварительного экспорта в отдельный формат.']]))
parts.append('Развёртывание описано в `compose.yaml`, каталогах `catalog/` и `trino/`. Образы движков, хранилища и каталога закреплены digest. Все команды запускаются из корня репозитория:\n\n```powershell\ndocker compose up -d storage postgres catalog trino\npython scripts/lab.py init\npython scripts/lab.py core\npython scripts/lab.py evolution\npython scripts/lab.py benchmark\npython scripts/lab.py audit\npython scripts/lab.py inventory\n.\\Run-Lab.ps1 -Action spark\n```\n')
parts.append('Первая команда создаёт сервисы, сеть и именованные тома. `init` создаёт бакеты, warehouse и схемы, затем сохраняет список сервисов и digest образов. `core` строит медальон и проверяет Q16. `benchmark` создаёт остальные варианты и выполняет замеры. Spark запущен отдельной командой; во время основного бенчмарка его вычисления остановлены. Подробные инструкции и адреса подключения приведены в `README.md`.\n')
parts.append('Оборудование: '+str(system['Cpu'])+f"; память компьютера {fmt(int(system['HostMemoryBytes'])/2**30)} ГиБ; Docker {fmt(int(system['DockerMemoryBytes'])/2**30)} ГиБ, {system['DockerCPUs']} логических CPU. Trino ограничен 8 vCPU и 8 ГиБ памяти контейнера, JVM heap 6 ГиБ, память запроса до 4 ГиБ. Это один coordinator, который также выполняет задачи. На хосте работали и другие контейнеры; ресурсы не выделялись эксклюзивно, поэтому абсолютные времена зависят от фоновой нагрузки.\n")
parts.append('## 3. Разбор Q16\n')
parts.append(table(['Таблица','Нужные колонки','Использование'],[
 ['part','partkey, brand, type, size','Соединение с partsupp, фильтры бренда, типа и размеров, группировка и вывод.'],
 ['partsupp','partkey, suppkey','Связь детали с поставщиком и COUNT(DISTINCT suppkey).'],
 ['supplier','suppkey, comment','Исключение поставщиков, в комментарии которых встречается шаблон %Customer%Complaints%.']]))
parts.append('Исходный SQL приведён в `sql/03_reference_q16.sql`. Результат показывает число разных поставщиков для группы бренд–тип–размер. Один поставщик, поставляющий несколько деталей одной группы, учитывается один раз. Порядок чтения Gold задаётся явно через `ORDER BY supplier_cnt DESC, brand, type, size`: порядок строк в файле не считается гарантией сортировки запроса.\n')
parts.append('## 4. Решения по слоям\n\nРазмеры в таблицах — сумма `file_size_in_bytes` активных data-файлов Iceberg. 1 МБ = 1 000 000 байт, 1 ГБ = 1 000 000 000 байт. Количество строк дополнительно получено SQL `SELECT count(*)`, результаты сохранены в `results/audit.json`. Метаданные учитываются отдельно в разделе 5.\n')
parts.append('### Bronze\n\nВсе исходные колонки сохранены, строки не обновляются и не удаляются. Таблицы один раз загружены `CREATE TABLE AS SELECT * FROM tpch.sf50.<table>`. Формат Parquet/ZSTD выбран для основного слоя; альтернативы изолированы в `bench`. Партиционирование не задано: Q16 не имеет фильтра по дате, а разбиение по высококардинальным ключам увеличило бы число небольших файлов. Целевой максимальный размер data-файла — 256 МБ; это верхняя цель, а не гарантия заполнения каждого файла.\n')
bronze=[w for w in writes if w['config']=='parquet_zstd']
parts.append(table(['Таблица','Колонки','Строк','МБ','Файлов','Формат / кодек','Партиции'],[[w['table'],', '.join(c[0] for c in audit['bronze.lakehouse.'+w['table']]['schema']),integer(w['rows']),mb(w['data_bytes']),w['files'],'Parquet / ZSTD','нет'] for w in bronze]))
parts.append('### Silver\n\n`eligible_parts` оставляет бизнес-поля деталей, проверяет обязательные ключи и значения, применяет фильтры Q16 и `trim` к текстовым атрибутам. `part_suppliers` соединяет разрешённые детали с поставками и поставщиками, исключает жалобы и обогащает связь атрибутами бренда, типа и размера. При повторяющихся поставщиках внутри группы Gold сохраняет семантику `COUNT DISTINCT`. Исходный TPC-H уже согласован: очистка не выдумывает отсутствующие ошибки, а проверки подтверждают качество входа.\n')
parts.append(table(['Таблица','Колонки','Строк','МБ','Формат / кодек','Партиции','Обоснование'],[[name.split('.')[-1],', '.join(c[0] for c in audit[name]['schema']),integer(info['rows']),mb(info['data_bytes']),'Parquet / ZSTD','нет','Колоночное чтение и компактное хранение; слою не нужно разбиение по дате.'] for name,info in layers.items() if name.startswith('silver.')]))
parts.append('### Gold\n')
parts.append(table(['Таблица','Колонки','Строк','МБ','Формат / кодек','Партиции','Обоснование'],[['supplier_counts','brand, type, size, supplier_cnt',integer(len(gold)),fmt(layers['gold.lakehouse.supplier_counts']['data_bytes']/1e6,4),'Parquet / ZSTD','нет','Компактная витрина читает готовый агрегат и не повторяет тяжёлые соединения. Партиции только усложнили бы маленькую таблицу.']]))
parts.append('Первые пять строк витрины:\n\n'+table(['brand','type','size','supplier_cnt'],gold[:5]))
parts.append('### Проверка расчёта\n\nПолное сравнение отсортированных строк исходного Q16 на `tpch.sf50`, Q16 на Bronze и чтения Gold дало совпадение. Это сравнение всех строк, а не только первых пяти. SHA-256 JSON-представления результата: `'+checks['gold_sha256']+'`.\n')
parts.append(table(['Проверка','Найдено нарушений'],[[k,v] for k,v in checks.items() if isinstance(v,int) and not isinstance(v,bool) and k!='gold_rows']))
parts.append('Результат и исходные идентификаторы запросов: `results/gold.csv`, `results/gold.json`, `results/checks.json`, `results/queries.jsonl`.\n')
parts.append('## 5. Бенчмарк форматов и кодеков\n\n### Методология\n\nВ каждой конфигурации материализованы одни и те же восемь таблиц `tpch.sf50` с теми же колонками. Время записи — клиентское монотонное время от отправки CTAS до завершения commit Iceberg, суммированное по восьми таблицам. Оно включает генерацию TPC-H, кодирование, сжатие, S3-запись и commit; это не изолированная скорость компрессора. Запись каждого набора измерена один раз. Порядок конфигураций фиксирован: Parquet/ZSTD, ORC/ZLIB, Parquet/GZIP, ORC/ZSTD. Первый набор создавался до построения медальона, остальные после него. Эти условия ограничивают выводы о небольших различиях во времени записи.\n\nTrino 476 принимает кодек через сессионное свойство `bronze.compression_codec` или `bench.compression_codec`. Табличное свойство `compression_codec` этой версией не поддерживается. В Python свойство передаётся заголовком `X-Trino-Session`; в SQL-консоли используется `SET SESSION bench.compression_codec=\'GZIP\'`. Настройка GZIP для ORC физически дала ZLIB/DEFLATE, что проверено чтением файла. Это явно показано в названиях, а не приравнено к формату контейнера gzip.\n\nДля чтения выполнены два запроса: исходный Q16 и полная агрегация lineitem по `returnflag, linestatus` с количеством строк, суммой количества и выручкой. Для выручки использованы DECIMAL, чтобы результат сравнивался точно и не зависел от порядка суммирования DOUBLE. Перед каждой серией движок Trino перезапущен; выполнено пробное чтение одного ключа из маленькой region того же каталога, чтобы дождаться готовности worker. Это прогревает базовый код чтения, но не целевую таблицу. Уровни кодеков не переопределялись. Во всех конфигурациях tpch.splits-per-node=8 и task.concurrency=8, партиционирование отсутствует. Выполнены один первый целевой запрос и три последовательных повтора; в таблице указана медиана повторов и их диапазон. Чтение результатов включено в клиентское время. Spark во время замеров остановлен. Итоговые 32 измерения перечислены в reads.json; служебные и предварительные запросы из queries.jsonl в расчёт не включены.\n\nПерезапуск Trino очищает его JVM и кэши, но не очищает файловый кэш ОС и хранилища. Первый запуск включает JIT и загрузку метаданных, поэтому он не обозначается как полностью холодное чтение с диска. Разницу с повторами нельзя приписать только одному виду кэша. Все результаты обеих агрегаций совпали между четырьмя конфигурациями.\n')
parts.append(table(['Конфигурация','Данные, ГБ','Data-файлов','Запись, с','Q16 после restart, с','Q16 медиана, с','lineitem после restart, с','lineitem медиана, с'],[[names[s['config']],fmt(s['bytes']/1e9),s['files'],fmt(s['write']),fmt(s['q16']['first']),fmt(s['q16']['median']),fmt(s['lineitem_aggregation']['first']),fmt(s['lineitem_aggregation']['median'])] for s in summary]))
parts.append('![Сравнение размеров, записи и чтения](results/benchmark.png)\n')
parts.append('Требуемое сравнение GZIP и ZSTD на трёх таблицах Bronze, необходимых Q16:\n')
parts.append(table(['Кодек Parquet','Размер трёх таблиц, МБ','Запись трёх таблиц, с','Q16 медиана, с'],[[codec,fmt(sum(w['data_bytes'] for w in writes if w['config']==config and w['table'] in ['part','partsupp','supplier'])/1e6),fmt(sum(w['wall_seconds'] for w in writes if w['config']==config and w['table'] in ['part','partsupp','supplier'])),fmt(next(s for s in summary if s['config']==config)['q16']['median'])] for config,codec in [('parquet_gzip','GZIP'),('parquet_zstd','ZSTD')]]))
parts.append('Диапазоны трёх повторов и прочитанные физические байты:\n\n'+table(['Конфигурация','Запрос','min, с','max, с','Медиана physicalInputBytes, МБ'],[[names[s['config']],q,fmt(s[q]['min']),fmt(s[q]['max']),fmt(s[q]['input_bytes']/1e6)] for s in summary for q in ['q16','lineitem_aggregation']]))
parts.append('### Метаданные\n\nВнешние метаданные Iceberg измерены перечислением S3-объектов `metadata/`, отдельно от `data/`. Это JSON описаний таблиц и Avro-манифесты; они не равны статистикам файлов Parquet/ORC. Встроенные footer и stripe statistics дополнительно прочитаны PyArrow из фактических файлов. Инвентаризация не подменяется `SHOW STATS`: статистика оптимизатора не является точным физическим размером хранилища.\n')
parts.append(table(['Конфигурация','Iceberg metadata, МБ','Доля от data+metadata, %'],[[names[s['config']],fmt(s['metadata']/1e6,4),fmt(s['metadata']/(s['metadata']+s['bytes'])*100,4)] for s in summary]))
samples=[p for p in physical if p['table']=='part' and p['config'] in configs]
parts.append('Пример встроенных метаданных: по одному реальному файлу `part` из каждой конфигурации. Это выборочные footer, а не сумма footer всего набора:\n\n'+table(['Конфигурация','Фактический кодек','Строк в файле','Footer, байт','Row groups / stripes'],[[names[p['config']],p['actual_codec'],integer(p['sample_rows']),p['sample_footer_bytes'],p.get('sample_row_groups',p.get('sample_stripes'))] for p in samples]))
parts.append('Размер встроенных метаданных зависит от количества колонок, групп строк/страйпов и статистик. Разные размеры sample-файлов ограничивают прямое сравнение их footer. Метаданные помогают пропускать ненужные файлы и столбцы, но имеют стоимость получения и разбора. На полном наборе абсолютные data-файлы существенно больше каталожных метаданных; на маленьких справочниках и витринах относительная доля метаданных может быть заметнее.\n')
smallest=min(summary,key=lambda s:s['bytes']); fastest_write=min(summary,key=lambda s:s['write']); fastest_q16=min(summary,key=lambda s:s['q16']['median']); fastest_scan=min(summary,key=lambda s:s['lineitem_aggregation']['median'])
zstd=next(s for s in summary if s['config']=='parquet_zstd')
gzip=next(s for s in summary if s['config']=='parquet_gzip')
differences=[]
for label,left,right,lower,higher in [
    ('размер',zstd['bytes'],gzip['bytes'],'меньше','больше'),
    ('время записи',zstd['write'],gzip['write'],'меньше','больше'),
    ('медиана Q16',zstd['q16']['median'],gzip['q16']['median'],'меньше','больше'),
]:
    delta=(left/right-1)*100
    differences.append(f'{label} на {fmt(abs(delta),1)}% {lower if delta<0 else higher}')
parts.append('Сравнение двух кодеков Parquet на полном наборе: для ZSTD относительно GZIP '+', '.join(differences)+'. Это наблюдения одного стенда с указанными выше ограничениями повторяемости.\n')
parts.append('### Интерпретация\n\n'+f"На этом стенде минимальный объём получил {names[smallest['config']]} ({fmt(smallest['bytes']/1e9)} ГБ), минимальное суммарное время материализации — {names[fastest_write['config']]} ({fmt(fastest_write['write'])} с). По медиане Q16 лидировал {names[fastest_q16['config']]} ({fmt(fastest_q16['q16']['median'])} с), по агрегации lineitem — {names[fastest_scan['config']]} ({fmt(fastest_scan['lineitem_aggregation']['median'])} с). Один победитель не обязан лидировать по всем метрикам.\n\n"+'Для основного медальона оставлен Parquet/ZSTD: он читается обоими проверенными движками, уменьшает объём хранения и сохраняет приемлемую скорость. Выбор не означает универсального превосходства на другом оборудовании. Стоимость облака в рублях не вычислялась: тарифа и облачного развёртывания в задании нет. Измеренные байты и времена показывают направления затрат на хранение и вычисления.\n')
parts.append('## 6. Осмысление и дополнительный инструмент\n\n### 6.1. Lakehouse, эволюция схемы и time travel\n')
parts.append(table(['Вопрос','Ответ'],[
 ['Зачем большие данные и lakehouse?','Когда файлы велики и их используют разные движки, S3 позволяет отдельно масштабировать хранение и вычисления. Iceberg добавляет согласованные снапшоты, схему и атомарные изменения набора файлов. Для небольшой транзакционной задачи обычная PostgreSQL может быть проще; здесь lakehouse нужен для файлового хранения, ELT и сравнения движков.'],
 ['Что произошло при schema evolution?','В отдельной Silver-таблице evolution_demo из трёх исходных поставщиков добавлен quality_note, затем переименован в review_note. Старые строки прочитались с NULL. ADD и RENAME изменили метаданные, без загрузки Bronze заново. После UPDATE у suppkey=1 появилось checked. Откат снапшота восстановил данные; схема с review_note осталась.'],
 ['Восстановление данных','Прочитан исходный snapshot, затем выполнен rollback_to_snapshot. Три восстановленные строки точно совпали с историческим чтением. Изменения демонстрировались на отдельной таблице, бизнес-витрина и исходные данные не переписывались.']]))
snapshot=evolution['original_snapshot']
parts.append(f"```sql\n-- ID реально созданного исходного снапшота: {snapshot}\nSELECT * FROM silver.lakehouse.\"evolution_demo$snapshots\";\nSELECT suppkey,name,comment\nFROM silver.lakehouse.evolution_demo FOR VERSION AS OF {snapshot}\nORDER BY suppkey;\nALTER TABLE silver.lakehouse.evolution_demo\nEXECUTE rollback_to_snapshot(snapshot_id => {snapshot});\n```\n\nПри воспроизведении ID будет другим: его нужно получить из `$snapshots`, как делает `lab.py`. Доказательства: `results/evolution.json` и SQL-запросы с ID в `results/queries.jsonl`.\n")
parts.append('### 6.2. Apache Spark подключён к Bronze\n\nВыбран Spark: это второй независимый вычислительный движок с Iceberg REST и S3FileIO. Для будущих пакетных преобразований он полезнее отдельного BI-интерфейса; дополнительная серверная БД или копия Bronze не понадобилась. Docker-сервис описан в `compose.yaml` (профиль tools), подключение и действия в консоли — в `scripts/spark_bronze.py`.\n\nSpark проверен на первых трёх таблицах Bronze, до загрузки пяти дополнительных таблиц полного бенчмарка. Шаги: `SHOW TABLES IN bronze.lakehouse`, чтение первых пяти поставщиков, затем `GROUP BY brand` по всем 10 млн деталей. Указаны REST URI `http://catalog:8181/catalog`, warehouse `bronze`, S3 endpoint `http://storage:9000` и path-style access. Использован Spark '+spark['spark_version']+' с Iceberg runtime и AWS bundle 1.9.1.\n')
parts.append(table(['Действие Spark','Время, с','Результат'],[[k,fmt(v['wall_seconds']),f"{len(v['rows'])} строк результата"] for k,v in spark['queries'].items()]))
parts.append('Количество деталей в 25 группах Spark: '+integer(sum(row['parts'] for row in spark['queries']['aggregation']['rows']))+'. Пример: `Brand#11` содержит '+integer(spark['queries']['aggregation']['rows'][0]['parts'])+' деталей. Полный вывод консоли сохранён структурированно в `results/spark_bronze.json`.\n')
parts.append('## 7. Выводы\n\nМедальон разделил исходный набор, проверенную связь деталей с поставщиками и готовый бизнес-агрегат. Совпадение Q16 с исходным генератором подтвердило сохранение логики при преобразованиях, включая DISTINCT и исключение жалоб. Формат и кодек дали измеримые различия размера, времени записи и чтения; их выбор зависит от конкретной нагрузки. Повторные чтения и перезапуск движка показали, почему сравнение требует явно описанного состояния кэшей. Общий каталог Iceberg позволил Trino и Spark использовать одни и те же файлы Bronze.\n')
parts.append('## Приложение. Все измерения записи\n\n'+table(['Конфигурация','Таблица','Строк','МБ data','Время CTAS, с','Data-файлов','Query ID'],[[names[w['config']],w['table'],integer(w['rows']),mb(w['data_bytes']),fmt(w['wall_seconds']),w['files'],w['query_id']] for w in writes]))
parts.append('## Источники и воспроизводимость\n\nУчебные материалы преподавателя: форма «отчет.md», «варианты.md», таблица «вариант.xlsx» (строка «Баланов Артём», вариант 4), «Гайд-лейкхаус-win.pdf», «Гайд-2-медальон.pdf», «Презентация к Р1. Лаб..pptx», занятия 6–8. Числа из учебных слайдов не использовались вместо собственных измерений.\n\nДокументация: [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html), [Iceberg configuration](https://iceberg.apache.org/docs/latest/configuration/), [Lakekeeper](https://docs.lakekeeper.io/), [Silo](https://github.com/pgsty/silo). Текущая документация Trino может описывать свойства новее версии 476; фактические возможности проверены на запущенной версии. Все числа отчёта сформированы из файлов `results/`, запросы имеют идентификаторы Trino. Файлы больших таблиц не включены в Git и воспроизводятся скриптами.\n')
(ROOT/'отчет.md').write_text('\n'.join(parts),encoding='utf-8')
print('Report and chart built from',len(writes),'writes and',len(reads),'read measurements')
