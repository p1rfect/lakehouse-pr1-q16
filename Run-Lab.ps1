param([ValidateSet('init','core','evolution','benchmark','inventory','audit','all','spark')][string]$Action='all')
$ErrorActionPreference='Stop'
Set-Location $PSScriptRoot
if ($Action -eq 'spark') {
    docker compose --profile tools up -d spark
    if ($LASTEXITCODE -ne 0) { throw 'Spark container failed to start' }
    docker compose exec -T spark /opt/spark/bin/spark-submit --driver-memory 1g --conf spark.jars.ivy=/opt/spark/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1 /lab/scripts/spark_bronze.py
} else {
    docker compose up -d storage postgres catalog trino
    if ($LASTEXITCODE -ne 0) { throw 'Lakehouse failed to start' }
    $ready=$false
    for ($attempt=0; $attempt -lt 90; $attempt++) {
        try {
            $info=Invoke-RestMethod 'http://localhost:18080/v1/info'
            if (-not $info.starting) { $ready=$true; break }
        } catch { }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) { throw 'Trino did not become ready within 180 seconds' }
    python scripts/lab.py $Action
}
if ($LASTEXITCODE -ne 0) { throw "Action $Action failed" }
