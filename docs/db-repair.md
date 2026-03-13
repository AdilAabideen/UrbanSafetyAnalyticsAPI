# Database Repair Runbook

This project keeps PostgreSQL in Docker. If analytics starts returning repeated `503` responses and the database logs show internal errors such as `shared buffer hash table corrupted`, use this workflow before rebuilding data.

## 1. Recreate the local DB container without deleting data

```bash
docker compose up -d --build --force-recreate db
```

The named `pgdata` volume is preserved. Do not use `docker compose down -v` unless you intend to delete the database.

## 2. Run the non-destructive repair workflow

```bash
python scripts/repair_postgres.py
```

What it does:

- checks that PostgreSQL is accepting connections
- enables `amcheck`
- runs `pg_amcheck` against `public.crime_events`
- runs `REINDEX TABLE CONCURRENTLY public.crime_events`
- runs `ANALYZE VERBOSE public.crime_events`
- runs `pg_amcheck` again

To run checks only:

```bash
python scripts/repair_postgres.py --check-only
```

## 3. Review logs

```bash
docker compose logs db --tail=200
```

Look for repeated internal errors, background worker crashes, or startup/platform warnings.

## 4. If corruption persists

1. Take a logical dump if the database is still readable.
2. Recreate the Postgres volume and reload data with the project scripts.
3. Re-run the repair workflow after reload.

If you are on Apple Silicon, keep the local DB image native to the host architecture. This repo now builds PostgreSQL/PostGIS locally instead of relying on an emulated image.

## 5. Collation version mismatch after image upgrades

If PostgreSQL warns that the database was created with an older collation version than the current container provides, rebuild the affected indexes and then refresh the recorded collation version.

Typical commands:

```bash
docker compose exec -T db psql -U app -d urban_risk -c "REINDEX DATABASE CONCURRENTLY urban_risk;"
docker compose exec -T db psql -U app -d urban_risk -c "ALTER DATABASE urban_risk REFRESH COLLATION VERSION;"
```

Do not refresh the collation version metadata before rebuilding the indexes that depend on the old collation.
