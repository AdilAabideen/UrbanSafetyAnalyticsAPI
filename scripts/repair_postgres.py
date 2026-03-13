import argparse
import shlex
import subprocess
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


def _run(command, check=True):
    print(f"$ {shlex.join(command)}", flush=True)
    return subprocess.run(command, cwd=ROOT_DIR, check=check)


def _docker_compose_command(args, *extra):
    return ["docker", "compose", *args, *extra]


def _exec_psql(service, user, database, sql):
    _run(
        _docker_compose_command(
            ["exec", "-T", service],
            "psql",
            "-U",
            user,
            "-d",
            database,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        )
    )


def _pg_amcheck(service, user, database, table, check=True):
    return _run(
        _docker_compose_command(
            ["exec", "-T", service],
            "pg_amcheck",
            "-U",
            user,
            "-d",
            database,
            "--table",
            table,
        ),
        check=check,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Run non-destructive PostgreSQL repair steps for the local Docker database."
    )
    parser.add_argument("--service", default="db", help="Docker Compose service name")
    parser.add_argument("--user", default="app", help="Postgres user")
    parser.add_argument("--database", default="urban_risk", help="Postgres database name")
    parser.add_argument("--table", default="public.crime_events", help="Table to check and repair")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Run readiness + amcheck only, without reindex/analyze",
    )
    args = parser.parse_args()

    _run(_docker_compose_command(["exec", "-T", args.service], "pg_isready", "-U", args.user, "-d", args.database))
    _exec_psql(args.service, args.user, args.database, "CREATE EXTENSION IF NOT EXISTS amcheck;")
    initial_check = _pg_amcheck(args.service, args.user, args.database, args.table, check=args.check_only)

    if args.check_only:
        print("Check-only mode complete.")
        return

    if initial_check.returncode != 0:
        print("Initial pg_amcheck reported issues. Continuing with reindex/analyze repair steps.")

    _exec_psql(
        args.service,
        args.user,
        args.database,
        f"REINDEX TABLE CONCURRENTLY {args.table};",
    )
    _exec_psql(
        args.service,
        args.user,
        args.database,
        f"ANALYZE VERBOSE {args.table};",
    )
    _pg_amcheck(args.service, args.user, args.database, args.table)
    print("Repair workflow complete.")


if __name__ == "__main__":
    main()
