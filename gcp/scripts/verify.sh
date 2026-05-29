#!/usr/bin/env bash
# scripts/verify.sh — quick sanity-check of pipeline results in Postgres.
#
# Usage:
#   ./scripts/verify.sh         # show counts + alert breakdown
#   ./scripts/verify.sh wipe    # truncate all medallion tables (DESTRUCTIVE)
#   ./scripts/verify.sh shell   # drop into psql for ad-hoc queries
#
# Connects to host:5433 directly via psql (no `docker compose exec` needed).
# That dodges podman/docker socket weirdness entirely.

set -euo pipefail

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-fraud_app}"
PG_DB="${PG_DB:-fraud_detection}"
export PGPASSWORD="${PG_PASSWORD:-fraud_app}"
# Skip GSSAPI/Kerberos negotiation (default on macOS triggers ugly errors)
export PGGSSENCMODE=disable

# Prefer host psql if installed (brew install libpq), else fall back to
# docker exec. Either way: no compose required.
if command -v psql >/dev/null 2>&1; then
    PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB")
else
    PSQL=(docker exec -i -e PGPASSWORD="$PGPASSWORD" fraud_pg \
          psql -U "$PG_USER" -d "$PG_DB")
fi

case "${1:-summary}" in
    wipe)
        echo "🧹 Truncating medallion tables..."
        "${PSQL[@]}" -c "TRUNCATE gold_fraud_alerts,
                                   silver_deduplicated_invoices,
                                   bronze_raw_events
                          RESTART IDENTITY;"
        echo "✅ Tables empty. Re-run: python -m pipeline.main"
        ;;
    shell)
        exec "${PSQL[@]}"
        ;;
    summary|*)
        echo "===== ROW COUNTS ====="
        "${PSQL[@]}" -c "
            SELECT 'bronze_raw_events'             AS layer, COUNT(*) FROM bronze_raw_events
            UNION ALL
            SELECT 'silver_deduplicated_invoices', COUNT(*) FROM silver_deduplicated_invoices
            UNION ALL
            SELECT 'gold_fraud_alerts',            COUNT(*) FROM gold_fraud_alerts
            ORDER BY 1;"

        echo "===== ALERT BREAKDOWN ====="
        "${PSQL[@]}" -c "
            SELECT rule_name, severity, COUNT(*) AS alerts
            FROM gold_fraud_alerts
            GROUP BY 1, 2
            ORDER BY 1, 2;"

        echo "===== TOP 5 CRITICAL ANOMALIES ====="
        "${PSQL[@]}" -c "
            SELECT invoice_id, vendor_id, severity, reason
            FROM gold_fraud_alerts
            WHERE severity = 'CRITICAL'
            LIMIT 5;"
        ;;
esac
