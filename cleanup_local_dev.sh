#!/usr/bin/env bash
# =============================================================================
# cleanup_local_dev.sh - Remove local development files (GCP-only deployment)
# =============================================================================
# This script removes files only needed for local PostgreSQL development.
# Run this if you ONLY use GCP Dataflow and don't need local testing.
#
# Files removed:
#   - pipeline/main.py (local DirectRunner + PostgreSQL version)
#   - pipeline/config.py (PostgreSQL connection config)
#   - docker-compose.yml (local PostgreSQL container)
#   - Dockerfile (local development image, not Dockerfile.flex)
#   - db/ (PostgreSQL schema files)
#   - airflow/ (local Airflow DAGs - not needed for GCP)
#
# Files KEPT (REQUIRED for GCP):
#   - pipeline/gcp_main.py (Flex Template entrypoint)
#   - pipeline/gcp_batch_main.py (batch pipeline logic)
#   - pipeline/gcp_stream_main.py (streaming pipeline logic)
#   - pipeline/transforms.py (fraud detection logic)
#   - pipeline/schemas.py (data models)
#   - Dockerfile.flex (production Flex Template image)
#   - gcp/ (all GCP deployment scripts)
# =============================================================================

set -euo pipefail

echo "════════════════════════════════════════════════════════════════════════"
echo " GCP-Only Cleanup - Remove Local Development Files"
echo "════════════════════════════════════════════════════════════════════════"
echo ""
echo "⚠️  WARNING: This will remove:"
echo "   • pipeline/main.py (local DirectRunner version)"
echo "   • pipeline/config.py (PostgreSQL config)"
echo "   • docker-compose.yml (local PostgreSQL)"
echo "   • Dockerfile (local dev image)"
echo "   • db/ (PostgreSQL schema)"
echo "   • airflow/ (local Airflow DAGs)"
echo ""
echo "   These files are ONLY needed for local development."
echo "   Your GCP Dataflow deployment will NOT be affected."
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cancelled."
    exit 0
fi

echo ""
echo "🗑️  Removing local development files..."

# Remove local pipeline version
if [ -f "pipeline/main.py" ]; then
    git rm pipeline/main.py
    echo "   ✅ Removed pipeline/main.py"
fi

# Remove PostgreSQL config
if [ -f "pipeline/config.py" ]; then
    git rm pipeline/config.py
    echo "   ✅ Removed pipeline/config.py"
fi

# Remove local Docker files
if [ -f "docker-compose.yml" ]; then
    git rm docker-compose.yml
    echo "   ✅ Removed docker-compose.yml"
fi

if [ -f "Dockerfile" ]; then
    git rm Dockerfile
    echo "   ✅ Removed Dockerfile (kept Dockerfile.flex)"
fi

# Remove PostgreSQL schema directory
if [ -d "db" ]; then
    git rm -r db
    echo "   ✅ Removed db/"
fi

# Remove local Airflow DAGs
if [ -d "airflow" ]; then
    git rm -r airflow
    echo "   ✅ Removed airflow/"
fi

# Update .dockerignore to remove references to removed files
if [ -f ".dockerignore" ]; then
    # Keep .dockerignore as it's still used by Dockerfile.flex
    echo "   ⚠️  Kept .dockerignore (still used by Dockerfile.flex)"
fi

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "📊 Summary:"
echo "   REMOVED: Local development files (PostgreSQL, DirectRunner, Airflow)"
echo "   KEPT:    GCP production files (Flex Template, Dataflow scripts)"
echo ""
echo "📝 Next steps:"
echo "   1. Review changes: git status"
echo "   2. Commit: git commit -m 'chore: remove local dev files (GCP-only)'"
echo "   3. Push: git push origin main"
echo ""
echo "🚀 Your GCP deployment is unaffected and ready to use!"
