#!/usr/bin/env bash
# Prints the SpiceDB bearer token on stdout. Used by both:
#   - the workflow pre-step (to seed SKYRAMP_TEST_TOKEN in $GITHUB_ENV)
#   - the skyramp/testbot action's authTokenCommand (backup token fetcher)
#
# SpiceDB uses a static preshared key (set via SPICEDB_GRPC_PRESHARED_KEY in
# docker-compose.testbot.yaml) — no token-exchange endpoint like Mealie.
# Kept as a separate script so the YAML/Skyramp runner never has to inline
# multi-line shell, matching mealie's get-token.sh pattern.

set -euo pipefail
echo "super-secret-key"
