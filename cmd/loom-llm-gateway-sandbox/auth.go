// Step-JWT verification. Mirrors loom.auth.verify_step_jwt
// (src/loom/auth.py) so this binary and the Python Control Plane
// agree on the wire format. Any drift between the two is an
// observability nightmare — bump _STEP_JWT_PREFIX and both sides
// at once if you change the shape.

package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Prefix on every step-JWT. Lets routers distinguish step tokens
// from team tokens (`loom_team_<hex>`) without a parse attempt.
// MUST equal `loom.auth._STEP_JWT_PREFIX`.
const stepJWTPrefix = "loom_step_"

// stepSessionClaims is the subset of the JWT payload this binary
// reads. The Control Plane mints additional fields (iss, sub, iat,
// scopes); we don't validate them — that's the gateway-router's
// job. We only need to confirm the bearer is who they claim, and
// the token hasn't expired.
type stepSessionClaims struct {
	// jwt.RegisteredClaims handles exp/iat/iss/etc and provides
	// the time-bound checks the standard validator runs.
	jwt.RegisteredClaims
	TeamID               string `json:"team_id"`
	TrialID              string `json:"trial_id"`
	StepID               string `json:"step_id"`
	ProviderConnectionID string `json:"provider_connection_id,omitempty"`
}

// verifyStepJWT parses+validates a `loom_step_*` token. Returns the
// decoded claims on success; an error whose message DOES NOT leak
// the token bytes on failure (logged claims should never include
// the raw token).
func verifyStepJWT(token string, signingKey []byte) (*stepSessionClaims, error) {
	if !strings.HasPrefix(token, stepJWTPrefix) {
		return nil, errors.New("not a step JWT (missing prefix)")
	}
	body := strings.TrimPrefix(token, stepJWTPrefix)

	claims := &stepSessionClaims{}
	_, err := jwt.ParseWithClaims(body, claims, func(t *jwt.Token) (any, error) {
		// Reject any non-HS256 — `alg: none` confusion attacks +
		// asymmetric-key confusion are the standard JWT bugs.
		if t.Method.Alg() != "HS256" {
			return nil, fmt.Errorf("unexpected alg %q", t.Method.Alg())
		}
		return signingKey, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		return nil, fmt.Errorf("verify: %w", err)
	}
	if claims.TeamID == "" || claims.TrialID == "" || claims.StepID == "" {
		return nil, errors.New("missing required claim (team_id/trial_id/step_id)")
	}
	// ParseWithClaims validates exp and nbf when present. Recheck expiry
	// before returning in case it elapsed during the remaining checks.
	if claims.ExpiresAt != nil && time.Now().After(claims.ExpiresAt.Time) {
		return nil, errors.New("token expired")
	}
	return claims, nil
}

// extractBearerToken pulls the token out of an Authorization header
// OR an x-api-key header (Anthropic dialect). Returns "" if neither
// is present. Per Phase B spec, both forms are accepted because
// the sandbox SDK's choice of header depends on the provider
// dialect it's emulating.
func extractBearerToken(authHeader, apiKeyHeader string) string {
	if authHeader != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(authHeader, prefix) {
			return strings.TrimPrefix(authHeader, prefix)
		}
		// Some SDKs send `Bearer<space><token>` with extra spacing.
		// Trim and try again so we don't 401 on benign whitespace.
		trimmed := strings.TrimSpace(authHeader)
		if strings.HasPrefix(trimmed, prefix) {
			return strings.TrimPrefix(trimmed, prefix)
		}
		return authHeader
	}
	return apiKeyHeader
}
