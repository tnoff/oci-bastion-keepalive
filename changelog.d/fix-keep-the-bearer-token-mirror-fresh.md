fix: mirror the bearer token on every refresh, not once at startup — a one-shot copy froze at the first token and 401'd again once it expired
