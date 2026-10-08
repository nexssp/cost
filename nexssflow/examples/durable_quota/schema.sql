-- PostgreSQL schema for the durable prepaid-wallet NFlow example.
-- Apply through the deployment's migration process before serving requests.

CREATE TABLE IF NOT EXISTS cost_tenants (
  tenant_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cost_users (
  tenant_id TEXT NOT NULL REFERENCES cost_tenants(tenant_id) ON DELETE RESTRICT,
  user_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS cost_wallets (
  wallet_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  currency CHAR(3) NOT NULL,
  balance_micros BIGINT NOT NULL CHECK (balance_micros >= 0),
  reserved_micros BIGINT NOT NULL DEFAULT 0 CHECK (reserved_micros >= 0),
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'closed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (tenant_id, user_id, currency),
  FOREIGN KEY (tenant_id, user_id) REFERENCES cost_users(tenant_id, user_id) ON DELETE RESTRICT,
  CHECK (reserved_micros <= balance_micros)
);

CREATE TABLE IF NOT EXISTS cost_wallet_reservations (
  id TEXT PRIMARY KEY,
  wallet_id TEXT NOT NULL REFERENCES cost_wallets(wallet_id) ON DELETE RESTRICT,
  amount_micros BIGINT NOT NULL CHECK (amount_micros >= 0),
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS cost_wallet_reservations_expiry_idx
  ON cost_wallet_reservations(wallet_id, expires_at);

-- Immutable financial entries. Amount is always positive; entry_type determines
-- whether a credit increases or a debit decreases the wallet balance.
CREATE TABLE IF NOT EXISTS cost_wallet_entries (
  id BIGSERIAL PRIMARY KEY,
  wallet_id TEXT NOT NULL REFERENCES cost_wallets(wallet_id) ON DELETE RESTRICT,
  entry_type TEXT NOT NULL CHECK (entry_type IN ('credit', 'debit')),
  amount_micros BIGINT NOT NULL CHECK (amount_micros > 0),
  idempotency_key TEXT NOT NULL,
  source TEXT,
  domain TEXT,
  operation TEXT,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (wallet_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS cost_wallet_entries_wallet_time_idx
  ON cost_wallet_entries(wallet_id, recorded_at);

-- Optional post-hoc usage facts. These do not alter wallet balances.
CREATE TABLE IF NOT EXISTS cost_wallet_usage_events (
  id BIGSERIAL PRIMARY KEY,
  wallet_id TEXT NOT NULL REFERENCES cost_wallets(wallet_id) ON DELETE RESTRICT,
  domain TEXT NOT NULL,
  operation TEXT NOT NULL,
  cost_micros BIGINT NOT NULL CHECK (cost_micros > 0),
  currency CHAR(3) NOT NULL,
  event_id TEXT,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS cost_wallet_usage_events_wallet_time_idx
  ON cost_wallet_usage_events(wallet_id, recorded_at);

CREATE UNIQUE INDEX IF NOT EXISTS cost_wallet_usage_event_id_idx
  ON cost_wallet_usage_events(wallet_id, event_id) WHERE event_id IS NOT NULL;

-- Idempotent wallet top-up. Call only from a trusted billing/operations service;
-- never expose arbitrary credits to an untrusted Flow request.
CREATE OR REPLACE FUNCTION cost_credit_wallet(
  p_wallet_id TEXT,
  p_amount_micros BIGINT,
  p_idempotency_key TEXT,
  p_source TEXT DEFAULT 'top_up'
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  existing_type TEXT;
  existing_amount BIGINT;
  new_balance BIGINT;
BEGIN
  IF p_amount_micros <= 0 THEN
    RAISE EXCEPTION 'credit amount must be positive';
  END IF;
  IF p_idempotency_key IS NULL OR btrim(p_idempotency_key) = '' THEN
    RAISE EXCEPTION 'idempotency key is required';
  END IF;

  PERFORM 1 FROM cost_wallets WHERE wallet_id = p_wallet_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wallet % does not exist', p_wallet_id;
  END IF;

  INSERT INTO cost_wallet_entries(wallet_id, entry_type, amount_micros, idempotency_key, source)
  VALUES (p_wallet_id, 'credit', p_amount_micros, p_idempotency_key, p_source)
  ON CONFLICT (wallet_id, idempotency_key) DO NOTHING;

  IF FOUND THEN
    UPDATE cost_wallets
    SET balance_micros = balance_micros + p_amount_micros,
        updated_at = CURRENT_TIMESTAMP
    WHERE wallet_id = p_wallet_id
    RETURNING balance_micros INTO new_balance;
    RETURN new_balance;
  END IF;

  SELECT entry_type, amount_micros
    INTO existing_type, existing_amount
  FROM cost_wallet_entries
  WHERE wallet_id = p_wallet_id AND idempotency_key = p_idempotency_key;
  IF existing_type <> 'credit' OR existing_amount <> p_amount_micros THEN
    RAISE EXCEPTION 'idempotency key % was already used for a different wallet entry', p_idempotency_key;
  END IF;
  SELECT balance_micros INTO new_balance FROM cost_wallets WHERE wallet_id = p_wallet_id;
  RETURN new_balance;
END;
$$;

-- PostgreSQL grants EXECUTE on new functions to PUBLIC by default. Remove it;
-- explicitly grant execution only to the trusted billing/top-up role.
REVOKE ALL ON FUNCTION cost_credit_wallet(TEXT, BIGINT, TEXT, TEXT) FROM PUBLIC;
