-- Reference data only: each initial wallet and its opening-credit entry are
-- created together once. Reapplying this file never resets an existing balance.
INSERT INTO cost_tenants(tenant_id, display_name)
VALUES ('tn_acme', 'Acme Support'), ('tn_beta', 'Beta Services')
ON CONFLICT (tenant_id) DO NOTHING;

INSERT INTO cost_users(tenant_id, user_id, display_name)
VALUES
  ('tn_acme', 'usr_alex', 'Alex Rivera'),
  ('tn_acme', 'usr_sam', 'Sam Chen'),
  ('tn_beta', 'usr_maria', 'Maria Nowak')
ON CONFLICT (tenant_id, user_id) DO NOTHING;

WITH created AS (
  INSERT INTO cost_wallets(wallet_id, tenant_id, user_id, currency, balance_micros)
  VALUES ('wal_acme_alex_usd', 'tn_acme', 'usr_alex', 'USD', 500000)
  ON CONFLICT (tenant_id, user_id, currency) DO NOTHING
  RETURNING wallet_id, balance_micros
)
INSERT INTO cost_wallet_entries(wallet_id, entry_type, amount_micros, idempotency_key, source)
SELECT wallet_id, 'credit', balance_micros, 'opening:wal_acme_alex_usd', 'opening_balance' FROM created;

WITH created AS (
  INSERT INTO cost_wallets(wallet_id, tenant_id, user_id, currency, balance_micros)
  VALUES ('wal_acme_sam_usd', 'tn_acme', 'usr_sam', 'USD', 150000)
  ON CONFLICT (tenant_id, user_id, currency) DO NOTHING
  RETURNING wallet_id, balance_micros
)
INSERT INTO cost_wallet_entries(wallet_id, entry_type, amount_micros, idempotency_key, source)
SELECT wallet_id, 'credit', balance_micros, 'opening:wal_acme_sam_usd', 'opening_balance' FROM created;

WITH created AS (
  INSERT INTO cost_wallets(wallet_id, tenant_id, user_id, currency, balance_micros)
  VALUES ('wal_beta_maria_usd', 'tn_beta', 'usr_maria', 'USD', 1000000)
  ON CONFLICT (tenant_id, user_id, currency) DO NOTHING
  RETURNING wallet_id, balance_micros
)
INSERT INTO cost_wallet_entries(wallet_id, entry_type, amount_micros, idempotency_key, source)
SELECT wallet_id, 'credit', balance_micros, 'opening:wal_beta_maria_usd', 'opening_balance' FROM created;
