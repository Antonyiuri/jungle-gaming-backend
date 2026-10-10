CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL,
    currency TEXT NOT NULL CHECK (currency IN ('BRL', 'USD', 'EUR')),
    balance_minor BIGINT NOT NULL CHECK (balance_minor >= 0),
    version BIGINT NOT NULL CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (player_id, currency),
    UNIQUE (id, currency)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,
    origin TEXT NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    provider_id TEXT,
    external_transaction_id TEXT,
    idempotency_key TEXT,
    payload_hash TEXT,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    player_id UUID NOT NULL,
    round_id TEXT,
    game_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
    currency TEXT NOT NULL CHECK (currency IN ('BRL', 'USD', 'EUR')),
    reference_external_id TEXT,
    reference_transaction_id UUID REFERENCES wager_transactions(id),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code TEXT,
    result JSONB,
    first_pending_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (provider_id, idempotency_key),
    UNIQUE (provider_id, external_transaction_id),
    UNIQUE (id, wallet_id, currency, amount_minor),
    CHECK ((kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)),
    CHECK ((origin = 'INTERNAL' AND kind = 'OPENING' AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL AND reference_external_id IS NULL AND reference_transaction_id IS NULL)
        OR (origin = 'EXTERNAL' AND kind <> 'OPENING' AND provider_id IS NOT NULL AND provider_id <> '' AND external_transaction_id IS NOT NULL AND external_transaction_id <> '' AND idempotency_key IS NOT NULL AND idempotency_key <> '' AND payload_hash IS NOT NULL AND length(payload_hash) = 64 AND round_id IS NOT NULL AND game_id IS NOT NULL)),
    CHECK ((kind IN ('REFUND', 'ROLLBACK') AND reference_external_id IS NOT NULL) OR kind NOT IN ('REFUND', 'ROLLBACK')),
    CHECK (kind NOT IN ('BET', 'LOSS') OR reference_external_id IS NULL),
    CHECK (status <> 'PENDING_REFERENCE' OR (reference_external_id IS NOT NULL AND first_pending_at IS NOT NULL AND expires_at IS NOT NULL AND next_attempt_at IS NOT NULL)),
    CHECK (status NOT IN ('REJECTED', 'FAILED') OR failure_code IS NOT NULL)
);

CREATE UNIQUE INDEX one_opening_per_wallet ON wager_transactions(wallet_id) WHERE kind = 'OPENING';
CREATE UNIQUE INDEX one_successful_reversal ON wager_transactions(reference_transaction_id) WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';
CREATE INDEX due_references ON wager_transactions(next_attempt_at) WHERE status = 'PENDING_REFERENCE';

CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL,
    transaction_id UUID NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency TEXT NOT NULL,
    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),
    wallet_version BIGINT NOT NULL CHECK (wallet_version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (wallet_id, transaction_id),
    UNIQUE (wallet_id, wallet_version),
    FOREIGN KEY (wallet_id, currency) REFERENCES wallets(id, currency),
    FOREIGN KEY (transaction_id, wallet_id, currency, amount_minor) REFERENCES wager_transactions(id, wallet_id, currency, amount_minor),
    CHECK ((direction = 'CREDIT' AND balance_after::NUMERIC = balance_before::NUMERIC + amount_minor::NUMERIC)
        OR (direction = 'DEBIT' AND balance_after::NUMERIC = balance_before::NUMERIC - amount_minor::NUMERIC))
);

CREATE TABLE inbox_messages (
    consumer_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    transaction_id UUID REFERENCES wager_transactions(id),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    disposition TEXT CHECK (disposition IN ('ACK', 'DLQ')),
    PRIMARY KEY (consumer_name, message_id),
    CHECK ((completed_at IS NULL AND disposition IS NULL) OR (completed_at IS NOT NULL AND disposition IS NOT NULL AND transaction_id IS NOT NULL))
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
    event_type TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    UNIQUE (transaction_id, event_type, version)
);
CREATE INDEX due_outbox ON outbox_events(next_attempt_at) WHERE published_at IS NULL;

CREATE FUNCTION deny_ledger_mutation() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'The wallet ledger is append-only' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger_entries FOR EACH ROW EXECUTE FUNCTION deny_ledger_mutation();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries FOR EACH STATEMENT EXECUTE FUNCTION deny_ledger_mutation();

CREATE FUNCTION guard_transaction_update() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'Terminal transactions are immutable' USING ERRCODE = '23514';
    END IF;
    IF (to_jsonb(NEW) - ARRAY['status','failure_code','result','reference_transaction_id','first_pending_at','expires_at','attempt_count','next_attempt_at','lease_token','lease_until','updated_at']) IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY['status','failure_code','result','reference_transaction_id','first_pending_at','expires_at','attempt_count','next_attempt_at','lease_token','lease_until','updated_at']) THEN
        RAISE EXCEPTION 'Transaction business fields are immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
        RAISE EXCEPTION 'Invalid transaction transition' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER transaction_update_guard BEFORE UPDATE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION guard_transaction_update();

CREATE FUNCTION guard_outbox_update() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.id, NEW.wallet_id, NEW.transaction_id, NEW.event_type, NEW.version, NEW.payload, NEW.occurred_at) IS DISTINCT FROM
       (OLD.id, OLD.wallet_id, OLD.transaction_id, OLD.event_type, OLD.version, OLD.payload, OLD.occurred_at) THEN
        RAISE EXCEPTION 'Event snapshots are immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_update_guard BEFORE UPDATE ON outbox_events FOR EACH ROW EXECUTE FUNCTION guard_outbox_update();

CREATE FUNCTION validate_wallet_movement() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    entry wallet_ledger_entries%ROWTYPE;
    current_wallet wallets%ROWTYPE;
BEGIN
    SELECT * INTO current_wallet FROM wallets WHERE id = NEW.id;
    IF (current_wallet.balance_minor, current_wallet.version) IS DISTINCT FROM (NEW.balance_minor, NEW.version) THEN
        RAISE EXCEPTION 'Only one wallet movement is allowed per SQL transaction' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.version <> 1 THEN
            RAISE EXCEPTION 'A new wallet must have version one' USING ERRCODE = '23514';
        END IF;
        IF NEW.balance_minor = 0 THEN
            IF EXISTS (SELECT 1 FROM wallet_ledger_entries WHERE wallet_id = NEW.id) THEN
                RAISE EXCEPTION 'An empty opening cannot have ledger entries' USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END IF;
        SELECT * INTO entry FROM wallet_ledger_entries WHERE wallet_id = NEW.id AND wallet_version = 1;
        IF NOT FOUND OR entry.direction <> 'CREDIT' OR entry.balance_before <> 0 OR entry.balance_after <> NEW.balance_minor OR
           NOT EXISTS (SELECT 1 FROM wager_transactions WHERE id = entry.transaction_id AND kind = 'OPENING' AND status = 'PROCESSED') THEN
            RAISE EXCEPTION 'A positive opening requires its ledger entry' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at) IS DISTINCT FROM (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
            RAISE EXCEPTION 'Wallet identity is immutable' USING ERRCODE = '23514';
        END IF;
        IF NEW.balance_minor = OLD.balance_minor THEN
            IF NEW.version <> OLD.version THEN
                RAISE EXCEPTION 'A version change requires a balance change' USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END IF;
        IF NEW.version::NUMERIC <> OLD.version::NUMERIC + 1 THEN
            RAISE EXCEPTION 'A movement must advance the wallet version once' USING ERRCODE = '23514';
        END IF;
        SELECT * INTO entry FROM wallet_ledger_entries WHERE wallet_id = NEW.id AND wallet_version = NEW.version;
        IF NOT FOUND OR entry.balance_before <> OLD.balance_minor OR entry.balance_after <> NEW.balance_minor THEN
            RAISE EXCEPTION 'A balance change requires the matching ledger entry' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wallet_movement_consistency AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_wallet_movement();

CREATE FUNCTION validate_transaction_financial_state() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    operation wager_transactions%ROWTYPE;
    original wager_transactions%ROWTYPE;
    target wallets%ROWTYPE;
    entry wallet_ledger_entries%ROWTYPE;
    expected_direction TEXT;
BEGIN
    -- Read the final row; NEW may represent the earlier PENDING insertion.
    SELECT * INTO operation FROM wager_transactions WHERE id = NEW.id;
    IF operation.status = 'PENDING' OR operation.result IS NULL THEN
        RAISE EXCEPTION 'An accepted transaction requires a durable result' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO entry FROM wallet_ledger_entries WHERE transaction_id = operation.id AND wallet_id = operation.wallet_id;
    IF operation.status <> 'PROCESSED' OR operation.kind = 'LOSS' THEN
        IF FOUND THEN
            RAISE EXCEPTION 'This transaction cannot have a ledger entry' USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
    END IF;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'A processed movement requires a ledger entry' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO target FROM wallets WHERE id = operation.wallet_id;
    IF (operation.player_id, operation.currency) IS DISTINCT FROM (target.player_id, target.currency) THEN
        RAISE EXCEPTION 'Processed transaction context does not match the wallet' USING ERRCODE = '23514';
    END IF;
    expected_direction := CASE WHEN operation.kind = 'BET' THEN 'DEBIT' ELSE 'CREDIT' END;
    IF operation.reference_external_id IS NOT NULL THEN
        SELECT * INTO original FROM wager_transactions WHERE id = operation.reference_transaction_id;
        IF NOT FOUND OR original.status <> 'PROCESSED' OR
           (operation.provider_id, operation.player_id, operation.wallet_id, operation.currency, operation.round_id, operation.game_id, operation.reference_external_id) IS DISTINCT FROM
           (original.provider_id, original.player_id, original.wallet_id, original.currency, original.round_id, original.game_id, original.external_transaction_id) THEN
            RAISE EXCEPTION 'Processed reference context is invalid' USING ERRCODE = '23514';
        END IF;
        IF operation.kind IN ('WIN', 'REFUND') AND original.kind <> 'BET' THEN
            RAISE EXCEPTION 'This operation requires a bet reference' USING ERRCODE = '23514';
        END IF;
        IF operation.kind IN ('REFUND', 'ROLLBACK') AND operation.amount_minor <> original.amount_minor THEN
            RAISE EXCEPTION 'A reversal must match the full original amount' USING ERRCODE = '23514';
        END IF;
        IF operation.kind = 'ROLLBACK' THEN
            IF original.kind NOT IN ('BET', 'WIN', 'REFUND') THEN
                RAISE EXCEPTION 'Invalid rollback reference kind' USING ERRCODE = '23514';
            END IF;
            expected_direction := CASE WHEN original.kind = 'BET' THEN 'CREDIT' ELSE 'DEBIT' END;
        END IF;
    END IF;
    IF entry.direction <> expected_direction THEN
        RAISE EXCEPTION 'Ledger direction does not match the operation' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER transaction_financial_consistency AFTER INSERT OR UPDATE ON wager_transactions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_transaction_financial_state();

CREATE FUNCTION validate_ledger_state() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    target wallets%ROWTYPE;
    operation wager_transactions%ROWTYPE;
BEGIN
    SELECT * INTO target FROM wallets WHERE id = NEW.wallet_id;
    SELECT * INTO operation FROM wager_transactions WHERE id = NEW.transaction_id;
    IF operation.status <> 'PROCESSED' OR operation.kind = 'LOSS' OR
       (target.version, target.balance_minor) IS DISTINCT FROM (NEW.wallet_version, NEW.balance_after) OR
       (NEW.wallet_version = 1 AND (operation.kind <> 'OPENING' OR NEW.balance_before <> 0 OR NEW.direction <> 'CREDIT')) OR
       (NEW.wallet_version <> 1 AND operation.kind = 'OPENING') THEN
        RAISE EXCEPTION 'Ledger entry does not match the committed financial state' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER ledger_financial_consistency AFTER INSERT ON wallet_ledger_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_ledger_state();

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jungle_app') THEN
        GRANT USAGE ON SCHEMA public TO jungle_app;
        GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, inbox_messages, outbox_events TO jungle_app;
        GRANT SELECT, INSERT ON wallet_ledger_entries TO jungle_app;
        REVOKE DELETE, TRUNCATE ON wallets, wager_transactions, inbox_messages, outbox_events FROM jungle_app;
        REVOKE UPDATE, DELETE, TRUNCATE ON wallet_ledger_entries FROM jungle_app;
    END IF;
END;
$$;
