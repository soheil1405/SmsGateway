-- وضعیت publishing برای claim چندنمونه‌ای outbox + حذف رزرو بلااستفاده
ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_events_status_check;
ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_status_check
    CHECK (status IN ('pending', 'publishing', 'published', 'failed'));

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

DROP TABLE IF EXISTS balance_reservations;
