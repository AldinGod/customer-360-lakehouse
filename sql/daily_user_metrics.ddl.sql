CREATE TABLE IF NOT EXISTS daily_user_metrics (
    event_date       date        NOT NULL,
    user_id          bigint      NOT NULL,

    events_count     bigint      NOT NULL,
    page_view_count  bigint      NOT NULL,
    login_count      bigint      NOT NULL,
    add_to_cart_count bigint     NOT NULL,
    checkout_count   bigint      NOT NULL,
    error_count      bigint      NOT NULL,

    purchase_events  bigint      NOT NULL,
    error_events     bigint      NOT NULL,

    total_amount     double precision,
    first_event_ts   timestamptz,
    last_event_ts    timestamptz,

    CONSTRAINT pk_daily_user_metrics PRIMARY KEY (event_date, user_id)
);

