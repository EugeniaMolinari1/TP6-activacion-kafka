// Each service applies this schema only in its OWN database, using its OWN role.
export const commonSchema = `
CREATE TABLE IF NOT EXISTS processed_events (
  event_id uuid PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS processed_age ON processed_events(processed_at);
CREATE TABLE IF NOT EXISTS outbox (
  seq bigserial PRIMARY KEY, event_id uuid UNIQUE NOT NULL, topic text NOT NULL,
  customer_id text NOT NULL, event jsonb NOT NULL,
  status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SENT')),
  created_at timestamptz NOT NULL DEFAULT now(), sent_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(status,seq);
`;
export const schemas: Record<string, string> = {
  "activation-api": `
CREATE TABLE IF NOT EXISTS activations (
 activation_id text PRIMARY KEY, customer_id text NOT NULL, plan_id text NOT NULL,
 channel text NOT NULL, simulate_failure text NOT NULL CHECK(simulate_failure IN ('none','billing','provisioning')),
 status text NOT NULL CHECK(status IN ('PENDING','IN_PROGRESS','ACTIVE','FAILED')),
 billing_status text CHECK(billing_status IN ('OK','FAILED')), provisioning_status text CHECK(provisioning_status IN ('OK','FAILED')),
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS activations_customer ON activations(customer_id);
CREATE TABLE IF NOT EXISTS activation_history (
 seq bigserial PRIMARY KEY, activation_id text NOT NULL REFERENCES activations(activation_id),
 event_id uuid NOT NULL UNIQUE, event jsonb NOT NULL, received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS history_activation ON activation_history(activation_id,seq);`,
  "billing-service": `
CREATE TABLE IF NOT EXISTS billing_accounts (
 activation_id text PRIMARY KEY, customer_id text NOT NULL, status text NOT NULL CHECK(status IN ('CREATED','CANCELLED')),
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS compensation_requests (activation_id text PRIMARY KEY, received_at timestamptz NOT NULL DEFAULT now());`,
  "provisioning-service": `CREATE TABLE IF NOT EXISTS provisioning_orders (
 activation_id text PRIMARY KEY, customer_id text NOT NULL, status text NOT NULL CHECK(status IN ('COMPLETED','FAILED')), created_at timestamptz NOT NULL DEFAULT now()
);`,
  "notification-service": `CREATE TABLE IF NOT EXISTS notifications (
 activation_id text NOT NULL, event_type text NOT NULL, event_id uuid UNIQUE NOT NULL,
 message_id text NOT NULL UNIQUE, sent_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(activation_id,event_type)
);`,
  "crm-analytics-service": `CREATE TABLE IF NOT EXISTS event_log (
 event_id uuid PRIMARY KEY, correlation_id text NOT NULL, event jsonb NOT NULL, received_at timestamptz NOT NULL DEFAULT now()
); CREATE INDEX IF NOT EXISTS event_log_correlation ON event_log(correlation_id);`,
  "loyalty-service": `CREATE TABLE IF NOT EXISTS loyalty_activations (
 activation_id text PRIMARY KEY, customer_id text NOT NULL, points integer NOT NULL CHECK(points=100), event_id uuid UNIQUE NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);`,
};
