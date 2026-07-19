-- ============================================================================
-- Logical replication artifacts consumed by Debezium (pgoutput plugin).
-- The publication scopes CDC to exactly the tables we want replicated.
-- ============================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
        CREATE PUBLICATION dbz_publication
            FOR TABLE public.customers, public.products, public.inventory,
                      public.orders, public.payments;
    END IF;
END $$;

-- Dedicated, least-privilege replication role.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_admin') THEN
        CREATE ROLE cdc_admin WITH LOGIN REPLICATION PASSWORD 'change_me_in_prod';
    END IF;
END $$;

GRANT USAGE ON SCHEMA public TO cdc_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_admin;

-- updated_at maintenance trigger (so UPDATEs advance the ordering column).
CREATE OR REPLACE FUNCTION public.touch_updated_at() RETURNS trigger AS $fn$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers','products','inventory','orders','payments'] LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_touch_%1$s ON public.%1$s;
             CREATE TRIGGER trg_touch_%1$s BEFORE UPDATE ON public.%1$s
             FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();', t);
    END LOOP;
END $$;
