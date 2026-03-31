-- trigger.sql
-- Hàm và trigger tự động cập nhật updated_at

-- =============================================================================
-- 1. FUNCTIONS
-- =============================================================================

BEGIN;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 2. TRIGGERS
-- =============================================================================

DROP TRIGGER IF EXISTS trg_fact_property_listing_set_updated_at ON fact_property_listing;
CREATE TRIGGER trg_fact_property_listing_set_updated_at
BEFORE UPDATE ON fact_property_listing
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
