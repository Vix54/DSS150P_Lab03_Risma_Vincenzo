\connect dss150p;

BEGIN;

ALTER TABLE curated.sales_order_lines DROP CONSTRAINT IF EXISTS chk_sol_quantity_range;
ALTER TABLE curated.sales_order_lines ADD CONSTRAINT chk_sol_quantity_range CHECK (quantity BETWEEN 1 AND 20);

ALTER TABLE curated.sales_order_lines DROP CONSTRAINT IF EXISTS chk_sol_unit_price_nonnegative;
ALTER TABLE curated.sales_order_lines ADD CONSTRAINT chk_sol_unit_price_nonnegative CHECK (unit_price >= 0);

ALTER TABLE curated.sales_order_lines DROP CONSTRAINT IF EXISTS chk_sol_discount_pct_range;
ALTER TABLE curated.sales_order_lines ADD CONSTRAINT chk_sol_discount_pct_range CHECK (discount_pct BETWEEN 0 AND 1);

ALTER TABLE curated.sales_order_lines DROP CONSTRAINT IF EXISTS chk_sol_amounts_nonnegative;
ALTER TABLE curated.sales_order_lines ADD CONSTRAINT chk_sol_amounts_nonnegative CHECK (gross_amount >= 0 AND discount_amount >= 0 AND net_amount >= 0);

ALTER TABLE curated.sales_order_lines DROP CONSTRAINT IF EXISTS chk_sol_net_amount_formula;
ALTER TABLE curated.sales_order_lines ADD CONSTRAINT chk_sol_net_amount_formula CHECK (net_amount = gross_amount - discount_amount);

ALTER TABLE curated.sales_order_lines DROP CONSTRAINT IF EXISTS chk_sol_status_allowed;
ALTER TABLE curated.sales_order_lines ADD CONSTRAINT chk_sol_status_allowed CHECK (status IN ('PENDING', 'PAID', 'PACKED', 'SHIPPED', 'DELIVERED', 'CANCELLED'));

ALTER TABLE curated.sales_order_lines DROP CONSTRAINT IF EXISTS chk_sol_record_hash_length;
ALTER TABLE curated.sales_order_lines ADD CONSTRAINT chk_sol_record_hash_length CHECK (char_length(record_hash) = 64);

COMMIT;
