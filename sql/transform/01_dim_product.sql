-- Type 1 upsert: insert new products, overwrite changed ones, leave the rest untouched.
INSERT INTO dw.dim_product (product_id, product_name, category, unit_price)
SELECT trim(s.product_id), trim(s.product_name), trim(s.category), s.unit_price::numeric(12, 2)
FROM staging.products s
WHERE s.batch_date = %(batch_date)s
  AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                  WHERE r.source_table = 'products' AND r.row_id = s._row_id)
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category,
    unit_price   = EXCLUDED.unit_price,
    updated_at   = now()
WHERE (dw.dim_product.product_name, dw.dim_product.category, dw.dim_product.unit_price)
      IS DISTINCT FROM (EXCLUDED.product_name, EXCLUDED.category, EXCLUDED.unit_price);
