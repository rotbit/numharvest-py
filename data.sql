CREATE TABLE IF NOT EXISTS phone_numbers (
  id               BIGSERIAL PRIMARY KEY,
  country_code     VARCHAR(8)   NOT NULL,
  national_number  VARCHAR(32)  NOT NULL,
  country          VARCHAR(64),
  region           VARCHAR(128),
  price_str        VARCHAR(50),
  original_price   INTEGER,
  adjusted_price   INTEGER,
  source_url       TEXT,
  source           VARCHAR(50),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (country_code, national_number)
);

-- 常用索引（可选但推荐）
CREATE INDEX IF NOT EXISTS idx_phone_updated ON phone_numbers(updated_at);
CREATE INDEX IF NOT EXISTS idx_phone_source  ON phone_numbers(source);
CREATE INDEX IF NOT EXISTS idx_phone_region  ON phone_numbers(country, region);
