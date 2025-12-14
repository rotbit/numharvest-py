CREATE TABLE IF NOT EXISTS phone_numbers (
  id               BIGSERIAL PRIMARY KEY,
  country_code     VARCHAR(8)   NOT NULL,
  area_code        VARCHAR(32) NOT NULL,
  local_number     VARCHAR(32)  NOT NULL,
  country          VARCHAR(64),
  state_code       VARCHAR(128),
  state_name       VARCHAR(128),
  price_str        VARCHAR(50),
  price            INTEGER,
  source_url       TEXT,
  source           VARCHAR(50),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (area_code, local_number)
);

-- 常用索引（可选但推荐）
CREATE INDEX IF NOT EXISTS idx_phone_source  ON phone_numbers(source);
CREATE INDEX IF NOT EXISTS idx_phone_state_code  ON phone_numbers(state_code);
