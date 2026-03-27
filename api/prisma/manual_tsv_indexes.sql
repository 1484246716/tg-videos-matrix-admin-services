-- tsvector 列
ALTER TABLE search_documents ADD COLUMN IF NOT EXISTS search_tsv tsvector;

-- tsvector 自动更新触发器
CREATE OR REPLACE FUNCTION search_documents_tsv_trigger() RETURNS trigger AS $$
BEGIN
  NEW.search_tsv := to_tsvector('simple', COALESCE(NEW.search_text, ''));
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tsvupdate ON search_documents;
CREATE TRIGGER tsvupdate BEFORE INSERT OR UPDATE OF search_text
  ON search_documents FOR EACH ROW EXECUTE FUNCTION search_documents_tsv_trigger();

-- GIN 索引
CREATE INDEX IF NOT EXISTS idx_search_documents_tsv ON search_documents USING GIN(search_tsv);
CREATE INDEX IF NOT EXISTS idx_search_documents_actors ON search_documents USING GIN(actors);
CREATE INDEX IF NOT EXISTS idx_search_documents_aliases ON search_documents USING GIN(aliases);
CREATE INDEX IF NOT EXISTS idx_search_documents_keywords ON search_documents USING GIN(keywords);
CREATE INDEX IF NOT EXISTS idx_search_documents_ext ON search_documents USING GIN(ext);
