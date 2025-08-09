MERGE INTO API_INGEST t
USING (
  SELECT
    %(source)s::STRING           AS source,
    %(endpoint)s::STRING         AS endpoint,
    PARSE_JSON(%(params)s)       AS params,
    PARSE_JSON(%(payload)s)      AS payload,
    %(phash)s::STRING            AS payload_hash
) s
ON t.payload_hash = s.payload_hash
WHEN NOT MATCHED THEN
  INSERT (source, endpoint, params, payload, payload_hash)
  VALUES (s.source, s.endpoint, s.params, s.payload, s.payload_hash);