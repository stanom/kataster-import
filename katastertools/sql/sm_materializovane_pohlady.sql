-- Materializované pohľady nad importovanými kat. dátami

-- ###################################################### --
-- ### FINÁLNY materializovaný náhľad "SK_KN_C"
-- ### vlastníci, správcovia, nájomcovia v samostatných stĺpcoch
SELECT NOW();
CREATE MATERIALIZED VIEW kataster."SK_KN_C"  AS
  SELECT
    utj.okres
    ,COALESCE(utj.okres_nazov,'') AS nazov_okresu
    ,p.ku
    ,COALESCE(utj.ku_nazov,'') AS nazov_ku
--    ,p.parckey
    ,COALESCE(kl.parcela,'') AS parcela
    ,p.cpa
    ,COALESCE(csl_ump.popis2,'') AS ump
    ,p.drp
    ,p.vym
    ,p.don
    ,p.clv
--    ,COUNT(v.vla) AS pocet_vlastnikov
    ,CONCAT(COUNT(*) FILTER (WHERE "v".tuc=1)) AS pocet_vlastnikov
--    ,STRING_AGG(DISTINCT (((v.cit::text || CHR(47)) || v.men::text) || ' - '::text) || v.vla::text, CHR(10)) AS vlastnik
-- rýchly spôsob (14 minút) >>>
    ,COALESCE(STRING_AGG(CASE WHEN "v".tuc=1 AND "v".cit>0 AND "v".men>0 THEN E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' END , E'\n' /*ORDER BY "v".pcs::numeric*/),'') AS vlastnik
    ,COALESCE(STRING_AGG(CASE WHEN "v".tuc=2 THEN E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' END , E'\n' /*ORDER BY "v".pcs::numeric*/),'') AS spravca
    ,COALESCE(STRING_AGG(CASE WHEN "v".tuc=3 THEN E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' END , E'\n' /*ORDER BY "v".pcs::numeric*/),'') AS najomca
-- pomalý spôsob (niekoľko hodín) >>>
--    ,ARRAY_TO_STRING(array(SELECT pcs::text || E'. [' || vla::text || E' {' || cit::text || E'/' || men::text || E'}]' FROM kataster.kn_vl sub_v INNER JOIN ciselnik.tuc tuc ON(tuc.id=sub_v.tuc) WHERE (1=1 AND "sub_v".ku = p.ku AND "sub_v".clv = p.clv AND tuc.id=1) ), E'\r\n') AS vlastnik
    ,p.cel
    ,COUNT(DISTINCT u.uzi::text) AS pocet_uzivatelov
--    ,STRING_AGG(DISTINCT u.uzi::text, CHR(10)) AS uzivatel
    ,COALESCE(STRING_AGG(DISTINCT E'[' || "u".uzi::text || E']', E'\n'),'') AS uzivatel
    ,kl.gid
--    ,kl.geom
  FROM kataster.kn_pa p
     INNER JOIN ciselnik.ump csl_ump ON(csl_ump.id=p.ump)
     LEFT JOIN ciselnik.kataster utj ON p.ku = utj.ku::numeric
     LEFT JOIN kataster.kn_kladpar kl ON p.parckey::text = kl.parckey::text
--     LEFT JOIN kataster.kn_vl v ON p.ku = v.ku AND p.clv = v.clv
     LEFT JOIN kataster.kn_vl_unique v ON p.ku = v.ku AND p.clv = v.clv
--     LEFT JOIN kataster.kn_uz u ON p.ku = u.ku AND p.cel = u.cel
     LEFT JOIN kataster.kn_uz_unique u ON p.ku = u.ku AND p.cel = u.cel
  WHERE 1=1
--AND p.ku=852104 AND p.clv=2360 AND p.cpa=14830010
  GROUP BY
    utj.okres
    ,utj.okres_nazov
    ,p.ku
    ,utj.ku_nazov
--    ,p.parckey
    ,kl.parcela
    ,p.cpa
    ,csl_ump.popis2
    ,p.drp
    ,p.vym
    ,p.don
    ,p.clv
    ,p.cel
    ,kl.gid
WITH NO DATA;
CREATE INDEX IF NOT EXISTS "idx_SK_KN_C_ku" ON kataster."SK_KN_C"(ku);
CREATE INDEX IF NOT EXISTS "idx_SK_KN_C_okres" ON kataster."SK_KN_C"(okres);
CREATE INDEX IF NOT EXISTS "idx_SK_KN_C_gid" ON kataster."SK_KN_C"(gid);
VACUUM kataster."SK_KN_C";
SELECT NOW();
REFRESH MATERIALIZED VIEW kataster."SK_KN_C";
SELECT NOW();
VACUUM kataster."SK_KN_C";
-- >> 7967330 riadkov; za 16min. 45s.

SELECT pg_size_pretty(pg_relation_size('kataster."SK_KN_C"'));

-- ###################################################### --
-- ### FINÁLNY materializovaný náhľad "SK_KN_E"
-- ### vlastníci, správcovia, nájomcovia v samostatných stĺpcoch
SELECT NOW();
CREATE MATERIALIZED VIEW kataster."SK_KN_E" AS
  SELECT
    utj.okres
    ,COALESCE(utj.okres_nazov,'') AS nazov_okresu
    ,p.ku
    ,COALESCE(utj.ku_nazov,'') AS nazov_ku
--    ,p.parckey
    ,COALESCE(kl.parcela,'') AS parcela
    ,p.cpa
    ,COALESCE(csl_ump.popis2,'') AS ump
    ,p.drp
    ,p.vym
    ,p.don
    ,p.clv
--    ,COUNT(v.vla) AS pocet_vlastnikov
    ,CONCAT(COUNT(*) FILTER (WHERE "v".tuc=1 AND "v".cit>0 AND "v".men>0))::numeric AS pocet_vlastnikov
    ,COALESCE(STRING_AGG(E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' , E'\n') FILTER (WHERE "v".tuc=1 AND "v".cit>0 AND "v".men>0),'') AS vlastnik
    ,p.cel
    ,COUNT(DISTINCT u.uzi::text) AS pocet_uzivatelov
--    ,STRING_AGG(DISTINCT u.uzi::text, CHR(10)) AS uzivatel
    ,COALESCE(STRING_AGG(DISTINCT E'[' || "u".uzi::text || E']', E'\n'),'') AS uzivatel
    ,kl.gid
--    ,kl.geom
  FROM kataster.kn_ep p
     INNER JOIN ciselnik.ump csl_ump ON(csl_ump.id=p.ump)
     LEFT JOIN ciselnik.kataster utj ON p.ku = utj.ku::numeric
     LEFT JOIN kataster.kn_uov kl ON p.parckey::text = kl.parckey::text
--     LEFT JOIN kataster.kn_vl v ON p.ku = v.ku AND p.clv = v.clv
     LEFT JOIN kataster.kn_vl_unique v ON p.ku = v.ku AND p.clv = v.clv
--     LEFT JOIN kataster.kn_uz u ON p.ku = u.ku AND p.cel = u.cel
     LEFT JOIN kataster.kn_uz_unique u ON p.ku = u.ku AND p.cel = u.cel
  WHERE 1=1
  GROUP BY
    utj.okres
    ,utj.okres_nazov
    ,p.ku
    ,utj.ku_nazov
--    ,p.parckey
    ,kl.parcela
    ,p.cpa
    ,csl_ump.popis2
    ,p.drp
    ,p.vym
    ,p.don
    ,p.clv
    ,p.cel
    ,kl.gid
WITH NO DATA;
CREATE INDEX IF NOT EXISTS "idx_SK_KN_E_ku" ON kataster."SK_KN_E"(ku);
CREATE INDEX IF NOT EXISTS "idx_SK_KN_E_okres" ON kataster."SK_KN_E"(okres);
CREATE INDEX IF NOT EXISTS "idx_SK_KN_E_gid" ON kataster."SK_KN_E"(gid);
VACUUM kataster."SK_KN_E";
SELECT NOW();
REFRESH MATERIALIZED VIEW kataster."SK_KN_E";
SELECT NOW();
VACUUM kataster."SK_KN_E";
-- >> 8199522 riadkov; za 42min. 10s.

SELECT pg_size_pretty(pg_relation_size('kataster."SK_KN_E"'));
