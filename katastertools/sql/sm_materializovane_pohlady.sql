-- Materializované pohľady nad importovanými kat. dátami


-- ###################################################### --
-- ### materializovaný náhľadu reg_C_vla_uzi - 1. nástrel
SELECT NOW();
CREATE MATERIALIZED VIEW kataster."reg_C_vla_uzi" AS
  SELECT
    utj.okres AS cislo_okresu
    ,utj.okr_nazov AS nazov_okresu
    ,p.ku
    ,utj.ku_nazov AS nazov_ku
--    ,p.parckey
    ,kl.parcela
    ,p.cpa
    ,p.drp
    ,p.vym
    ,p.don
    ,p.clv
    ,count(v.vla) AS pocet_vlastnikov
--    ,string_agg(DISTINCT (((v.cit::text || chr(47)) || v.men::text) || ' - '::text) || v.vla::text, chr(10)) AS vlastnici
    ,string_agg("v".pcs::text || E'. [' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' , E'\n' ORDER BY "v".pcs::numeric) AS vlastnici
    ,p.cel
    ,count(DISTINCT u.uzi::text) AS pocet_uzivatelov
--    ,string_agg(DISTINCT u.uzi::text, chr(10)) AS uzivatelia
    ,string_agg(DISTINCT E'[' || "u".uzi::text || E']', E'\n') AS uzivatelia
    ,kl.gid
--    ,kl.geom
  FROM kn_pa p
     JOIN ciselnik.kataster utj ON p.ku = utj.kataster::numeric
     LEFT JOIN kn_kladpar kl ON p.parckey::text = kl.parckey::text
     LEFT JOIN kn_vl v ON p.ku = v.ku AND p.clv = v.clv
     LEFT JOIN kn_uz u ON p.ku = u.ku AND p.cel = u.cel
  WHERE 1=1
  GROUP BY
    utj.okres
    ,utj.okr_nazov
    ,p.ku
    ,utj.ku_nazov
--    ,p.parckey
    ,kl.parcela
    ,p.
    ,p.drp
    ,p.vym
    ,p.don
    ,p.clv
    ,p.cel
    ,kl.gid
WITH NO DATA;
CREATE INDEX IF NOT EXISTS "idx_reg_C_vla_uzi_ku" ON "reg_C_vla_uzi"(ku);
CREATE INDEX IF NOT EXISTS "idx_reg_C_vla_uzi_cislo_okresu" ON "reg_C_vla_uzi"(cislo_okresu);
CREATE INDEX IF NOT EXISTS "idx_reg_C_vla_uzi_gid" ON "reg_C_vla_uzi"(gid);
SELECT NOW();
REFRESH MATERIALIZED VIEW "reg_C_vla_uzi" ;
SELECT NOW();



-- ###################################################### --
-- ### materializovaný náhľadu reg_C_vla_uzi - 2. nástrel
CREATE MATERIALIZED VIEW kataster."reg_C_vla_uzi_2" AS
  SELECT
    utj.okres AS cislo_okresu
    ,utj.okr_nazov AS nazov_okresu
    ,p.ku
    ,utj.ku_nazov AS nazov_ku
--    ,p.parckey
    ,kl.parcela
    ,p.cpa
    ,csl_ump.popis2 AS ump
    ,p.drp
    ,p.vym
    ,p.don
    ,p.clv
    ,count(v.vla) AS pocet_vlastnikov
--    ,string_agg(DISTINCT (((v.cit::text || chr(47)) || v.men::text) || ' - '::text) || v.vla::text, chr(10)) AS vlastnici
-- rýchly spôsob (14 minút) >>>
    ,string_agg(CASE WHEN "v".tuc=1 THEN E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' END , E'\n' ORDER BY "v".pcs::numeric) AS vlastnici
-- pomalý spôsob (niekoľko hodín) >>>
--    ,array_to_string(array(    SELECT pcs::text || E'. [' || vla::text || E' {' || cit::text || E'/' || men::text || E'}]' FROM kataster.kn_vl sub_v INNER JOIN ciselnik.tuc tuc ON(tuc.id=sub_v.tuc) WHERE (1=1 AND "sub_v".ku = p.ku AND "sub_v".clv = p.clv AND tuc.id=1) ), E'\r\n') AS vlastnici
    ,p.cel
    ,count(DISTINCT u.uzi::text) AS pocet_uzivatelov
--    ,string_agg(DISTINCT u.uzi::text, chr(10)) AS uzivatelia
    ,string_agg(DISTINCT E'[' || "u".uzi::text || E']', E'\n') AS uzivatelia
    ,kl.gid
--    ,kl.geom
  FROM kataster.kn_pa p
     INNER JOIN ciselnik.ump csl_ump ON(csl_ump.id=p.ump)
     LEFT JOIN ciselnik.kataster utj ON p.ku = utj.kataster::numeric
     LEFT JOIN kataster.kn_kladpar kl ON p.parckey::text = kl.parckey::text
     LEFT JOIN kataster.kn_vl v ON p.ku = v.ku AND p.clv = v.clv
     LEFT JOIN kataster.kn_uz u ON p.ku = u.ku AND p.cel = u.cel
  WHERE 1=1
--AND p.ku=852104 AND p.clv=2360 AND p.cpa=14830010
  GROUP BY
    utj.okres
    ,utj.okr_nazov
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
CREATE INDEX IF NOT EXISTS "idx_reg_C_vla_uzi_2_ku" ON kataster."reg_C_vla_uzi_2"(ku);
CREATE INDEX IF NOT EXISTS "idx_reg_C_vla_uzi_2_cislo_okresu" ON kataster."reg_C_vla_uzi_2"(cislo_okresu);
CREATE INDEX IF NOT EXISTS "idx_reg_C_vla_uzi_2_gid" ON kataster."reg_C_vla_uzi_2"(gid);
SELECT NOW();
REFRESH MATERIALIZED VIEW kataster."reg_C_vla_uzi_2";
SELECT NOW();



-- ###################################################### --
-- ### FINÁLNY materializovaný náhľad "SK_KN_C"
-- ### vlastníci, správcovia, nájomcovia v samostatných stĺpcoch
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
--    ,count(v.vla) AS pocet_vlastnikov
    ,concat(count(*) filter (where "v".tuc=1)) AS pocet_vlastnikov
--    ,string_agg(DISTINCT (((v.cit::text || chr(47)) || v.men::text) || ' - '::text) || v.vla::text, chr(10)) AS vlastnici
-- rýchly spôsob (14 minút) >>>
    ,COALESCE(string_agg(CASE WHEN "v".tuc=1 THEN E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' END , E'\n' ORDER BY "v".pcs::numeric),'') AS vlastnik
    ,COALESCE(string_agg(CASE WHEN "v".tuc=2 THEN E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' END , E'\n' ORDER BY "v".pcs::numeric),'') AS spravca
    ,COALESCE(string_agg(CASE WHEN "v".tuc=3 THEN E'[' || "v".vla::text || E' {' || "v".cit::text || E'/' || "v".men::text || E'}]' END , E'\n' ORDER BY "v".pcs::numeric),'') AS najomca
-- pomalý spôsob (niekoľko hodín) >>>
--    ,array_to_string(array(    SELECT pcs::text || E'. [' || vla::text || E' {' || cit::text || E'/' || men::text || E'}]' FROM kataster.kn_vl sub_v INNER JOIN ciselnik.tuc tuc ON(tuc.id=sub_v.tuc) WHERE (1=1 AND "sub_v".ku = p.ku AND "sub_v".clv = p.clv AND tuc.id=1) ), E'\r\n') AS vlastnici
    ,p.cel
    ,count(DISTINCT u.uzi::text) AS pocet_uzivatelov
--    ,string_agg(DISTINCT u.uzi::text, chr(10)) AS uzivatelia
    ,COALESCE(string_agg(DISTINCT E'[' || "u".uzi::text || E']', E'\n'),'') AS uzivatelia
    ,kl.gid
--    ,kl.geom
  FROM kataster.kn_pa p
     INNER JOIN ciselnik.ump csl_ump ON(csl_ump.id=p.ump)
     LEFT JOIN ciselnik.kataster utj ON p.ku = utj.ku::numeric
     LEFT JOIN kataster.kn_kladpar kl ON p.parckey::text = kl.parckey::text
     LEFT JOIN kataster.kn_vl v ON p.ku = v.ku AND p.clv = v.clv
     LEFT JOIN kataster.kn_uz u ON p.ku = u.ku AND p.cel = u.cel
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
CREATE INDEX IF NOT EXISTS "idx_SK_KN_C_cislo_okresu" ON kataster."SK_KN_C"(cislo_okresu);
CREATE INDEX IF NOT EXISTS "idx_SK_KN_C_gid" ON kataster."SK_KN_C"(gid);
VACUUM kataster."SK_KN_C";
SELECT NOW();
REFRESH MATERIALIZED VIEW kataster."SK_KN_C";
SELECT NOW();
VACUUM kataster."SK_KN_C";
-- >> 7967330 riadkov; za 16min. 45s.
