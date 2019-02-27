-- ### FUNCTION parcels_by_district(int, int, '[c|e]') ###
-- funkcia pre generovanie parcelného stavu '[c|e]' podľa okresov
-- # Príklad: "SELECT parcels_by_district(600, 700, 'c') ;" generuje tabuľky s parc. stavom 'c'  v schéme 'public' okresov 60x:
-- # # # public.kn_c_okres_601 --COMMENT: 'Parcely C v okrese Banská Bystrica'
-- # # # public.kn_c_okres_602 --COMMENT: 'Parcely C v okrese Banská Štiavnica'
-- # # # public.kn_c_okres_603 --COMMENT: 'Parcely C v okrese Brezno'
-- # # # public.kn_c_okres_604 --COMMENT: 'Parcely C v okrese Detva'
-- # # # public.kn_c_okres_605 --COMMENT: 'Parcely C v okrese Krupina'
-- # # # public.kn_c_okres_606 --COMMENT: 'Parcely C v okrese Lučenec'
-- # # # public.kn_c_okres_607 --COMMENT: 'Parcely C v okrese Poltár'
-- # # # public.kn_c_okres_608 --COMMENT: 'Parcely C v okrese Revúca'
-- # # # public.kn_c_okres_609 --COMMENT: 'Parcely C v okrese Rimavská Sobota'
-- # # # public.kn_c_okres_610 --COMMENT: 'Parcely C v okrese Veľký Krtíš'
-- # # # public.kn_c_okres_611 --COMMENT: 'Parcely C v okrese Zvolen'
-- # # # public.kn_c_okres_612 --COMMENT: 'Parcely C v okrese Žarnovica'
-- # # # public.kn_c_okres_613 --COMMENT: 'Parcely C v okrese Žiar nad Hronom'


SET search_path TO kataster,ciselnik,public ;

CREATE OR REPLACE FUNCTION parcels_by_district(IN rozsah_od integer, IN rozsah_do integer, IN stav varchar DEFAULT '') RETURNS integer AS $$

DECLARE prechadzanie_adresara RECORD;
DECLARE katastre_v_okrese RECORD;
DECLARE parcely_v_ku RECORD;

DECLARE tbl VARCHAR DEFAULT 'kn_c_okres_';
DECLARE source_tbl_geo VARCHAR DEFAULT 'kn_kladpar';
DECLARE source_tbl_parcels VARCHAR DEFAULT 'kn_pa';

DECLARE i integer;

BEGIN
IF $3 = 'c' THEN
    tbl := 'kn_c_okres_';
    source_tbl_geo := 'kn_kladpar';
    source_tbl_parcels := 'kn_pa';
ELSIF $3 = 'e' THEN
    tbl = 'kn_e_okres_';
    source_tbl_geo := 'kn_uov';
    source_tbl_parcels := 'kn_ep';
ELSE
    RETURN 'CHYBA: nezadal si tretí parameter [c|e]';
END IF;

i := 0;
    SET search_path TO ciselnik;
    FOR prechadzanie_adresara IN
        SELECT DISTINCT
        okres AS cislo_okresu
        ,okr_nazov AS nazov_okresu
        FROM kataster
        WHERE 1=1
            AND okres BETWEEN $1 AND $2
        ORDER BY okres
    LOOP
--    RAISE NOTICE 'Creating table...';
    SET search_path TO public;
--    RAISE NOTICE '%', tbl;
-- ak tbl este neexistuje:
    IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname = CONCAT(tbl, prechadzanie_adresara.cislo_okresu) AND relkind='r') THEN
--        RAISE NOTICE 'Creating table...';

            EXECUTE '
               CREATE TABLE IF NOT EXISTS ' || tbl || prechadzanie_adresara.cislo_okresu || ' (
                  gid serial PRIMARY KEY
                , okres_cislo smallint
                , ku integer
                , parckey character varying(17)
                , parcela character varying(40)
                , kmen integer
                , podlomenie integer
                , vym integer
                   , drp smallint
            , pkk integer
                , don smallint
                , ump smallint
            , spn smallint
--            , tuc smallint -- to je z tbl. vl
--            , tvl smallint -- to je z tbl. vl
            , drv integer
                , clv integer
                , pocet_vlastnikov smallint
                , vlastnici text
                , cel integer
                , pocet_uzivatelov smallint
                , uzivatelia text
--            , geom geometry(MultiPolygon, 5514)
--            ,wkt text
                   )';

            EXECUTE format('
        COMMENT ON TABLE %s IS ''Parcely %s v okrese %s''', tbl || prechadzanie_adresara.cislo_okresu, UPPER($3), prechadzanie_adresara.nazov_okresu);
--            RAISE NOTICE 'Table % created.', tbl || prechadzanie_adresara.cislo_okresu ;

            SET search_path TO kataster,public;

--        RAISE NOTICE 'Adding geom column ...';
        EXECUTE format('
        ALTER TABLE %I ADD COLUMN geom geometry(MultiPolygon, 5514)', tbl || prechadzanie_adresara.cislo_okresu
);
--        RAISE NOTICE 'Creating indexes ...';
--        EXECUTE format('
--        CREATE INDEX idx_%I_gid ON ' || tbl || prechadzanie_adresara.cislo_okresu || ' (gid)', tbl || prechadzanie_adresara.cislo_okresu);
        EXECUTE format('
        CREATE INDEX idx_%I_ku ON ' || tbl || prechadzanie_adresara.cislo_okresu || ' (ku)', tbl || prechadzanie_adresara.cislo_okresu);
        EXECUTE format('
        CREATE INDEX idx_%I_parckey ON ' || tbl || prechadzanie_adresara.cislo_okresu || ' (parckey)', tbl || prechadzanie_adresara.cislo_okresu);
        EXECUTE format('
        CREATE INDEX idx_%I_parcela ON ' || tbl || prechadzanie_adresara.cislo_okresu || ' (parcela)', tbl || prechadzanie_adresara.cislo_okresu);
        EXECUTE format('
        CREATE INDEX idx_%I_geom ON ' || tbl || prechadzanie_adresara.cislo_okresu || ' USING gist (geom)', tbl || prechadzanie_adresara.cislo_okresu);

        i := i + 1;
        RAISE NOTICE '%',i;
--        RAISE NOTICE 'Now begin inserting data...';
        FOR katastre_v_okrese IN
         SELECT DISTINCT cislo_ku
            FROM adresar
            WHERE 1=1
                AND cislo_okresu = @prechadzanie_adresara.cislo_okresu
        LOOP
--                RAISE NOTICE 'Taham parcely z ku: %', katastre_v_okrese.cislo_ku ;
            FOR parcely_v_ku IN

            EXECUTE '
                SELECT
                   s.gid
                  , ' || prechadzanie_adresara.cislo_okresu || ' AS cislo_okresu
                     , s.ku
                     , s.parckey
                     , s.parcela
                     , s.kmen
                     , s.podlomenie
                     , s.vym
                     , s.drp
            , s.pkk
                     , s.don
                     , s.ump
            , s.spn
--            , s.tuc
--            , s.tvl
            , s.drv
                     , s.clv
                     , v.pocet_vlastnikov
                     , v.vlastnici
                     , s.cel
                     , u.pocet_uzivatelov
                     , u.uzivatelia
                     , s.geom AS geom
                    FROM(
                    SELECT
                           kn.gid
                          , kn.ku
                        , kn.parckey
                         , parcela
                         , kmen
                         , podlomenie
                         , pa.vym
                         , pa.drp
                , pa.pkk
                         , pa.don
                         , pa.ump
                , pa.spn
--                , pa.tuc
--                , pa.tvl
                , pa.drv
                         , pa.clv
                         , pa.cel
                         , geom AS geom
                    FROM
                            ' || source_tbl_geo || ' kn
                    LEFT JOIN ' || source_tbl_parcels || ' pa ON(kn.parckey=pa.parckey)
                    WHERE 1=1
                            AND kn.ku = ' || katastre_v_okrese.cislo_ku || '
--            LIMIT 10
                    ) s
                    LEFT JOIN (
            SELECT
                ku
                , clv
                , count(vla) AS pocet_vlastnikov
                , string_agg(DISTINCT cit::text || chr(47) || men::text || chr(32) || chr(58) || chr(32) || vl.vla::text, chr(10)) AS vlastnici
            FROM kn_vl vl
            WHERE 1=1
                AND ku= ' || katastre_v_okrese.cislo_ku || '
            GROUP BY ku, clv
            ) v ON(s.ku=v.ku AND s.clv=v.clv)
                    LEFT JOIN (
            SELECT
                ku
                , cel
                , count(uzi) AS pocet_uzivatelov
                , string_agg(DISTINCT uzi::text, chr(10)) AS uzivatelia
            FROM kn_uz uz
            WHERE 1=1
                AND ku=' || katastre_v_okrese.cislo_ku || '
            GROUP BY ku, cel
            ) u ON(s.ku=u.ku AND s.cel=u.cel)
            '

            LOOP
--            RAISE NOTICE 'Parcela: %', parcely_v_ku ;
--            RAISE NOTICE 'Parcela: %', parcely_v_ku.parcela ;
                    EXECUTE format('
            INSERT INTO ' || tbl || prechadzanie_adresara.cislo_okresu || ' (
                gid
                , okres_cislo
                , ku
                , parckey
                , parcela
                , kmen
                , podlomenie
                , vym
                , drp
                , pkk
                , don
                , ump
                , spn
--                , tuc
--                , tvl
                , drv
                , clv
                , pocet_vlastnikov
                , vlastnici
                , cel
                , pocet_uzivatelov
                , uzivatelia
                , geom
            )
            VALUES(
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
            );
            ')
            USING
            parcely_v_ku.gid
            , parcely_v_ku.cislo_okresu
            , parcely_v_ku.ku
            , parcely_v_ku.parckey
            , parcely_v_ku.parcela
            , parcely_v_ku.kmen
            , parcely_v_ku.podlomenie
            , parcely_v_ku.vym
            , parcely_v_ku.drp
            , parcely_v_ku.pkk
            , parcely_v_ku.don
            , parcely_v_ku.ump
            , parcely_v_ku.spn
--            , parcely_v_ku.tuc
--            , parcely_v_ku.tvl
            , parcely_v_ku.drv
            , parcely_v_ku.clv
            , parcely_v_ku.pocet_vlastnikov
            , parcely_v_ku.vlastnici
            , parcely_v_ku.cel
            , parcely_v_ku.pocet_uzivatelov
            , parcely_v_ku.uzivatelia
            , parcely_v_ku.geom
            ;
            END LOOP;
        END LOOP;
--            RAISE NOTICE 'Uz len spustit: VACUUM (ANALYZE) %', tbl || prechadzanie_adresara.cislo_okresu ;
            RAISE NOTICE 'VACUUM (ANALYZE) %', tbl || prechadzanie_adresara.cislo_okresu || ';';
--        EXECUTE 'VACUUM (ANALYZE) ' || tbl || prechadzanie_adresara.cislo_okresu ; -- toto sa neda spustit cez funkciu
-- ak tbl uz existuje
        ELSE
              RAISE NOTICE 'tbl uz existovala';
        END IF;
    END LOOP;
    RETURN 1;
END;
$$ LANGUAGE plpgsql;
