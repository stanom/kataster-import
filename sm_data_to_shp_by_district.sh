#!/bin/bash
# skript pre konvert psql kat. dát do shp, podľa okresov
# Autor: S.Motyčka
# prihlasovacie údaje pre psql sa načítavajú z ~/.pgpass
# spustenie: './sm_data_to_shp_by_district.sh'

if [[ "$1" == "" ]] ; then
  echo -e "nebol zadany povinny parameter 'YYYYMMDD'"
  exit 0
elif [[ ! $1 =~ ^[0-9]{8}$ ]] ; then
  echo -e "vstupny parameter musi byt 8-ciselny 'YYYYMMDD'"
  exit 0
fi

### PREMENNE ###
datum="$1"

OUTPUT_DIR="/var/tmp" 
PG_HOST="127.0.0.1" 
PG_PORT="5432" 
PG_USER="postgres" 
PG_DB="kataster" 
#PG_SQL="SELECT DISTINCT okres FROM ciselnik.\"kataster\" WHERE 1=1 AND okres IN(308) ORDER BY okres ;" 
PG_SQL="SELECT DISTINCT okres FROM ciselnik.\"kataster\" WHERE 1=1 ORDER BY okres ;"
#PG_SQL="SELECT DISTINCT okres FROM ciselnik.\"kataster\" WHERE 1=1 AND okres BETWEEN 506 AND 507 ORDER BY okres ;"


### SKRIPT ###
    date
    for o in $(psql -U postgres -h ${PG_HOST} -p ${PG_PORT} -d ${PG_DB} -q -t -A -c "`echo "${PG_SQL}"`"); do
        f=$(psql -U postgres -h ${PG_HOST} -p ${PG_PORT} -d ${PG_DB} -q -t -A -c "SELECT REPLACE(okres_nazov,' ','') FROM ciselnik.kataster WHERE 1=1 AND okres=${o} LIMIT 1")
        OUTPUT_FILE="KN_C_okres_${f}"
	OUTPUT_EXTENSION="shp"
        nice -n 19 ionice -c2 -n7 \
	ogr2ogr \
		--debug off \
                -f "ESRI Shapefile" ${OUTPUT_DIR}/${OUTPUT_FILE}.${OUTPUT_EXTENSION} \
                PG:"host=${PG_HOST} user=${PG_USER} dbname=${PG_DB}" \
                -sql "SELECT DISTINCT kn_c.gid ,ST_CollectionExtract(ST_MakeValid(kn_c.geom),3) AS geom ,utj.okres /*,COALESCE(utj.okres_nazov, ''::character varying) AS okres_nazov*/ ,kn_c.ku ,COALESCE(utj.ku_nazov, ''::character varying) AS ku_nazov ,COALESCE(kn_c.parcela, ''::character varying) AS parcela ,pa.cpa ,pa.ump ,pa.pkk ,pa.drp ,pa.vym ,pa.don ,pa.clv ,v.vla AS vlastnik ,v.cit ,v.men ,v.tuc ,v.pcs FROM kataster.kn_kladpar AS kn_c LEFT JOIN kataster.kn_pa pa ON pa.parckey::text = kn_c.parckey::text LEFT JOIN ciselnik.kataster utj ON kn_c.ku::numeric = utj.ku LEFT JOIN kataster.kn_vl v ON(pa.ku = v.ku AND pa.clv = v.clv) WHERE 1=1 AND kn_c.kmen > 0 AND kn_c.kmen IS NOT NULL AND kn_c.ku IN(SELECT DISTINCT ku FROM ciselnik.kataster WHERE 1=1 AND okres=${o}) ORDER BY cpa ,tuc ,pcs" \
               -nln "okres_${f}" \
               -nlt "MULTIPOLYGON" \
               -a_srs NULL \
               -lco ENCODING="UTF-8"
#cat <<EOF >${OUTPUT_DIR}/${OUTPUT_FILE}.prj
#PROJCS["S-JTSK / Krovak East North",GEOGCS["S-JTSK",DATUM["System_Jednotne_Trigonometricke_Site_Katastralni",SPHEROID["Bessel 1841",6377397.155,299.1528128,AUTHORITY["EPSG","7004"]],TOWGS84[485,169.5,483.8,7.786,4.398,4.103,0],AUTHORITY["EPSG","6156"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4156"]],PROJECTION["Krovak"],PARAMETER["latitude_of_center",49.5],PARAMETER["longitude_of_center",24.83333333333333],PARAMETER["x_scale",-1.0],PARAMETER["y_scale",1.0],PARAMETER["azimuth",30.28813972222222],PARAMETER["pseudo_standard_parallel_1",78.5],PARAMETER["scale_factor",0.9999],PARAMETER["false_easting",0],PARAMETER["false_northing",0],PARAMETER["xy_plane_rotation",90.0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","5514"]]
#EOF
sleep 1 &&\
       7z a -sdel -bso0 ${OUTPUT_DIR}/${OUTPUT_FILE}.7z ${OUTPUT_DIR}/${OUTPUT_FILE}.* &&\
	rsync ${OUTPUT_DIR}/${OUTPUT_FILE}.7z root@172.30.1.21:/mnt/smb/cop/gis_official/kataster_SHP/${datum}/ &&\
	rm -f ${OUTPUT_DIR}/${OUTPUT_FILE}.* &&\ 
	echo -en "\rokres_${f} - OK"
	
    done
date
