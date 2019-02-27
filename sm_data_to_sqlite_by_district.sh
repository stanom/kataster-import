#!/bin/bash
# skript pre konvert psql kat. dát do sqlite, podľa okresov
# prihlasovacie údaje pre psql sa načítavajú z ~/.pgpass
# spustenie: './sm_data_to_sqlite_by_district.sh'

### PREMENNE ###
OUTPUT_DIR="/tmp" 
PG_HOST="127.0.0.1" 
PG_PORT="5432" 
PG_USER="postgres" 
PG_DB="kataster" 
PG_SQL="SELECT DISTINCT okres FROM ciselnik.\"kataster\" WHERE 1=1 ORDER BY okres ;" 

### FUNKCIE ###
sql_data()
    {
    echo "SELECT r.*,kl.geom FROM kataster.\"reg_C_vla_uzi_3\" r LEFT JOIN kataster.\"kn_kladpar\" kl ON(r.gid=kl.gid) WHERE 1=1 AND cislo_okresu=$1" 
    }

### SKRIPT ###
    date
    for o in $(psql -U postgres -h ${PG_HOST} -p ${PG_PORT} -d ${PG_DB} -q -t -A -c "`echo "${PG_SQL}"`"); do
    s=$(echo "`sql_data ${o}`")
    ogr2ogr \
               -f "SQLite" ${OUTPUT_DIR}/kn_okres_${o}.sqlite \
           PG:"host=${PG_HOST} user=${PG_USER} dbname=${PG_DB}" \
           -sql "`echo "${s}"`" \
               -dsco SPATIALITE=YES \
               -nln "okres_${o}" \
    && 7z a -bso0 ${OUTPUT_DIR}/kn_okres_${o}.sqlite.7z ${OUTPUT_DIR}/kn_okres_${o}.sqlite \
      && rm -f ${OUTPUT_DIR}/kn_okres_${o}.sqlite \
      && echo -en "\rkn_okres_${o} - OK" 
done
date
