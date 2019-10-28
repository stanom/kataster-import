#!/bin/bash
# skript pre konvert psql kat. dát do sqlite, podľa okresov
# Autor: S.Motyčka
# prihlasovacie údaje pre psql sa načítavajú z ~/.pgpass
# spustenie: './sm_data_to_sqlite_by_district.sh "c" ["EPSG:4326"]'

### PREMENNE ###
OUTPUT_DIR="/var/tmp"
PG_HOST="127.0.0.1" 
PG_PORT="5432" 
PG_USER="postgres" 
PG_DB="kataster" 
#PG_SQL="SELECT DISTINCT okres FROM ciselnik.\"kataster\" WHERE 1=1 AND okres IN(405) ORDER BY okres ;" 
PG_SQL="SELECT DISTINCT okres FROM ciselnik.\"kataster\" WHERE 1=1 AND UNACCENT(okres_nazov) NOT LIKE okres_nazov ORDER BY okres ;" 
#PG_SQL="SELECT DISTINCT okres FROM ciselnik.\"kataster\" WHERE 1=1 ORDER BY okres ;" 

if [ "$1" == "ArcMap" ] || [ "$1" == "Qgis" ]; then
  GIS_APP="$1"
else
  echo -e "nebol zadany povinny parameter [\"ArcMap\"|\"Qgis\"]"
  exit 0
fi

if [ "$2" == "c" ] || [ "$2" == "e" ]; then
  STAV="$2"
else
  echo -e "nebol zadany povinny parameter [c|e]"
  exit 0
fi

#SRS_text=""
#SRS_code=""

if [[ $3 =~ ^EPSG:[0-9]+$ ]]; then
  SRS=$3
#  SRS_text=$(echo $SRS | cut -f1 -d:)
#  SRS_code=$(echo $SRS | cut -f2 -d:)
fi


### FUNKCIE ###
sql_data()
  {
    if [ "${STAV}" == "c" ]; then
      echo "SELECT r.*,kl.geom FROM kataster.\"SK_KN_C\" r LEFT JOIN kataster.\"kn_kladpar\" kl ON(r.gid=kl.gid) WHERE 1=1 AND okres=$1" 
    else
      echo "SELECT r.*,kl.geom FROM kataster.\"SK_KN_E\" r LEFT JOIN kataster.\"kn_uov\" kl ON(r.gid=kl.gid) WHERE 1=1 AND okres=$1"
    fi
  }

### SKRIPT ###
    date
    for o in $(psql -U postgres -h ${PG_HOST} -p ${PG_PORT} -d ${PG_DB} -q -t -A -c "`echo "${PG_SQL}"`"); do
      s=$(echo "`sql_data ${o}`")
      f_name_unaccent=$(psql -U postgres -h ${PG_HOST} -p ${PG_PORT} -d ${PG_DB} -q -t -A -c "SELECT UNACCENT(REPLACE(okres_nazov,' ','')) FROM ciselnik.kataster WHERE 1=1 AND okres=${o} LIMIT 1")
      f_name=$(psql -U postgres -h ${PG_HOST} -p ${PG_PORT} -d ${PG_DB} -q -t -A -c "SELECT REPLACE(okres_nazov,' ','') FROM ciselnik.kataster WHERE 1=1 AND okres=${o} LIMIT 1")
      OUTPUT_FILE="${GIS_APP}_KN_`echo ${STAV} |tr a-z A-Z`_okres_${f_name_unaccent}`[[ ${SRS} != "" ]] && echo "_${SRS}" |sed 's/://g'`.sqlite"
      OUTPUT_FILE_zip="${GIS_APP}_KN_`echo ${STAV} |tr a-z A-Z`_okres_${f_name}`[[ ${SRS} != "" ]] && echo "_${SRS}" |sed 's/://g'`.sqlite"
      ogr2ogr \
               -f "SQLite" ${OUTPUT_DIR}/${OUTPUT_FILE} \
           PG:"host=${PG_HOST} user=${PG_USER} dbname=${PG_DB}" \
           -sql "`echo "${s}"`" \
               -dsco SPATIALITE=YES \
               -nln "okres_${f_name_unaccent}" \
               -nlt "MULTIPOLYGON" \
               `[[ ${SRS} != "" ]] && echo "-t_srs ${SRS}"` \
               `[[ ${GIS_APP} != "Qgis" ]] && echo "-a_srs NULL"` \
      && [[ ${GIS_APP} != "Qgis" ]] && sqlite3 -batch ${OUTPUT_DIR}/${OUTPUT_FILE} "UPDATE geometry_columns SET srid=-1;" \
      && 7z a -bso0 ${OUTPUT_DIR}/${OUTPUT_FILE_zip}.7z ${OUTPUT_DIR}/${OUTPUT_FILE} \
      && rm -f ${OUTPUT_DIR}/${OUTPUT_FILE} \
      && echo -en "\rokres_${f_name} - OK" 
    done
date


# BUGS:
# 2019-04-23: E-parcely (BratislavaIV, BratislavaV) sa natiahnu v Qgise len ako tybuľky
# # Problém: geometrie boli dvojakého typu "Polygon" a "Multipolygon"
# # # Oprava: pridávam parameter '-nlt "MULTIPOLYGON"'
# 2019-05-22: súradnicový systém 'EPSG:5514' sa v ArcMAP nezobrazuje dobre
# # # Oprava: pridávam parameter '-a_srs NULL' - pre ArcMap exporty
# 2019-10-07: '-a_srs NULL' - pre ArcMap exporty neprináša požadovaný efekt, ostáva nastavený súr. systém 'EPSG:5514'
# # # Oprava: využitie aplikácie 'sqlite3' -> 'sqlite3 -batch ${OUTPUT_DIR}/${OUTPUT_FILE} "UPDATE geometry_columns SET srid=-1;"'
# 2019-10-28: ArcMap nenačíta vrstvy, ktorých názvoslovie okresu tvorí diakritické znamienko [Šaľa, Čadca, ...]
# # # Oprava: využitie postgresql extenzie "unaccent"
