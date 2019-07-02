#!/bin/bash
# použitie: ./prepare_dbf2pg.sh data/
# požiadavky: [pgdbf](https://github.com/kstrauser/pgdbf) (verzia min.: 0.6.3)
# výhody (oproti skriptu **kt-import_dbf2**):
# # # nástroj **pgdbf**, ktorý je tu využívaný, navrhuje aj štruktúru výslednej psql tabuľky (nie je teda potrebné dolaďovať sql štruktúru v ďalších skriptoch) 
# nevýhody:
# # # v štruktúre tbl. sú všetky číselné atribúty navrhnuté ako DATATYPE-NUMERIC-DECIMAL, aj tie, ktoré by mohli byť typu INTEGER

set -e

# kontrola min. požadovanej verzie 'pgdbf' 0.6.3
#if [ $(expr `man pgdbf |grep -i '^version' |awk '{print $2}' |sed 's/\.//g'` + 0) -lt "63" ]; then
if [ $(expr `pgdbf -h |grep '^PgDBF' |awk '{print $2}' |sed 's/\.//g'` + 0) -lt "63" ]; then
    echo -e "minimálne požadovaná verzia PGDBF je 0.6.3"
    exit 0;
fi

ROOT_DIR="$(readlink -e $1)"
cd "$ROOT_DIR"
INPUT_DIR="$ROOT_DIR/dbf"
OUTPUT_DIR="$ROOT_DIR/sql_p"
LOG_FILE="$ROOT_DIR/log/konv_dbf.txt"

SAVEIFS=$IFS
IFS=$'\n';
dbf_typy=(bp cs ep lv pa pk pv uz vl)
#dbf_typy=(bp cs)
for typ in "${dbf_typy[@]}"; do
  START_TIME=`date +%s`
  echo -e ">>> ${typ} >>>" >> $LOG_FILE
  echo -e " START: `date`" >> $LOG_FILE
  
  COUNTER=1
  
#  echo -e " pocet DBF: $(ls ${INPUT_DIR}/${typ}*.dbf |wc -l)" >> $LOG_FILE
  
  for f in $(find ${INPUT_DIR} -type f -iname "${typ}*.dbf" -print); do
    echo -en "\r${COUNTER}";

    name=$(basename $f '.dbf')
    ku=$(echo ${name} |sed -E 's,^([a-zA-Z]{2})([0-9]{6}),\2,g')
    tbl_name="kn_${typ}"

    # ak dbf subor nema ani 1 zaznam, nespracuvat ho
    # # # pozn.: dbfinfo vie tiez zobrazit pocet zaznamov, avsak zapocitava aj tie so statusom "DELETED"
    # are_records_in_cur_file=$(dbf_dump $f |wc -l)
    # # # pozn.: "dbf_dump |wc -l" je zdlhave, staci mi vediet, ci aspon 1 zaznam je v dbf subore
    if [ ${typ} == "pv" ] && [ -f "${INPUT_DIR}/${name}.fpt" ]; then
       are_records_in_cur_file=$(pgdbf -C -T -r -m ${INPUT_DIR}/${name}.fpt ${f} |grep -v '^\\' |head -1 |wc -l)
    else
       are_records_in_cur_file=$(pgdbf -C -T -r ${f} |grep -v '^\\' |head -1 |wc -l)
    fi

    if [ "${are_records_in_cur_file}" == "0" ]; then
       continue
    fi

# to pre testovani, keď chcem obmedziť len na 10 dbf súborov z každého typu
#    if [ "${COUNTER}" -gt "10" ] ; then 
#       break
#    fi

    # zisťovanie, či existuje aj pridružený ".fpt" súbor
    # # # pozn.: fpt súbory sú zväčša (resp. vždy) len pri pv....dbf súboroch
    m=false
    if [ ${typ} == "pv" ] && [ -f "${INPUT_DIR}/${name}.fpt" ]; then
      memo="-m ${INPUT_DIR}/${name}.fpt "
      m=true
    else
      memo=""
    fi;

    # pri prvom subore z daneho typu, vytvoriť najprv SQL vyraz pre vytvorenie tabuľky
    if [ ${COUNTER} == "1" ]; then
       nove_stlpce="row_id SERIAL PRIMARY KEY\,"
       # ak sa jedna o typ 'cs' alebo 'ep' alebo 'pa', treba dopocitat aj stlpec parckey
       if [ ${typ} == "cs" ] || [ ${typ} == "ep" ] || [ ${typ} == "pa" ] ; then
          nove_stlpce="${nove_stlpce} parckey VARCHAR(17)\,"
       fi
       nove_stlpce="${nove_stlpce} ku NUMERIC(6)\,"

       #echo "pgdbf -P -T -s 'cp852' -c -D -E ${memo}${f} |grep -P '^CREATE TABLE' |sed -E 's,(CREATE TABLE)\ ('"${tbl_name}"')([0-9]{6}) (\()(.*),\1 \2 \4 '"${nove_stlpce}"' \5,g' >> ${OUTPUT_DIR}/${typ}.sql"
       # spustenie príkazu na tvorbu SQL výrazu "CREATE TABLE" pre typy 'pv'
       if [ ${typ} == "pv" ] || [ ${m} == "true" ]; then
         pgdbf -P -T -s 'cp852' -c -D -E -m ${INPUT_DIR}/${name}.fpt ${f} \
	   |grep -P '^CREATE TABLE' \
	   |sed -E 's,(CREATE TABLE)\ ('"${typ}"')([0-9]{6}) (\()(.*),\1 '"${tbl_name}"' \4 '"${nove_stlpce}"' \5,g' \
	   |sed 's/\ DATE, /\ VARCHAR(10), /g' \
	     >> ${OUTPUT_DIR}/${typ}.sql
       # spustenie príkazu na tvorbu SQL výrazu "CREATE TABLE" pre ostatné typy (tj. okrem 'pv')
       else
         pgdbf -P -T -s 'cp852' -c -D -E ${f} \
	   |grep -P '^CREATE TABLE' \
           |sed -E 's,(CREATE TABLE)\ ('"${typ}"')([0-9]{6}) (\()(.*),\1 '"${tbl_name}"' \4 '"${nove_stlpce}"' \5,g' \
	   |sed 's/\ DATE, /\ VARCHAR(10), /g' \
             >> ${OUTPUT_DIR}/${typ}.sql
       fi
#       echo "CREATE INDEX idx_${tbl_name}_ku ON ${tbl_name}(ku);" >>${OUTPUT_DIR}/${typ}.sql
#       echo "BEGIN;" >> ${OUTPUT_DIR}/${typ}.sql
#       echo "\\COPY ${tbl_name} FROM STDIN" >> ${OUTPUT_DIR}/${typ}.sql
    fi

    # spočítavanie riadkov
    if [ ${COUNTER} == "1" ]; then
       # na začiatku má výsledný sql súbor 0 záznamov
       prev_riadok_counter=0
    fi

    echo "BEGIN;" >> ${OUTPUT_DIR}/${typ}.sql
    echo "SAVEPOINT pred_copy_ku_${ku};" >> ${OUTPUT_DIR}/${typ}.sql
    echo "\\COPY ${tbl_name} FROM STDIN" >> ${OUTPUT_DIR}/${typ}.sql

    # samotné dolovanie údajov do výsledného súboru
    if [ ${typ} == "pv" ] || [ ${m} == "true" ]; then
#    if [ ${typ} == "pv" ]; then
#       pgdbf -P -T -s 'cp852' -C -D -E -r -m ${INPUT_DIR}/${name}.fpt ${f} |grep '^[0-9].*' |awk '{printf "%s\t%s\t%s\n",NR + '"$(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l)"','"${ku}"',$0}' >> ${OUTPUT_DIR}/${typ}.sql
       pgdbf -P -T -s 'cp852' -C -D -E -r -m ${INPUT_DIR}/${name}.fpt ${f} |grep '^[0-9].*' |awk '{ printf "%s\t%s\t%s\n", NR + '"${prev_riadok_counter}"', '"${ku}"', $0 }' >> ${OUTPUT_DIR}/${typ}.sql
    else
       # ak sa jedna o 'cs' alebo 'ep' alebo 'pa', treba dopocitat aj stlpec parckey
       if [ ${typ} == "cs" ] || [ ${typ} == "ep" ] || [ ${typ} == "pa" ] ; then
#          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{printf "%s\t'"${ku}"'%011d\t%s\t%s\n", NR + '"$(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l)"', $1, '"${ku}"' ,$0}' >> ${OUTPUT_DIR}/${typ}.sql
          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{ printf "%s\t'"${ku}"'%011d\t%s\t%s\n", NR + '"${prev_riadok_counter}"', $1, '"${ku}"' , $0 }' >> ${OUTPUT_DIR}/${typ}.sql
#pokusy:
# # # prev_riadok_count=0; pgdbf -P -T -s 'cp852' -C -D -E -r /mnt/tmp/kataster-import/data/dbf/pa852104.dbf | head -5 |grep '^[0-9].*' |awk '{printf "%s\t852104%011d\t%s\t%s\n", NR + '"${prev_riadok_count}"', $1, 852 ,$0; print "prev_riadok_counter=" NR'} ; echo "${prev_riadok_count}";
       else
#          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{printf "%s\t%s\t%s\n", NR + '"$(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l)"', '"${ku}"', $0}' >> ${OUTPUT_DIR}/${typ}.sql
          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{ printf "%s\t%s\t%s\n", NR + '"${prev_riadok_counter}"', '"${ku}"', $0 }' >> ${OUTPUT_DIR}/${typ}.sql
       fi
    fi

    # spočítavanie riadkov pomocou 'tail' ('wc -l' sa postupným narastaním výsledného súboru spomaľoval)
    prev_riadok_counter=`tail -1 ${OUTPUT_DIR}/${typ}.sql |awk '{ print $1 }'`

    # uzavretie SQL transakcie
    echo "\\." >> ${OUTPUT_DIR}/${typ}.sql
    echo "COMMIT;" >> ${OUTPUT_DIR}/${typ}.sql


#    echo "unikátny počet stlpcov typu ${typ}: `grep -P '\t' ${OUTPUT_DIR}/${typ}.sql | awk -F"\t" '{print NF}' | sort -nu | uniq`"
    COUNTER=$(expr $COUNTER + 1)
  done

#  echo "COMMIT;" >> ${OUTPUT_DIR}/${typ}.sql
  echo " pocet spracovanych suborov, ktore maju min. 1 zaznam: `expr ${COUNTER} - 1`" >> $LOG_FILE
  echo -e " STOP: `date`" >> $LOG_FILE
  END_TIME=`date +%s`
  # ako dlho trval proces spracovania dbf súborov daného typu
  DURATION=$(($END_TIME-$START_TIME))
  echo -e " TRVANIE: `printf '%02dd:%02dh:%02dm:%02ds\n' $(($DURATION/86400)) $(($DURATION%86400/3600)) $(($DURATION%3600/60))   $(($DURATION%60))`" >> $LOG_FILE
  echo -e "<<< ${typ} <<<" >> $LOG_FILE

  # nahradenie znakov nového riadku vo výslednom súbore
  sed -i 's/\\r\\n/ /g' ${OUTPUT_DIR}/${typ}.sql
done
IFS=$SAVEIFS

### PO ZBEHNUTÍ TOHOTO SKRIPTU BY MALO NASLEDOVAT ###
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/bp.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/cs.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/ep.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/lv.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/pa.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/pk.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/pv.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/uz.sql &&echo -e "${pred}\n`date`" 
#pred=$(date); PGOPTIONS="-c search_path=kataster,public" psql -p 5432 -h 127.0.0.1 -U postgres -d kataster -f data/sql_p/vl.sql &&echo -e "${pred}\n`date`" 


### TODO ###
# identifikovanie numerickych stlpcov, vhodnych na indexovanie:
# # # dbf_dump --info ${f} |grep '^[1-9]' | grep -P '\sN\s' |awk '{print $2}'
# # # dbf_dump --info data/dbf/bp800481.dbf |grep '^[1-9]' | grep -P '\s.*\s' |awk '{print $2" "$3"("$4","$5")"}'

# # # SQL> SELECT (ku || lpad(cpa::text,11,'0')( AS parckey FROM kn_pa;
# # # BASH> printf "852104%011d" 1234567
# OK, ZAPRACOVANÉ: pri uz a vl su texty obsahujuce "\r\n" -> sed 's/\\r\\n/ /g' {uz,vl}.sql
# OK, ZAPRACOVANÉ: časť skriptu $(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l) môže spôsobovať postupné spomaľovanie, nakoľko, pri každom spracovanom dbf súbore sa zisťuje počet riadkov výsledného súboru, ktorý narastá 
# # # OK, ZAPRACOVANÉ: treba nastaviť špeciálne počítadlo, ktoré si bude zapamätávať poslednú max. hodnotu záznamu
# OK, ZAPRACOVANÉ: na začiatku spustiť kontrolu verzie 'pgdbf', vyžaduje sa totiž min. '0.6.3'

### SKUSENOSTI ###
# pgdbf prekonvertoval hodnotu '8401000000' na '8.401E+10' (stalo sa to pri súbore "vl801411.dbf" z 31.12.2018)
# # # dosledok: psql: ERROR:  numeric field overflow (https://github.com/kstrauser/pgdbf/issues/34)
# # # # # #  # #      DETAIL:  A field with precision 10, scale 0 must round to an absolute value less than 10^10.

##############################################################################################
### CAS IMPORTU ###
# bp:  0h. 28min.  3s. ( 1318580 z.) ---> AKTUÁLNE, po doladeni: 37s. !
# cs:  0h. 52min. 22s. ( 1829847 z.) ---> AKTUÁLNE, po doladeni: 50s. !
# ep:  2h. 53min. 25s. ( 7907918 z.) ---> AKTUÁLNE, po doladeni: 1min. 19s. !
# lv:  1h. 45min. 17s. ( 4578633 z.) ---> AKTUÁLNE, po doladeni: 55s. !
# pa:  3h. 14min. 40s. ( 7967216 z.) ---> AKTUÁLNE, po doladeni: 1min. 27s. !
# pk:  0h.  2min. 31s. (    6032 z.) ---> AKTUÁLNE, po doladeni: 29s. !
# pv:  - veľmi dlho -  (57354535 z.) ---> AKTUÁLNE, po doladeni: 7min. 52s. !
# uz:  0h. 45min. 23s. ( 1522075 z.) ---> AKTUÁLNE, po doladeni: 51s. !
# vl: 23h.             (28587998 z.) ---> AKTUÁLNE, po doladeni: 7min. 28s. !
### CELKOVÉ TRVANIE spracovania DBF súborov celoslovenských katastrálnych dát: 21min. 48s. ###
##############################################################################################

### BUG-y ###
# pgdbf -P -T -s 'cp852' -C -D -E -r -i 'idu,poz' -m /mnt/tmp/kataster-import/data/dbf/pv812129.fpt /mnt/tmp/kataster-import/data/dbf/pv812129.dbf 
# # # Segmentation fault
