#!/bin/bash
# použitie: ./prepare_dbf2pg.sh data/
# požiadavky: [pgdbf](https://github.com/kstrauser/pgdbf) (verzia min.: 0.6.3)
# výhodou oproti skriptu **kt-import_dbf2** je, že nástroj **pgdbf**, ktorý je tu využívaný, navrhuje aj štruktúru výslednej psql tabuľky (nie je teda potrebné dolaďovať sql štruktúru v ďalších skriptoch) 
# nevýhodou sú: pomalšie spracovanie; v štruktúre tbl. sú všetky číselné atribúty navrhnuté ako [numeric](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL)

set -e

ROOT_DIR="$(readlink -e $1)"
cd "$ROOT_DIR"

SAVEIFS=$IFS
IFS=$'\n';
dbf_typy=(bp cs ep lv pa pk pv uz vl)
for typ in "${dbf_typy[@]}"; do
  COUNTER=1
  echo -e "${typ}: START: `date`" >> $ROOT_DIR/log/konv_dbf.txt
  echo -e "pocet DBF: $(ls $ROOT_DIR/dbf/${typ}*.dbf |wc -l)" |tee -a $ROOT_DIR/log/konv_dbf.txt
  
  for f in $(find $ROOT_DIR/dbf -type f -iname "${typ}*.dbf" -print); do
    echo -en "\r${COUNTER}";
    
    # ak dbf subor nema ani 1 zaznam, nespracuvat ho
    if [ `dbf_dump $f |wc -l` == "0" ]; then
       continue
    fi

    cesta=$(dirname $f)
    name=$(basename $f '.dbf')
    ku=$(echo ${name} |sed -E 's,^([a-zA-Z]{2})([0-9]{6}),\2,g')
    tbl_name="kn_${typ}"

    m=false
    if [ -f "${cesta}/${name}.fpt" ]; then
      memo="-m ${ROOT_DIR}/dbf/${name}.fpt "
      m=true
    else
      memo=""
    fi;

    if [ ${COUNTER} == "1" ]; then
       #echo "pgdbf -P -T -s 'cp852' -c -D -E ${memo}${f} |grep -P '^CREATE TABLE' |sed -E 's,(CREATE TABLE)\ ('"${tbl_name}"')([0-9]{6}) (\()(.*),\1 \2 \4row_id SERIAL\, icutj NUMERIC(6)\, \5,g' >> $ROOT_DIR/sql_p/${typ}.sql"
       if [ ${m} == "true" ]; then
         pgdbf -P -T -s 'cp852' -c -D -E -m ${cesta}/${name}.fpt ${f} |grep -P '^CREATE TABLE' |sed -E 's,(CREATE TABLE)\ ('"${tbl_name}"')([0-9]{6}) (\()(.*),\1 \2 \4row_id SERIAL PRIMARY KEY\, icutj NUMERIC(6)\, \5,g' >> $ROOT_DIR/sql_p/${typ}.sql
       else
         pgdbf -P -T -s 'cp852' -c -D -E ${f} |grep -P '^CREATE TABLE' |sed -E 's,(CREATE TABLE)\ ('"${tbl_name}"')([0-9]{6}) (\()(.*),\1 \2 \4row_id SERIAL PRIMARY KEY\, icutj NUMERIC(6)\, \5,g' >> $ROOT_DIR/sql_p/${typ}.sql
       fi
       echo "CREATE INDEX idx_${tbl_name}_icutj ON ${tbl_name}(icutj);" >>$ROOT_DIR/sql_p/${typ}.sql
       echo "BEGIN;" >> $ROOT_DIR/sql_p/${typ}.sql
       echo "\\COPY ${tbl_name} FROM STDIN" >> $ROOT_DIR/sql_p/${typ}.sql
    fi

#    echo "BEGIN;" >> $ROOT_DIR/sql_p/${typ}.sql
#    echo "SAVEPOINT pred_copy_${ku};"
#    echo "\\COPY ${typ} FROM STDIN" >> $ROOT_DIR/sql_p/${typ}.sql
    if [ ${m} == "true" ]; then
       pgdbf -P -T -s 'cp852' -C -D -E -r -m ${cesta}/${name}.fpt ${f} |grep '^[0-9].*' |awk '{printf "%s\t%s\t%s\n",NR + '"$(grep ^[0-9] sql_p/${typ}.sql |wc -l)"','"${ku}"',$0}' >> $ROOT_DIR/sql_p/${typ}.sql
    else
       pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{printf "%s\t%s\t%s\n",NR + '"$(grep ^[0-9] sql_p/${typ}.sql |wc -l)"','"${ku}"',$0}' >> $ROOT_DIR/sql_p/${typ}.sql
    fi
    echo "\\." >> $ROOT_DIR/sql_p/${typ}.sql
#    echo "COMMIT;" >> $ROOT_DIR/sql_p/${typ}.sql

#    echo "unikátny počet stlpcov typu ${typ}: `grep -P '\t' sql/${typ}.sql | awk -F"\t" '{print NF}' | sort -nu | uniq`"
    COUNTER=$(expr $COUNTER + 1)
  done
  echo "COMMIT;" >> $ROOT_DIR/sql_p/${typ}.sql
  echo "pocet spracovanych suborov, ktore maju min. 1 zaznam: `expr ${COUNTER} - 1`" >> $ROOT_DIR/log/konv_dbf.txt
  echo -e "${typ}: STOP: `date`" >> $ROOT_DIR/log/konv_dbf.txt
done
IFS=$SAVEIFS

### TODO ###
# identifikovanie numerickych stlpcov, vhodnych na indexovanie
# dbf_dump --info ${f} |grep '^[1-9]' | grep -P '\sN\s' |awk '{print $2}'
# dbf_dump --info data/dbf/bp800481.dbf |grep '^[1-9]' | grep -P '\s.*\s' |awk '{print $2" "$3"("$4","$5")"}'
