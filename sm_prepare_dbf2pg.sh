#!/bin/bash
# použitie: ./prepare_dbf2pg.sh data/
# požiadavky: [pgdbf](https://github.com/kstrauser/pgdbf) (verzia min.: 0.6.3)
# výhody (oproti skriptu **kt-import_dbf2**):
# # # nástroj **pgdbf**, ktorý je tu využívaný, navrhuje aj štruktúru výslednej psql tabuľky (nie je teda potrebné dolaďovať sql štruktúru v ďalších skriptoch) 
# nevýhody:
# # # v štruktúre tbl. sú všetky číselné atribúty navrhnuté ako DATATYPE-NUMERIC-DECIMAL, aj tie, ktoré by mohli byť typu INTEGER

set -e

ROOT_DIR="$(readlink -e $1)"
cd "$ROOT_DIR"
INPUT_DIR="$ROOT_DIR/dbf"
OUTPUT_DIR="$ROOT_DIR/sql_p"
LOG_FILE="$ROOT_DIR/log/konv_dbf.txt"

SAVEIFS=$IFS
IFS=$'\n';
#dbf_typy=(bp cs ep lv pa pk pv uz vl)
dbf_typy=(bp cs)
for typ in "${dbf_typy[@]}"; do
  START_TIME=`date +%s`
  echo -e ">>> ${typ} >>>" >> $LOG_FILE
  echo -e " START: `date`" >> $LOG_FILE
  
  COUNTER=1
  
#  echo -e " pocet DBF: $(ls ${INPUT_DIR}/${typ}*.dbf |wc -l)" >> $LOG_FILE
  
  for f in $(find ${INPUT_DIR} -type f -iname "${typ}*.dbf" -print); do
    echo -en "\r${COUNTER}";
    
    # ak dbf subor nema ani 1 zaznam, nespracuvat ho
    # dbfinfo vi tiez zobrazit pocet zaznamov, avsak zapocitava aj tie so statusom "DELETED"
    #are_records_in_cur_file=$(dbf_dump $f |wc -l)
    #"dbf_dump |wc -l" je zdlhave, staci mi vediet, ci aspon 1 zaznam je v dbf subore
    are_records_in_cur_file=$(pgdbf -C -T -r ${f} |grep -v '^\\' |head -1 |wc -l)
    if [ "${are_records_in_cur_file}" == "0" ]; then
       continue
    fi
    
#    if [ "${COUNTER}" -gt "10" ] ; then 
#       break
#    fi

    cesta=$(dirname $f)
    name=$(basename $f '.dbf')
    ku=$(echo ${name} |sed -E 's,^([a-zA-Z]{2})([0-9]{6}),\2,g')
    tbl_name="kn_${typ}"

    m=false
    if [ -f "${cesta}/${name}.fpt" ]; then
      memo="-m ${INPUT_DIR}/${name}.fpt "
      m=true
    else
      memo=""
    fi;

    if [ ${COUNTER} == "1" ]; then
       nove_stlpce="row_id SERIAL PRIMARY KEY\,"
       # ak sa jedna o 'cs' alebo 'ep' alebo 'pa', treba dopocitat aj stlpec parckey
       if [ ${typ} == "cs" ] || [ ${typ} == "ep" ] || [ ${typ} == "pa" ] ; then
          nove_stlpce="${nove_stlpce} parckey VARCHAR(17)\,"
       fi
       nove_stlpce="${nove_stlpce} ku NUMERIC(6)\,"
       
       #echo "pgdbf -P -T -s 'cp852' -c -D -E ${memo}${f} |grep -P '^CREATE TABLE' |sed -E 's,(CREATE TABLE)\ ('"${tbl_name}"')([0-9]{6}) (\()(.*),\1 \2 \4 '"${nove_stlpce}"' \5,g' >> ${OUTPUT_DIR}/${typ}.sql"
       if [ ${m} == "true" ]; then
         pgdbf -P -T -s 'cp852' -c -D -E -m ${cesta}/${name}.fpt ${f} \
	   |grep -P '^CREATE TABLE' \
	   |sed -E 's,(CREATE TABLE)\ ('"${typ}"')([0-9]{6}) (\()(.*),\1 '"${tbl_name}"' \4 '"${nove_stlpce}"' \5,g' \
	   |sed 's/\ DATE, /\ VARCHAR(10), /g' \
	     >> ${OUTPUT_DIR}/${typ}.sql
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
    
    if [ ${m} == "true" ]; then
#    if [ ${typ} == "pv" ]; then
#       pgdbf -P -T -s 'cp852' -C -D -E -r -m ${cesta}/${name}.fpt ${f} |grep '^[0-9].*' |awk '{printf "%s\t%s\t%s\n",NR + '"$(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l)"','"${ku}"',$0}' >> ${OUTPUT_DIR}/${typ}.sql
       pgdbf -P -T -s 'cp852' -C -D -E -r -m ${cesta}/${name}.fpt ${f} |grep '^[0-9].*' |awk '{printf "%s\t%s\t%s\n",NR + '"${prev_riadok_counter}"','"${ku}"',$0}' >> ${OUTPUT_DIR}/${typ}.sql
    else
       # ak sa jedna o 'cs' alebo 'ep' alebo 'pa', treba dopocitat aj stlpec parckey
       if [ ${typ} == "cs" ] || [ ${typ} == "ep" ] || [ ${typ} == "pa" ] ; then
#          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{printf "%s\t'"${ku}"'%011d\t%s\t%s\n", NR + '"$(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l)"', $1, '"${ku}"' ,$0}' >> ${OUTPUT_DIR}/${typ}.sql
          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{ printf "%s\t'"${ku}"'%011d\t%s\t%s\n", NR + '"${prev_riadok_counter}"', $1, '"${ku}"' ,$0 }' >> ${OUTPUT_DIR}/${typ}.sql
#pokusy:
# # # prev_riadok_count=0; pgdbf -P -T -s 'cp852' -C -D -E -r /mnt/tmp/kataster-import/data/dbf/pa852104.dbf | head -5 |grep '^[0-9].*' |awk '{printf "%s\t852104%011d\t%s\t%s\n", NR + '"${prev_riadok_count}"', $1, 852 ,$0; print "prev_riadok_counter=" NR'} ; echo "${prev_riadok_count}";
       else
#          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{printf "%s\t%s\t%s\n", NR + '"$(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l)"', '"${ku}"', $0}' >> ${OUTPUT_DIR}/${typ}.sql
          pgdbf -P -T -s 'cp852' -C -D -E -r ${f} |grep '^[0-9].*' |awk '{ printf "%s\t%s\t%s\n", NR + '"${prev_riadok_counter}"', '"${ku}"', $0 }' >> ${OUTPUT_DIR}/${typ}.sql
       fi
    fi
    prev_riadok_counter=`tail -1 ${OUTPUT_DIR}/${typ}.sql |awk '{print $1}'`
    echo "\\." >> ${OUTPUT_DIR}/${typ}.sql
    echo "COMMIT;" >> ${OUTPUT_DIR}/${typ}.sql


#    echo "unikátny počet stlpcov typu ${typ}: `grep -P '\t' ${OUTPUT_DIR}/${typ}.sql | awk -F"\t" '{print NF}' | sort -nu | uniq`"
    COUNTER=$(expr $COUNTER + 1)
  done
#  echo "COMMIT;" >> ${OUTPUT_DIR}/${typ}.sql
  echo " pocet spracovanych suborov, ktore maju min. 1 zaznam: `expr ${COUNTER} - 1`" >> $LOG_FILE
  echo -e " STOP: `date`" >> $LOG_FILE
  END_TIME=`date +%s`
  DURATION=$(($END_TIME-$START_TIME))
  echo -e " TRVANIE: `printf '%02dd:%02dh:%02dm:%02ds\n' $(($DURATION/86400)) $(($DURATION%86400/3600)) $(($DURATION%3600/60))   $(($DURATION%60))`" >> $LOG_FILE
  echo -e "<<< ${typ} <<<" >> $LOG_FILE
  sed -i 's/\\r\\n/ /g' ${OUTPUT_DIR}/${typ}.sql
done
IFS=$SAVEIFS

### TODO ###
# identifikovanie numerickych stlpcov, vhodnych na indexovanie
# dbf_dump --info ${f} |grep '^[1-9]' | grep -P '\sN\s' |awk '{print $2}'
# dbf_dump --info data/dbf/bp800481.dbf |grep '^[1-9]' | grep -P '\s.*\s' |awk '{print $2" "$3"("$4","$5")"}'
# OK: dopočítať atribút {ep,pa}.parckey (z "ku" a "cpa")
# # # SQL> SELECT (ku || lpad(cpa::text,11,'0')( AS parckey FROM kn_pa;
# # # BASH> printf "852104%011d" 1234567
# OK: pri uz a vl su texty obsahujuce "\r\n" -> sed 's/\\r\\n/ /g' {uz,vl}.sql
# časť skriptu $(grep ^[0-9] ${OUTPUT_DIR}/${typ}.sql |wc -l) môže spôsobovať postupné spomaľovanie, nakoľko, pri každom spracovanom dbf súbore sa zisťuje počet riadkov výsledného súboru, ktorý narastá 
# # # treba nastaviť špeciálne počítadlo, ktoré si bude zapamätávať poslednú max. hodnotu záznamu

### SKUSENOSTI ###
# pgdbf prekonvertoval hodnotu '8401000000' na '8.401E+10' (stalo sa to pri súbore "vl801411.dbf" z 31.12.2018)
# # # dosledok: psql: ERROR:  numeric field overflow (https://github.com/kstrauser/pgdbf/issues/34)
# # # # # #  # #      DETAIL:  A field with precision 10, scale 0 must round to an absolute value less than 10^10.

### CAS IMPORTU ###
# bp:  0h. 28min.  3s. ( 1318580 z.) - po doladeni: 37s. !
# cs:  0h. 52min. 22s. ( 1829847 z.) (import do DB: 31s.) - po doladeni: 50s. !
# ep:  2h. 53min. 25s. ( 7907918 z.) - po doladeni: 1min. 19s. !
# lv:  1h. 45min. 17s. ( 4578633 z.) - po doladeni: 55s. !
# pa:  3h. 14min. 40s. ( 7967216 z.) - po doladeni: 1min. 27s. !
# pk:  0h.  2min. 31s. (    6032 z.) - po doladeni: 29s. !
# pv:  - veľmi dlho -  (57354535 z.) - po doladeni: -
# uz:  0h. 45min. 23s. ( 1522075 z.) - po doladeni: 51s. !
# vl: 23h.             (28587998 z.) - po doladeni: 7min. 28s. !
: '
1Table pv800481 has memo fields, but couldn't open the related memo file
2Table pv802735 has memo fields, but couldn't open the related memo file
3Table pv803065 has memo fields, but couldn't open the related memo file
4Table pv803570 has memo fields, but couldn't open the related memo file
5Table pv803693 has memo fields, but couldn't open the related memo file
6Table pv803707 has memo fields, but couldn't open the related memo file
7Table pv804096 has memo fields, but couldn't open the related memo file
8Table pv804274 has memo fields, but couldn't open the related memo file
9Table pv804380 has memo fields, but couldn't open the related memo file
10Table pv804690 has memo fields, but couldn't open the related memo file
11Table pv804959 has memo fields, but couldn't open the related memo file
12Table pv805211 has memo fields, but couldn't open the related memo file
13Table pv805301 has memo fields, but couldn't open the related memo file
14Table pv805343 has memo fields, but couldn't open the related memo file
15Table pv805556 has memo fields, but couldn't open the related memo file
16Table pv805700 has memo fields, but couldn't open the related memo file
17Table pv805866 has memo fields, but couldn't open the related memo file
18Table pv806005 has memo fields, but couldn't open the related memo file
19Table pv806099 has memo fields, but couldn't open the related memo file
20Table pv807265 has memo fields, but couldn't open the related memo file
21Table pv808814 has memo fields, but couldn't open the related memo file
22Table pv808881 has memo fields, but couldn't open the related memo file
23Table pv809985 has memo fields, but couldn't open the related memo file
24Table pv810649 has memo fields, but couldn't open the related memo file
25Table pv811483 has memo fields, but couldn't open the related memo file
26Table pv813303 has memo fields, but couldn't open the related memo file
27Table pv814482 has memo fields, but couldn't open the related memo file
28Table pv815594 has memo fields, but couldn't open the related memo file
29Table pv819824 has memo fields, but couldn't open the related memo file
30Table pv819867 has memo fields, but couldn't open the related memo file
31Table pv820407 has memo fields, but couldn't open the related memo file
32Table pv820865 has memo fields, but couldn't open the related memo file
33Table pv821128 has memo fields, but couldn't open the related memo file
34Table pv821438 has memo fields, but couldn't open the related memo file
35Table pv821446 has memo fields, but couldn't open the related memo file
36Table pv821560 has memo fields, but couldn't open the related memo file
37Table pv821756 has memo fields, but couldn't open the related memo file
38Table pv821888 has memo fields, but couldn't open the related memo file
39Table pv822094 has memo fields, but couldn't open the related memo file
40Table pv822256 has memo fields, but couldn't open the related memo file
41Table pv822884 has memo fields, but couldn't open the related memo file
42Table pv822892 has memo fields, but couldn't open the related memo file
43Table pv823023 has memo fields, but couldn't open the related memo file
44Table pv823554 has memo fields, but couldn't open the related memo file
45Table pv826626 has memo fields, but couldn't open the related memo file
46Table pv826634 has memo fields, but couldn't open the related memo file
47Table pv826642 has memo fields, but couldn't open the related memo file
48Table pv828203 has memo fields, but couldn't open the related memo file
49Table pv828211 has memo fields, but couldn't open the related memo file
50Table pv829773 has memo fields, but couldn't open the related memo file
51Table pv830399 has memo fields, but couldn't open the related memo file
52Table pv832189 has memo fields, but couldn't open the related memo file
53Table pv833231 has memo fields, but couldn't open the related memo file
54Table pv835196 has memo fields, but couldn't open the related memo file
55Table pv835625 has memo fields, but couldn't open the related memo file
56Table pv835803 has memo fields, but couldn't open the related memo file
57Table pv835811 has memo fields, but couldn't open the related memo file
58Table pv836079 has memo fields, but couldn't open the related memo file
59Table pv837628 has memo fields, but couldn't open the related memo file
60Table pv838039 has memo fields, but couldn't open the related memo file
61Table pv838519 has memo fields, but couldn't open the related memo file
62Table pv838527 has memo fields, but couldn't open the related memo file
63Table pv839019 has memo fields, but couldn't open the related memo file
64Table pv841731 has memo fields, but couldn't open the related memo file
65Table pv841749 has memo fields, but couldn't open the related memo file
66Table pv841838 has memo fields, but couldn't open the related memo file
67Table pv842036 has memo fields, but couldn't open the related memo file
68Table pv845931 has memo fields, but couldn't open the related memo file
69Table pv846163 has memo fields, but couldn't open the related memo file
70Table pv846554 has memo fields, but couldn't open the related memo file
71Table pv846775 has memo fields, but couldn't open the related memo file
72Table pv846791 has memo fields, but couldn't open the related memo file
73Table pv846813 has memo fields, but couldn't open the related memo file
74Table pv846821 has memo fields, but couldn't open the related memo file
75Table pv847755 has memo fields, but couldn't open the related memo file
76Table pv851922 has memo fields, but couldn't open the related memo file
77Table pv851931 has memo fields, but couldn't open the related memo file
78Table pv852716 has memo fields, but couldn't open the related memo file
79Table pv852813 has memo fields, but couldn't open the related memo file
80Table pv853771 has memo fields, but couldn't open the related memo file
81Table pv854964 has memo fields, but couldn't open the related memo file
82Table pv856657 has memo fields, but couldn't open the related memo file
83Table pv857335 has memo fields, but couldn't open the related memo file
84Table pv859303 has memo fields, but couldn't open the related memo file
85Table pv859320 has memo fields, but couldn't open the related memo file
86Table pv859338 has memo fields, but couldn't open the related memo file
87Table pv859346 has memo fields, but couldn't open the related memo file
88Table pv859354 has memo fields, but couldn't open the related memo file
89Table pv859524 has memo fields, but couldn't open the related memo file
90Table pv860760 has memo fields, but couldn't open the related memo file
91Table pv860778 has memo fields, but couldn't open the related memo file
92Table pv861111 has memo fields, but couldn't open the related memo file
93Table pv863475 has memo fields, but couldn't open the related memo file
94Table pv863491 has memo fields, but couldn't open the related memo file
95Table pv865958 has memo fields, but couldn't open the related memo file
96Table pv865966 has memo fields, but couldn't open the related memo file
97Table pv868191 has memo fields, but couldn't open the related memo file
98Table pv868850 has memo fields, but couldn't open the related memo file
99Table pv868868 has memo fields, but couldn't open the related memo file
100Table pv869481 has memo fields, but couldn't open the related memo file
101Table pv869554 has memo fields, but couldn't open the related memo file
102Table pv869571 has memo fields, but couldn't open the related memo file
103Table pv869686 has memo fields, but couldn't open the related memo file
104Table pv869945 has memo fields, but couldn't open the related memo file
105Table pv870293 has memo fields, but couldn't open the related memo file
106Table pv871133 has memo fields, but couldn't open the related memo file
107Table pv871788 has memo fields, but couldn't open the related memo file
108Table pv871796 has memo fields, but couldn't open the related memo file
awk: cmd. line:1: {printf "%s\t%s\t%s\n",NR + \COPY,871796,$0}
awk: cmd. line:1:                             ^ backslash not last character on line
awk: cmd. line:1: {printf "%s\t%s\t%s\n",NR + \COPY,871796,$0}
awk: cmd. line:1:                             ^ syntax error
'
