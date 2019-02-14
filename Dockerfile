FROM alpine:edge
RUN \
 apk upgrade --update-cache --available \
 && apk add --no-cache git gcc g++ make openssh python py-pip bash zip findutils curl gnupg rhash
RUN pip install --upgrade pip
WORKDIR /
RUN git clone https://github.com/imincik/kataster-import.git
RUN cd /kataster-import
WORKDIR /kataster-import

RUN pip install -r requirements.txt

RUN mkdir /data
CMD echo -e "EXAMPLES:\n" \
        "\tdocker run --rm --name some-katimport stano/kataster-import /bin/bash -c '/kataster-import/./kt-vgi2shp'" \
        "\tdocker run --rm --name some-katimport stano/kataster-import /bin/bash -c '/kataster-import/./kt-sql'" \
        ""
ENTRYPOINT ["/bin/bash"]
