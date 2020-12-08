FROM ubuntu

#create directories for shared applications, data, and results
RUN mkdir /usr/share/applications && \
	mkdir /usr/share/data && \
	mkdir /usr/share/results

#create volume to store exomiser application data
VOLUME /exomiser_data
	
WORKDIR /usr/share/applications

#install java
RUN apt update && apt -y install default-jre

#install python, and it's packages
RUN apt install -y python3.8 wget unzip sed python-yaml python3-pip && \
	python3.8 -m pip install pandas boto3 requests PyYAML


#download exomiser, the data, unzip the data, and then clean up
RUN wget https://data.monarchinitiative.org/exomiser/latest/exomiser-cli-12.1.0-distribution.zip && \
	wget https://data.monarchinitiative.org/exomiser/latest/2003_hg19.zip && \
	wget https://data.monarchinitiative.org/exomiser/latest/2003_phenotype.zip && \
	unzip exomiser-cli-12.1.0-distribution.zip && \
	unzip 2003_hg19.zip -d /exomiser_data && \
	unzip 2003_phenotype.zip -d /exomiser_data && \
	rm 2003_*.zip && \
	rm exomiser-cli-12.1.0-distribution.zip

	
#modify exomiser application.properties to 2003 datasets
#exomiser.hg19.data-version=1902 to exomiser.hg19.data-version=2003
#exomiser.hg19.variant-white-list-path=1902_hg19_clinvar_whitelist.tsv.gz to exomiser.hg19.variant-white-list-path=2003_hg19_clinvar_whitelist.tsv.gz
#exomiser.phenotype.data-version=1902 to  exomiser.phenotype.data-version=2003
##exomiser.hg19.transcript-source=ensembl to exomiser.hg19.transcript-source=ensembl
##spring.cache.type=none to spring.cache.type=caffeine
##spring.cache.caffeine.spec=maximumSize=60000 to spring.cache.caffeine.spec=maximumSize=300000
##logging.file=logs/exomiser.log to logging.file=exomiser.log
#exomiser.data-directory= to exomiser.data-directory=/exomiser_data

RUN sed -i 's/1902/2003/g' exomiser-cli-12.1.0/application.properties && \
	sed -i 's/\#exomiser.hg19.transcript-source=ensembl/exomiser.hg19.transcript-source=ensembl/g' exomiser-cli-12.1.0/application.properties && \
	sed -i 's/\#spring.cache.type=none/spring.cache.type=caffeine/g' exomiser-cli-12.1.0/application.properties && \	
	sed -i 's/\#spring.cache.caffeine.spec=maximumSize=60000/spring.cache.caffeine.spec=maximumSize=300000/g' exomiser-cli-12.1.0/application.properties && \
	sed -i 's/\#logging.file=logs\/exomiser.log/logging.file=exomiser.log/g' exomiser-cli-12.1.0/application.properties && \
	sed -i 's/\#exomiser.data-directory=/exomiser.data-directory=\/exomiser_data/g' exomiser-cli-12.1.0/application.properties


#COPY python script to run exomiser
COPY ./run_exomiser_job.py /usr/share/applications
