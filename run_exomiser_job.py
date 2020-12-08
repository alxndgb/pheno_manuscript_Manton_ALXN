#!/usr/bin/python


##################################################
## This script runs exomiser based on a job_ID, base yml file, an input VCF from S3, input HPO term file from S3, and an output S3 directory (from environment variables) - this is within a docker image 
## It assumes some default environment variables: exomiser_Xmx for java max memory and write_bucket where results get upload (appropriate permissions are needed)
## It assumes docker image gets AWS credentials from environment variables or Assume Role
## 
## docker run --rm -it -e exomiser_job_id=test -e exomiser_vcf_file=s3://mybucket/Pfeiffer.vcf -e exomiser_hpo_file=s3://mybucket/NLPoutput_Pfeiffer.txt -e exomiser_base_yml_file=s3://mybucket/test-analysis-exome.yml -e exomiser_Xmx=4g -e write_bucket=mybucket -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY jiggyjsq/exomiser:12.1.0__hg19_2003__pheno_2003 python3.8 run_exomiser_job.py
## 
## This script was written to support the following paper
## "Parikh JR, Genetti CA et al.  A data-driven architecture using natural language processing to improve phenotyping efficiency and accelerate genetic diagnoses of rare disorders."
##################################################
## Author: Jiggy Parikh
## Version: 0.1.0
## Email: jiggy@jsquarelabs.com
## Status: Dev
##################################################

import sys
import uuid
import boto3
import os
import glob
import pandas as pd
import yaml
import shutil
import json
import botocore
import re

print("Downloading Files and Setting up Run...")

#updatable environment variables
job_id = os.environ.get("exomiser_job_id")
vcf_file = os.environ.get("exomiser_vcf_file")
hpo_file = os.environ.get("exomiser_hpo_file")
base_yml_file = os.environ.get("exomiser_base_yml_file")

sample_id = os.path.splitext(os.path.basename(vcf_file))[0]

#default environment variable
write_bucket = os.environ.get("write_bucket")
Xmx = os.environ.get("exomiser_Xmx")

s3 = boto3.resource("s3")

#create folders for run - uuid.uuid4().hex
run_uuid = uuid.uuid4().hex
data_dir = os.path.join("/usr/share/data", run_uuid)
results_dir = os.path.join("/usr/share/results", run_uuid)
if not os.path.exists(data_dir):
	os.mkdir(data_dir)
if not os.path.exists(results_dir):
	os.mkdir(results_dir)


#copy base yml, vcf and nl_hpo files to job data dir from s3
s3.Bucket(re.match("s3://(.+?)/.+", vcf_file).groups()[0]).download_file(vcf_file.replace("s3://" + re.match("s3://(.+?)/.+", vcf_file).groups()[0] + "/", "") , os.path.join(data_dir, os.path.basename(vcf_file)))
s3.Bucket(re.match("s3://(.+?)/.+", hpo_file).groups()[0]).download_file(hpo_file.replace("s3://" + re.match("s3://(.+?)/.+", hpo_file).groups()[0] + "/", "") , os.path.join(data_dir, os.path.basename(hpo_file)))
s3.Bucket(re.match("s3://(.+?)/.+", base_yml_file).groups()[0]).download_file(base_yml_file.replace("s3://" + re.match("s3://(.+?)/.+", base_yml_file).groups()[0] + "/", "") , os.path.join(data_dir, os.path.basename(base_yml_file)))

#parse HPO file to get HPO terms of the patient
fh = open(os.path.join(data_dir, os.path.basename(hpo_file)))
hpo_terms = []
sample_id_found = False
for line in fh:
	line = line.strip().split(",")
	if len(line) == 1:
		if sample_id_found:
			break
		key = line[0]
		if key == sample_id:
			sample_id_found = True
	elif line[0] == "Criterion":
		pass
	else:
		if sample_id_found:
			hpo_terms.append(line[0])
fh.close()

if (sample_id_found == False) or (len(hpo_terms) == 0):
	raise

hpo_terms = list(map(lambda x: x.split("_")[0].replace("hp", "HP:"), hpo_terms))

#edit yaml file
#replace vcf file path, hpo terms, and results file prefix

fname = os.path.join(data_dir, os.path.basename(base_yml_file))
with open(fname, "r") as fh:
	data = yaml.safe_load(fh)

data["analysis"]["vcf"] = os.path.join(data_dir, os.path.basename(vcf_file))
data["analysis"]["hpoIds"] = hpo_terms
data["outputOptions"]["outputPrefix"] = os.path.join(results_dir, sample_id)


with open(fname, "w") as yaml_file:
    yaml_file.write(yaml.dump(data, default_flow_style=True, sort_keys=False))
    

#run exomiser
print("Running Exomiser...")
os.system("java -jar -Xmx"+Xmx + " exomiser-cli-12.1.0/exomiser-cli-12.1.0.jar --analysis " + os.path.join(data_dir, os.path.basename(base_yml_file)) + " --spring.config.location=exomiser-cli-12.1.0/application.properties")

#create result tab file from json output
print("Collect and Upload Results to S3...")
html_result_file = os.path.join(results_dir, sample_id+".html")
json_result_file = os.path.join(results_dir, sample_id+".json")
tab_result_file = os.path.join(results_dir, sample_id+".tab")

result_genes = []
with open(json_result_file) as fh:
	result_json = json.load(fh)
	for gene in result_json:
		result_genes.append([gene["geneSymbol"], gene["combinedScore"], gene["variantScore"], gene["priorityScore"]])
	
result_genes_df = pd.DataFrame(data=result_genes, columns=["Gene", "Combined_Score", "Genetic_Score", "Phenotype_score"])
result_genes_df.to_csv(tab_result_file, sep="\t", index=False)


#upload results to S3
#create folder in S3 corresponding to jobID if it doesn't exist
#create sub folders in S3 for html, json, and tab if run_category parent folder don't exist
try:
	s3.Object(write_bucket, "exomiser_results/" + job_id + "/").load()
except botocore.exceptions.ClientError as e:
	if e.response["Error"]["Code"] == "404":
		s3.meta.client.put_object(Bucket=write_bucket, Key=("exomiser_results/" + job_id + "/"))
		s3.meta.client.put_object(Bucket=write_bucket, Key=("exomiser_results/" + job_id + "/html/"))
		s3.meta.client.put_object(Bucket=write_bucket, Key=("exomiser_results/" + job_id + "/json/"))
		s3.meta.client.put_object(Bucket=write_bucket, Key=("exomiser_results/" + job_id + "/tab/"))		
	else:
		raise
else:
	pass



#upload results to respective folders on S3
s3.Bucket(write_bucket).upload_file(html_result_file, "exomiser_results/" + job_id + "/html/" + os.path.basename(html_result_file))
s3.Bucket(write_bucket).upload_file(json_result_file, "exomiser_results/" + job_id + "/json/" + os.path.basename(json_result_file))
s3.Bucket(write_bucket).upload_file(tab_result_file, "exomiser_results/" + job_id + "/tab/" + os.path.basename(tab_result_file))

#delete run folder (in case re-using a fully loaded container makes more sense)
shutil.rmtree(data_dir) 
shutil.rmtree(results_dir) 








