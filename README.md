# Phenotype Data Processing for Variant Prioritizers

This project consists of scripts to support the manuscript:

Parikh JR, Genetti CA et al.  *A data-driven architecture using natural language processing to improve phenotyping efficiency and accelerate genetic diagnoses of rare disorders.*


## Data Processing Pipeline
The manuscript defines a set of 3 filters (frequency, depth, and diversity) that can be applied to a set of [HPO](https://hpo.jax.org/app/) terms. These filters have been implemented in `post_process_NLP.py`. 

* `pheno_pipeline_params.yaml` must be edited to reference to AWS S3 bucket and folder paths containing phenotype data and VCF files. 
* `post_process_NLP.py` applies NLP term filters across a 3D space of parameters described in the manuscript and writes the filtered set of terms back to S3.

## Dockerized Exomiser
Parallel processing of our data processing for gene/variant prioritization was enabled by containerizing [Exomiser](http://exomiser.github.io/Exomiser/). The image used for our manuscript is available on Dockerhub at [jiggyjsq/exomiser:12.1.0__hg19_2003__pheno_2003](https://hub.docker.com/r/jiggyjsq/exomiser/tags?page=1&ordering=last_updated).

The following files are required to build your own image using a different version of Exomiser:
* `Dockerfile`
* `run_exomiser_job.py` is copied during the build

`ray_parallel_nlp.py` is a helper script to run dockerized Exomiser in parallel after filtered NLP term sets have been created.

## Tiered Filtering Pipeline
The manuscript describes a tiered process to help reduce the effort required for manual review of gene/variant prioritization results. `pipeline_parse_html.R` parses the HTML Exomiser reports to only keep a limited number of genes and places them in separate folders for each of the 4 steps in the tiered process.

