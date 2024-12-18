[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Health Gorilla Connector

## 🔗  Docs
Check out our [docs](https://thetuvaproject.com/) to learn about the Tuva Project and how you can use it on your healthcare data.
<br/><br/>

## 🧰  What does this repo do?
The Health Gorilla Connector is a dbt project that maps flattened Health Gorilla FHIR data to the Tuva input layer, and then builds all of the available clinical Tuva Data Marts.  You can flatten Health Gorilla FHIR data into the tabular format this project expects using the [FHIR_Inferno](https://github.com/tuva-health/FHIR_connector) utility and the provided Health Gorilla configuration files.  See our [guide](https://thetuvaproject.com/connectors/fhir-inferno) for a more detailed step by step walkthrough of that process.
<br/><br/>  

## 🔌 Database Support
- BigQuery
- Redshift
- Snowflake
<br/><br/>  

## ✅  Quickstart Guide

### Step 1: Pre-requisites
You must have Health Gorilla data in FHIR format that has been flattened and loaded to your warehouse.  See the [FHIR_inferno](https://github.com/tuva-health/FHIR_inferno) repo including the [Health Gorilla configurations](https://github.com/tuva-health/FHIR_inferno/tree/main/configurations/configuration_Health_Gorilla), and the [FHIR preprocessing guide](https://thetuvaproject.com/connectors/fhir-inferno). You must have dbt installed and a profile set up, or you must have a connection to your warehouse set up in dbt cloud. 
<br/><br/> 

### Step 2: Configure Input Database and Schema
Next you need to tell dbt where your source data is located.  Do this using the variables `input_database` and `input_schema` in the `dbt_project.yml` file.  You also need to configure your `profile` in the `dbt_project.yml`.
<br/><br/> 

### Step 4: dbt deps
Execute the command `dbt deps` to install The Tuva Project.  By default, this connector will use any version of the Tuva Project after 0.5.0 which is when clinical support was released.
<br/><br/>

### Step 4: Run
Now you're ready to run the connector and the Tuva Project.  For example, using dbt CLI you would `cd` to the project root folder in the command line and execute `dbt build`.  You're now ready to do clinical data analytics!  Check out the [data marts](https://thetuvaproject.com/data-marts/overview) in our docs to learn what tables you should query.
<br/><br/>

## 🙋🏻‍♀️ How do I contribute?
Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  Feel free to submit an issue or drop a message in Slack.
<br/><br/>

## 🤝 Join our community!
Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!