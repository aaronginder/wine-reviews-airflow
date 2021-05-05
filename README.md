# Airflow Tutorial

## Introduction
These instructions will get you started using Apache Airflow with Cloud Composer on GCP. This repository should be used in conjunction with the article which will explain the core concepts of Apache Airflow.

The output of these instructions is a simple data pipeline that can be triggered using Cloud Composer. I have also provided the docker-compose configurations with instructions below if you wish to run your code
locally rather than deploying to Google Cloud.

## Getting Started
### Pre-requites
Before following these instructions, ensure you the below installed:
* Python 3 ([download and install from here](https://www.python.org/downloads/release/python-380/))
* Google Cloud SDK ([download and install from here](https://cloud.google.com/sdk/docs/install))
* Git ([download and install from here](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git))
* An integrated development environment (IDE) such as VS Code ([download and install from here](https://code.visualstudio.com/download))
* *Optional* - Docker ([install Docker Desktop](https://docs.docker.com/docker-for-windows/install/))

If you do not want to install any of the above on your machine, you can use Cloud Shell Visual Edtior which has all the above pre-installed.

## Instructions
### Deploying to Google Cloud
The high-level instructions of how to get started are below:
1. Clone this repository using git & open in your IDE
2. Run the set_up_environments.sh script located in the scripts directory. Open your terminal & run `./scripts/set_up_environment.sh`
3. Execute push_dag_to_cloud.sh by running `./scripts/push_dag_to_cloud.sh`
4. Open the Cloud Composer Airflow Webserver & view your DAG running

*It is recommended that you create a Python virtual environment to install Python packages into. Otherwise, there is a risk of package incompatibility and potential errors when following this tutorial.*

#### **1. Clone this Repository**
Execute the following command:

`git clone https://github.com/aaronginder/airflow-tutorial.git && cd airflow-tutorial`

Then, open the folder in your IDE.

#### **2. Set up your Environment**
The script in this step:
* Ensures your gcloud is configured to your project
* Creates the *Wine_Reviews* dataset in your Google Cloud project
* Creates the Cloud Composer environment (this takes circa 20 minutes to complete, dependent on connectivity)

#### **3. Deploy your DAG to Cloud Composer**
To deploy your DAG to Cloud Composer, execute the push_dag_to_cloud.sh script located in the scripts directory.

#### **4. View your DAG Running**
Go to the Composer environment in the Google Cloud console and click on the Airflow webserver hyperlink. This will open the graphical user interface where you can monitor your pipeline. From here, you can click on the DAG and view the latest pipeline run.

### Debugging the Pipeline Locally
If you want to deploy your pipeline, you must have **DockerHub** installed.

To start the Docker service, execute the following script:
`./scripts/docker/start_local_docker_container.sh`

To finish the local Docker service, execute the following script:
`./scripts/docker/stop_local_docker_container`

## Additional Information

* Docker
* Apache Airflow
* Google Cloud Composer
