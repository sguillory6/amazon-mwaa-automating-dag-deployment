#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0


import docker
import boto3
import base64
import sys
import time

"""
This runs mwaa local image and the DB. After they are run, it runs the DAG integrity test and unit test.
returns non-zero exit code if the test fails
"""
def testWorkflow():
    mwaa_image = 'public.ecr.aws/w7c2o1k6/mwaa-local:2.0.2'
    mwaa_db_image = 'postgres:alpine3.16'
    mwaa = None
    postgres = None
    try:
        docker_client_ecr = docker.from_env(version='1.24')
        docker_client_hub = docker.from_env()
        ecr_client = boto3.client('ecr-public', region_name=region)

        docker_client_hub.images.pull(mwaa_db_image)

        token = ecr_client.get_authorization_token()
        
#        username, password = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
        username, password = base64.b64decode(token['authorizationData']['authorizationToken']).decode().split(':')
        
        docker_client_ecr.login(username, password, registry='public.ecr.aws')

        auth_config = {'username': username, 'password': password}
        docker_client_ecr.images.pull(mwaa_image, auth_config=auth_config)

        postgres = docker_client_hub.containers.run(
            mwaa_db_image,
            detach=True,
            ports={"5432": 5432},  
            volumes={
                pwd+"/data": {"bind": "/var/lib/postgresql/data", "mode": "rw"},
            },
            environment={
                "POSTGRES_USER":"airflow",
                "POSTGRES_PASSWORD":"airflow",
                "POSTGRES_DB":"airflow"
            },
            hostname="postgres"
        )
        mwaa = docker_client_ecr.containers.run(
            mwaa_image,
            detach=True,
            ports={"8080/tcp": 8080},  # expose local port 8080 to container
            volumes={
                pwd+"/dags": {"bind": "/usr/local/airflow/dags/", "mode": "rw"},
                pwd+"/test": {"bind": "/usr/local/airflow/test/", "mode": "rw"},
                pwd+"/plugins": {"bind": "/usr/local/airflow/plugins/", "mode": "rw"},
            },
            environment={
                "LOAD_EX":"n",
                "EXECUTOR":"Local",
                "AIRFLOW__CORE__SQL_ALCHEMY_CONN":"postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
            },
            command="local-runner",
            links={postgres.name:postgres.name},
        ) 
        initDB(mwaa)
        testRequirements(mwaa, requirements)
        runIntegrityTest(mwaa)
        runUnitTest(mwaa)
        print("All test done")

    except Exception as inst:
        print(inst)
        sys.exit(-1)
    finally:
        if(mwaa != None):
            mwaa.stop()
            postgres.stop()


def initDB(mwaa):

    exit_code, message = mwaa.exec_run(
        "airflow db check"
    )
    #Allow Postgres TCP port is available for connection
    if(exit_code == 1):
        print("wait for DB to be up")
        time.sleep(5)
    exit_code, message = mwaa.exec_run(
        "airflow db init"
    )
"""
Test requirements can be loaded , check if the contraint file is in the requirements
"""

def testRequirements(mwaa, requirements):

    print("start testing requirements ...")
    exit_code = 0
    with open(requirements, "r") as requirementFile:
        requirementFileContent = requirementFile.read()
        if constraintFileName not in requirementFileContent:
            exit_code = 1
        requirementFile.close()

    if (exit_code == 1):
        raise Exception(f'Contraints file {constraintFileName} is not specified in the requirements file.')

    exit_code, message = mwaa.exec_run(
        "/entrypoint.sh test-requirements"
    )
    if (exit_code == 1):
        raise Exception(message)
    print("Requirements are good")


def runIntegrityTest(mwaa):
    print("running DAG integrity test ...")

    exit_code, dag_test_output = mwaa.exec_run(
        "python3 -m unittest /usr/local/airflow/test/dag_validation.py"
        
    )
    if (exit_code == 1):
        raise Exception(dag_test_output)
    print("Dag integrity test is done")

def runUnitTest(mwaa):
 
    print("running unit test ...")
    exit_code, dag_test_output = mwaa.exec_run(
            "python3 -m unittest discover -s /usr/local/airflow/test/dags -p 'test*.py'"
        )
    if (exit_code == 1):
        raise Exception(dag_test_output)
    print("Unit test is done")


region = sys.argv[1]
account=sys.argv[2]
pwd=sys.argv[3]
requirements=sys.argv[4]
constraintFileName=sys.argv[5]

testWorkflow()
