import datetime
import os
import json
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

var_dict=models.Variable.get('non_prod')
var_dict=json.loads(var_dict)
artifact='wordcount-job-1.0-SNAPSHOT.jar'

output_file = os.path.join(
    var_dict['output_bucket'], 'wordcount',
    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep

input_file = 'gs://pub/shakespeare/rose.txt'

wordcount_args = ['org/apache/hadoop/examples/WordCount', input_file, output_file]

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': var_dict['project']
}

with models.DAG(
        'composer_hadoop',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-hadoop-cluster-{{ ds_nodash }}',
        num_workers=2,
        zone=var_dict['zone'],
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    run_dataproc_hadoop = dataproc_operator.DataProcHadoopOperator(
        task_id='run_dataproc_hadoop',
        main_jar='{}/{}'.format(var_dict['staging_bucket'], artifact),
        cluster_name='composer-hadoop-cluster-{{ ds_nodash }}',
        gcp_conn_id=var_dict['conn_id'],
        arguments=wordcount_args)

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-hadoop-cluster-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster