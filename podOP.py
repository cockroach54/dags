import datetime

from airflow import models
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator

# A Secret is an object that contains a small amount of sensitive data such as
# a password, a token, or a key. Such information might otherwise be put in a
# Pod specification or in an image; putting it in a Secret object allows for
# more control over how it is used, and reduces the risk of accidental
# exposure.

# secret_env = secret.Secret(
#     # Expose the secret as environment variable.
#     deploy_type='env',
#     # The name of the environment variable, since deploy_type is `env` rather
#     # than `volume`.
#     deploy_target='SQL_CONN',
#     # Name of the Kubernetes Secret
#     secret='airflow-secrets',
#     # Key of a secret stored in this Secret object
#     key='sql_alchemy_conn')
# secret_volume = secret.Secret(
#     'volume',
#     # Path where we mount the secret as volume
#     '/var/secrets/google',
#     # Name of Kubernetes Secret
#     'service-account',
#     # Key in the form of service account file name
#     'service-account.json')

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# If a Pod fails to launch, or has an error occur in the container, Airflow
# will show the task as failed, as well as contain all of the task logs
# required to debug.
with models.DAG(
        dag_id='k8s_pod_op',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:
    # Only name, namespace, image, and task_id are required to create a
    # KubernetesPodOperator. In Cloud Composer, currently the operator defaults
    # to using the config file found at `/home/airflow/composer_kube_config if
    # no `config_file` parameter is specified. By default it will contain the
    # credentials for Cloud Composer's Google Kubernetes Engine cluster that is
    # created upon environment creation.

    afe = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='auto-feature-engineering',  # airflow task id
        name='auto-feature-engineering',  # k8s pod name
        namespace='default',
        image='automl.afe:latest',
        cmds=['echo', 'AFE'])
    pps = kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='preprocessing',
        # Name of task you want to run, used to generate Pod ID.
        name='preprocessing',
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        cmds=['echo','PPS'],
        namespace='default',
        image='automl.pps:latest')
    trn_1 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='train-models__SVM__',
        name='train-models__SVM__',
        namespace='default',
        image='automl.trn:latest',
        cmds=['echo', 'TRN_1'])
    trn_2 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='train-models__TENSORFLOW__',
        name='train-models__TENSORFLOW__',
        namespace='default',
        image='automl.trn:latest',
        cmds=['echo', 'TRN_2'])     
    trn_3 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='train-models__LIGHTGBM__',
        name='train-models__LIGHTGBM__',
        namespace='default',
        image='automl.trn:latest',
        cmds=['echo', 'TRN_3'])                
    sel = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='select-best-model',
        name='select-best-model',
        namespace='default',
        image='automl.sel:latest',
        cmds=['echo', '$\{EXAMPLE_VAR\}'],
        env_vars={
            'EXAMPLE_VAR': '/example/value',
            'GOOGLE_APPLICATION_CREDENTIALS': '/var/secrets/google/service-account.json'})

    afe >> pps >> [trn_1, trn_2, trn_3] >> sel

