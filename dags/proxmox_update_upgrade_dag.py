import logging
import concurrent.futures

from datetime import datetime,timedelta

from airflow import DAG
from airflow.sdk import task
from airflow.providers.ssh.hooks.ssh import SSHHook
# Python

def update_pct(pct_id:str):
    logger = logging.getLogger("airflow.task")
    logger.info(f'🕐 Actualizando {pct_id}...')
    ssh_hook = SSHHook(ssh_conn_id='proxmox_ssh')
    command = f"""pct exec {pct_id} -- bash -c '
        if [ -f /etc/alpine-release ]; then
            apk update && apk upgrade && apk fix && apk cache clean
        elif [ -f /etc/debian_version ]; then
            apt-get update && apt-get upgrade -y && apt-get autoremove -y && apt-get clean
        else
            echo "OS no soportado" >&2; exit 1;
        fi
    '"""
    try:
        with ssh_hook.get_conn() as ssh_client:
            logger.info(f"🪏 Ejecutando comando: {command} ")

            _, stdout, stderr = ssh_client.exec_command(command=command)

            _ = stdout.read().decode('utf-8').strip()
            errores = stderr.read().decode('utf-8').strip()

            if(errores):
                logger.warning(f"⚠️ PCT {pct_id} Salida del stderr: {errores}")
    except Exception as e:
        logger.error(f"❌ Fallo en el  de {pct_id}ssh: {e}")
        raise e


@task
def list_pct():
    logger = logging.getLogger("airflow.task")
    logger.info('📝 Enlistando pcts...')
    ssh_hook = SSHHook(ssh_conn_id='proxmox_ssh')
    command: str = "pct list"
    
    try:
        with ssh_hook.get_conn() as ssh_client:

            logger.info(f"🪏 Ejecutando comando: {command} ")

            _, stdout, stderr = ssh_client.exec_command(command=command)

            salida = stdout.read().decode('utf-8').strip()
            errores = stderr.read().decode('utf-8').strip()

            if(errores):
                logger.warning(f"⚠️ Enlistando pct Salida del stderr: {errores}")
            
    except Exception as e:
        logger.error(f"Fallo en el ssh: {e}")
        raise e

    ids_pct = []
    for line in salida.split('\n')[1:]:

        if not line.strip():
            continue
        
        part =  line.split()
        if len(part) > 1 and part[1] == 'running':
            ids_pct.append(part[0])
            
    logger.info('📝 listado completado...')
    return ids_pct

@task
def update_pcts(pct_ids):
    logger = logging.getLogger("airflow.task")
    logger.info(f"🚀 Iniciando actualización concurrente para: {pct_ids}")

    max_workers = 5
    resultados = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = {executor.submit(update_pct,pct_id):pct_id for pct_id in pct_ids}

        for future in concurrent.futures.as_completed(futures):
            resultados.append(future.result())

    return resultados

@task
def update_host():
    logger = logging.getLogger("airflow.task")
    logger.info('🕐 Actualizando host...')
    ssh_hook = SSHHook(ssh_conn_id='proxmox_ssh')

    command = "apt-get update && apt-get upgrade -y && apt-get autoremove -y && apt-get clean"

    try:
        with ssh_hook.get_conn() as ssh_client: 

            _, stdout, stderr = ssh_client.exec_command(command=command)
            
            _ = stdout.read().decode('utf-8').strip()
            errores = stderr.read().decode('utf-8').strip()

            if(errores):
                logger.warning(f"⚠️ HOST Salida del stderr: {errores}")

    except Exception as e:
        logger.error(f"❌ Fallo ssh update host: {e}")
        raise e
# DAGS

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['jafetdc16@gmail.com','cporvenir3102@gmail.com'],
    'retries': 1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    dag_id='proxmox_lxc_updater_dag',
    default_args=default_args,
    description='Actualización automatizada y determinista de nodos LXC y Host Proxmox',
    schedule='0 2 * * *', # Se efectuara diariamente a las 2 am 
    start_date= datetime(2026,8,7), #ISO DEFAULT ARGS YYYY-MM-DD
    catchup=False,
    tags=['Promox','Infraestructura','Mantenimiento'],
)as dag:
    ids = list_pct()
    update_pcts(ids)
    update_host()
