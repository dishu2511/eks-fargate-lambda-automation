import base64
import logging
import re
import boto3
from kubernetes import client, config
import os
from botocore.signers import RequestSigner

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS SDK clients
session = boto3.session.Session()
sts = session.client("sts")
service_id = sts.meta.service_model.service_id
eks = boto3.client("eks")
dynamodb_client = boto3.client("dynamodb")

# Define constants
TABLE_NAME = os.environ.get("TABLE_NAME")
CLUSTER_CACHE = {}


def get_eks_client(account_id, target_region):
    role_arn = f"arn:aws:iam::{account_id}:role/eks-automation"
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="CrossAccountSession"
    )
    session_credentials = assumed_role["Credentials"]
    return boto3.client(
        "eks",
        region_name=target_region,
        aws_access_key_id=session_credentials["AccessKeyId"],
        aws_secret_access_key=session_credentials["SecretAccessKey"],
        aws_session_token=session_credentials["SessionToken"],
    )


def get_cluster_info(account_id, target_region):
    if (account_id, target_region) in CLUSTER_CACHE:
        return CLUSTER_CACHE[(account_id, target_region)]

    eks_client = get_eks_client(account_id, target_region)
    response = eks_client.list_clusters()
    cluster_names = response["clusters"]
    CLUSTER_NAME = cluster_names[0]
    if CLUSTER_NAME not in cluster_names:
        raise Exception(
            f"No cluster named {CLUSTER_NAME} found in the account and region."
        )

    cluster_info = eks_client.describe_cluster(name=CLUSTER_NAME)["cluster"]
    endpoint = cluster_info["endpoint"]
    cert_authority = cluster_info["certificateAuthority"]["data"]
    cluster_details = {"endpoint": endpoint, "ca": cert_authority}

    CLUSTER_CACHE[(account_id, target_region)] = cluster_details
    return cluster_details


def get_token(cluster_name):
    signer = RequestSigner(
        service_id,
        session.region_name,
        "sts",
        "v4",
        session.get_credentials(),
        session.events,
    )

    params = {
        "method": "GET",
        "url": f"https://sts.{session.region_name}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
        "body": {},
        "headers": {"x-k8s-aws-id": cluster_name},
        "context": {},
    }

    signed_url = signer.generate_presigned_url(
        params,
        region_name=session.region_name,
        expires_in=60,
        operation_name="",
    )
    base64_url = base64.urlsafe_b64encode(signed_url.encode("utf-8")).decode("utf-8")
    return "k8s-aws-v1." + re.sub(r"=*", "", base64_url)


def scale_deployments(namespace, replicas):
    """Scale deployments in a given namespace to the specified number of replicas."""
    v1_apps = client.AppsV1Api()
    deployments = v1_apps.list_namespaced_deployment(namespace).items

    for deployment in deployments:
        scale_body = {"spec": {"replicas": replicas}}
        try:
            v1_apps.patch_namespaced_deployment_scale(
                name=deployment.metadata.name, namespace=namespace, body=scale_body
            )
            action_text = "stopped" if replicas == 0 else "started"
            logger.info(
                f"Deployment {deployment.metadata.name} in {namespace} has been {action_text}."
            )
        except client.exceptions.ApiException as e:
            logger.error(
                f"Error scaling deployment {deployment.metadata.name} in {namespace}: {str(e)}"
            )


def check_pods(namespace):
    v1_core = client.CoreV1Api()
    pods = v1_core.list_namespaced_pod(namespace).items
    if not pods:
        logger.info(f"No pods found in the namespace: {namespace}")
        return []

    deployment_info = []
    for pod in pods:
        pod_status = pod.status.phase
        pod_name = pod.metadata.name
        if pod_status == "Failed":
            logger.error(f"Pod {pod_name} in namespace {namespace} has failed.")
            raise ErrorTaskException(f"Pod {pod_name} has failed.")
        elif pod_status == "Pending":
            logger.warning(f"Pod {pod_name} in namespace {namespace} is still pending.")
            error_handling(
                f"Pod {pod_name} in namespace {namespace} is in {pod_status} state."
            )
        elif pod_status in ["Succeeded", "Running"]:
            logger.info(
                f"Pod {pod_name} in namespace {namespace} is {pod_status.lower()}."
            )
        else:
            logger.warning(
                f"Pod {pod_name} in namespace {namespace} is in an unknown state: {pod_status}"
            )
        deployment_info.append({"pod_name": pod_name, "status": pod_status})
    return deployment_info


def lambda_handler(event, _context):
    scheduled_action = event.get("scheduled_action", "stop")
    action_type = event.get("action_type", "scale")
    replicas = 0 if scheduled_action == "stop" else 1
    customer_table = dynamodb_client.scan(TableName=TABLE_NAME)
    account_details = customer_table["Items"]
    action_taken = []
    overall_status = "successful"
    detailed_messages = []
    for account_detail in account_details:
        CLUSTER_NAME = account_detail.get("CLUSTER_NAME", {}).get("S", "")
        TARGETED_NAMESPACES = [
            ns["S"] for ns in account_detail.get("TARGETED_NAMESPACES", {}).get("L", [])
        ]
        account_id = account_detail.get("account_id", {}).get("S", "")
        target_region = account_detail.get("target_region", {}).get("S", "")
        cluster_info = get_cluster_info(account_id, target_region)
        kubeconfig = {
            "apiVersion": "v1",
            "clusters": [
                {
                    "name": CLUSTER_NAME,
                    "cluster": {
                        "certificate-authority-data": cluster_info["ca"],
                        "server": cluster_info["endpoint"],
                    },
                }
            ],
            "contexts": [
                {
                    "name": "context1",
                    "context": {"cluster": CLUSTER_NAME, "user": "user1"},
                }
            ],
            "current-context": "context1",
            "kind": "Config",
            "users": [{"name": "user1", "user": {"token": get_token(CLUSTER_NAME)}}],
        }

        config.load_kube_config_from_dict(kubeconfig)

        for namespace in TARGETED_NAMESPACES:
            if action_type == "scale":
                try:
                    scale_deployments(namespace, replicas)
                    action_text = "stopped" if scheduled_action == "stop" else "started"
                    logger.info(f"Deployments in {namespace} have been {action_text}.")
                    action_taken.append(namespace)
                except Exception as e:
                    logger.error(
                        f"Error performing action {scheduled_action} on deployments in {namespace}: {str(e)}"
                    )
                    detailed_messages.append(f"Error in {namespace}: {str(e)}")
                    overall_status = "failed"

            elif action_type == "check_status":
                try:
                    deployment_info = check_pods(namespace)
                    if deployment_info:
                        for info in deployment_info:
                            message = f"Namespace '{namespace}', Pod '{info['pod_name']}': {info['status']} status"
                            detailed_messages.append(message)
                except Exception as e:
                    logger.error(
                        f"Error checking Pod status in namespace '{namespace}': {str(e)}"
                    )
                    detailed_messages.append(
                        f"Error checking status in {namespace}: {str(e)}"
                    )
                    overall_status = "failed"

    if action_taken or detailed_messages:
        return {
            "statusCode": 200 if overall_status == "successful" else 400,
            "status": overall_status,
            "body": (
                " ; ".join(detailed_messages)
                if detailed_messages
                else f"Deployments in namespaces {', '.join(action_taken)} have been {action_text}."
            ),
        }
    else:
        return {
            "statusCode": 201,
            "status": "no-action",
            "body": "No action was taken. No Pods found.",
        }
