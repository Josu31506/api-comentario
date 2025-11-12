import os
import json
import uuid
import boto3
from datetime import datetime, timezone

s3 = boto3.client('s3')

def _parse_event_body(event):
    """
    Soporta body como dict (Lambda test) o string (API Gateway proxy).
    Espera campos: tenant_id, texto
    """
    body = event.get('body')
    if body is None:
        # Algunos mapeos directos ya envían el JSON como dict dentro de 'body'
        body = event
    if isinstance(body, str):
        body = json.loads(body)
    return body

def lambda_handler(event, context):
    # --- Entrada ---
    print("Event recibido:", json.dumps(event)[:800])
    body = _parse_event_body(event)
    tenant_id = body['tenant_id']
    texto = body['texto']

    # --- Variables de entorno ---
    bucket_ingesta = os.environ['BUCKET_INGESTA']  # definido en serverless.yml

    # --- Proceso (UUID v1 + key estructurada) ---
    uuidv1 = str(uuid.uuid1())  # v1 como se pide en el curso
    now = datetime.now(timezone.utc)
    y, m, d = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

    # Estructuramos por stage/fecha/tenant:
    stage = os.environ.get('AWS_STAGE') or os.environ.get('STAGE') or os.environ.get('AWS_STAGE_NAME') or os.environ.get('ENV', 'dev')
    # Si no hay variable de stage autodefinida, usamos 'dev' por defecto
    key = f"{stage}/{tenant_id}/{y}/{m}/{d}/{uuidv1}.json"

    # Payload a almacenar (puedes enriquecerlo si deseas)
    comentario = {
        "tenant_id": tenant_id,
        "uuid": uuidv1,
        "detalle": {"texto": texto},
        "timestamp_utc": now.isoformat()
    }

    # --- Ingesta Push a S3 ---
    s3.put_object(
        Bucket=bucket_ingesta,
        Key=key,
        Body=json.dumps(comentario, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
        # No usamos ACLs para evitar errores con buckets que las bloquean
    )

    # --- Respuesta ---
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "bucket": bucket_ingesta, "key": key, "comentario": comentario}, ensure_ascii=False)
    }
