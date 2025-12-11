# DCe cancellation Lambda

This repository contains a Python AWS Lambda handler that validates DCe cancellation events according to the SVRS *DCe Recepção Evento* contract, enqueues events to SQS, and records cancellation status in DynamoDB.

## Package layout

- `lambada_handler.py` – Lambda entry point that validates payloads, dispatches messages to SQS, and upserts cancellation status in DynamoDB.
- `src/domain/validation.py` – Request validation aligned with the simplified cancellation contract (`id`, `cancelReason`) and client ID requirements.
- `src/config/config.py` – Environment-driven configuration for queue/table names and key attributes.
- `src/adapters/clients.py` – Thin factories for SQS and DynamoDB boto3 clients.

## Deployment and configuration

Deploy the Lambda with the source under `src/` and configure the following environment variables:

- `SQS_QUEUE_URL` – URL of the SQS queue that receives cancellation events.
- `DCE_TABLE_NAME` – DynamoDB table to store DCe cancellation status.
- `DCE_TABLE_PK` (optional, default `pk`) – Partition key attribute name.
- `DCE_TABLE_SK` (optional, default `sk`) – Sort key attribute name.
- `API_AUTH_TOKEN` – Shared bearer token required in the `Authorization: Bearer <token>` header.
- `LOG_DCE_TABLE_NAME` (optional, default `logDce`) – DynamoDB table used to authorize the `clientId` for a given DCe access key.
The Lambda expects API Gateway or direct invocation payloads with the following structure:

```json
{
  "id": "1234567890",                            // DCe access key
  "cancelReason": "Solicitação de cancelamento por duplicidade."
}
```

The request **must** include a `Client-Id` (or `client-id`) HTTP header. The `clientId` is validated against the
`LOG_DCE_TABLE_NAME` table for the provided `id`. The handler fills in `eventCancelDate` automatically with the current UTC
timestamp before sending the message to SQS and updating DynamoDB.

### SQS message format

Messages published to SQS include the validated payload plus a `correlationId` (either provided via `X-Correlation-Id`, `correlationId` in the body, or generated). The correlation ID is also included as an SQS message attribute for traceability.

### DynamoDB record strategy

The Lambda upserts a record keyed by `pk="DCE#{dceId}"` and `sk="LATEST"` (or the configured attribute names) and updates:

- `status` → `CANCELLATION_REQUESTED`
- `operationStatus` → `RECEIVED`
- `correlationId`
- `eventCode`
- `eventTimestamp`
- `updatedAt`/`requestedAt` (server-side timestamps)
- `cancellationReason`
- `clientId`

This approach preserves a single authoritative cancellation status per DCe while keeping keys configurable.

### IAM permissions

The Lambda execution role needs permissions to:

- `sqs:SendMessage` on the configured queue.
- `dynamodb:UpdateItem` on the configured table.

## Explicação (visão geral em português)

1. **Recepção do evento** – O Lambda exposto pelo API Gateway recebe o corpo JSON e extraí o payload do campo `body` quando ele vem em formato string.
2. **Validação** – O módulo `validation.py` garante que o evento siga o contrato simplificado: campos obrigatórios `id` e `cancelReason`. Durante a validação, o `eventCancelDate` é preenchido automaticamente com o horário atual em UTC.
3. **Autorização do client** – Antes de prosseguir, o handler valida o `clientId` enviado no header (`Client-Id`/`client-id`) consultando a tabela DynamoDB `logDce` (ou a definida em `LOG_DCE_TABLE_NAME`) buscando um item com a combinação da chave de acesso (`id`) e do `clientId` informado. Se não encontrar, retorna 403 informando que o cliente não pode cancelar aquele DCe.
4. **Correlação e enfileiramento** – O handler gera ou reutiliza um `correlationId` (de cabeçalho, corpo ou UUID novo) e publica o payload validado no SQS configurado (`SQS_QUEUE_URL`), anexando o ID também como atributo da mensagem para rastreabilidade.
5. **Persistência no DynamoDB** – Em seguida, a função atualiza (ou cria) um registro na tabela (`DCE_TABLE_NAME`) usando as chaves configuráveis (`DCE_TABLE_PK` e `DCE_TABLE_SK`). O item recebe `status`/`operationStatus` para o cancelamento, `correlationId`, código do evento, timestamps (`eventTimestamp`, `requestedAt`/`updatedAt`), além do `cancellationReason` e `clientId`.
6. **Tratamento de erros** – Falhas de validação retornam HTTP 400; erros do SQS ou DynamoDB retornam HTTP 502; qualquer exceção inesperada retorna HTTP 500, mantendo logs estruturados para diagnóstico.

Com isso, o fluxo cobre desde a validação rígida do contrato até a orquestração dos efeitos colaterais (fila e banco), deixando filas, tabelas e chaves totalmente configuráveis por variáveis de ambiente.

## Development

### Installing dependencies

Install runtime and test dependencies locally:

```bash
pip install -r requirements-dev.txt
```

### Running tests

```bash
pytest
```

### Local end-to-end demo (sem AWS)

Para exercitar o fluxo completo sem precisar de recursos reais da AWS, use o script com `moto` que cria filas e tabelas em memória:

```bash
pip install -r requirements-dev.txt  # garante que "moto" está instalado
python scripts/local_flow_demo.py
```

O script:

1. Sobe SQS e DynamoDB falsos via `moto`.
2. Configura variáveis de ambiente (`SQS_QUEUE_URL` e `DCE_TABLE_NAME`) apontando para esses recursos.
3. Invoca o handler com um payload de cancelamento de exemplo.
4. Exibe a resposta do Lambda, a mensagem enfileirada e o item gravado no DynamoDB, permitindo verificar o comportamento sem depender da AWS.
