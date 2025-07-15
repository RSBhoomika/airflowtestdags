def emit_lineage_by_query(script_name: str):
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.entity.services.databaseService import DatabaseService
    import os
    server_config = OpenMetadataConnection(
        hostPort="http://100.94.70.9:32325/api",
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImF1dG9waWxvdGFwcGxpY2F0aW9uYm90Iiwicm9sZXMiOltudWxsXSwiZW1haWwiOiJhdXRvcGlsb3RhcHBsaWNhdGlvbmJvdEBvcGVubWV0YWRhdGEub3JnIiwiaXNCb3QiOnRydWUsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTc1MjU4MDg4OCwiZXhwIjpudWxsfQ.ThRsRbcJLTS0lLeik1zENF8QvCKx4H1XXuFystGkWT-OkWCMub9mHoSahJJv8P-A6ONELberfcqsduVvTIkMmbQNGGfyRgoKEKhi01--iB2nOFsqlWDjK7WpTpaHmaAUdK3RsSuTg8jSeDYpMfXn_9CqlTto-XGEweRCPkVAYeccvUZ5x9XgKbXgYG4T6Wh6CGwtKhErta6dHv-ngxNYHoOFMheIsUqG1HOSVcjYR97q_rg4a4AkSwt98VvcA3sv_nnG5aJr4UDpu3z8NMSZjKZ-CXJHkHnF_mLM51P2uAKXIYQYcaN1yqrTlxiecKiHNyYC39aXPXVJDKtRyKIcGA"
            ),
    )

    metadata = OpenMetadata(server_config)

    
    database_service = metadata.get_by_name(
        entity=DatabaseService,
        fqn="demo_environment"
    )

    script_path = f"/opt/airflow/dags/repo/tests/scripts/{script_name}.sql"

    if not os.path.exists(script_path):
        raise FileNotFoundError(f"SQL script not found: {script_path}")

    with open(script_path, "r") as f:
        sql_query = f.read()
    metadata.add_lineage_by_query(
        database_service=database_service,
        timeout=200,
        sql=sql_query
    )

    print("Lineage emitted for query:", script_name)





