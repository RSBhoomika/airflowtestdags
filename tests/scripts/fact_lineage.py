def emit_lineage_by_query(script_name: str):
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.entity.services.databaseService import DatabaseService
    import os
    server_config = OpenMetadataConnection(
        hostPort="http://35.203.40.184:31291/api",
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImF1dG9waWxvdGFwcGxpY2F0aW9uYm90Iiwicm9sZXMiOltudWxsXSwiZW1haWwiOiJhdXRvcGlsb3RhcHBsaWNhdGlvbmJvdEBvcGVubWV0YWRhdGEub3JnIiwiaXNCb3QiOnRydWUsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTc1MDg1NTYyMywiZXhwIjpudWxsfQ.LFVF6sq6yoXv-1uZSpPXymLuzutPW_V5NEZE446LCTSGkgXkQyTi4fN0LTjJcgMqth2I79cW9A0ue7dMIkW3_sKqYHYeCSb8dpSjzmjk-x_RYE1yRb6y9Sd6FigZhmrQHlVeoaun9NO9N6bYgXagXROqpnt94yqQi7ZwXV4nV-3Jd99ueHtsLll0pH9vEPI5eBeD1YwHaAEZyhBZhOGIhlb2PppbBL4wfMScD-SftgSGYvNEVqQvuvgIqQAU2PVTYChKLyhQW4m51rf1poHT7gtwJyLagYfEXbxOWg6gWoMNSCQeiuAPJdV7Tu5VqW73eI1pe5KYT742tNM9u4QRqw"
        ),
    )

    metadata = OpenMetadata(server_config)

    
    database_service = metadata.get_by_name(
        entity=DatabaseService,
        fqn="demo_environment"
    )

    script_path = f"/opt/airflow/dags/repo/Airflow/MOVE/Dags/scripts/{script_name}.sql"

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





