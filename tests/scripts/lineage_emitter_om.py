def emit_lineage_to_om(
    source_fqn: str,
    target_fqn: str,
    column_mappings: list  # mixed: ["col1", ("col2", "target_col2"), ...]
):
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.data.container import Container
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, ColumnLineage, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference

    # Static JWT token (keep secure)
    server_config = OpenMetadataConnection(
        hostPort="http://100.94.70.9:32325/api",
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImF1dG9waWxvdGFwcGxpY2F0aW9uYm90Iiwicm9sZXMiOltudWxsXSwiZW1haWwiOiJhdXRvcGlsb3RhcHBsaWNhdGlvbmJvdEBvcGVubWV0YWRhdGEub3JnIiwiaXNCb3QiOnRydWUsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTc1MjU4MDg4OCwiZXhwIjpudWxsfQ.ThRsRbcJLTS0lLeik1zENF8QvCKx4H1XXuFystGkWT-OkWCMub9mHoSahJJv8P-A6ONELberfcqsduVvTIkMmbQNGGfyRgoKEKhi01--iB2nOFsqlWDjK7WpTpaHmaAUdK3RsSuTg8jSeDYpMfXn_9CqlTto-XGEweRCPkVAYeccvUZ5x9XgKbXgYG4T6Wh6CGwtKhErta6dHv-ngxNYHoOFMheIsUqG1HOSVcjYR97q_rg4a4AkSwt98VvcA3sv_nnG5aJr4UDpu3z8NMSZjKZ-CXJHkHnF_mLM51P2uAKXIYQYcaN1yqrTlxiecKiHNyYC39aXPXVJDKtRyKIcGA"
        ),
    )

    metadata = OpenMetadata(server_config)

    # Fetch entities
    source = metadata.get_by_name(entity=Container, fqn=source_fqn)
    target = metadata.get_by_name(entity=Table, fqn=target_fqn)

    if not source or not target:
        raise ValueError(f"Source or target not found. source={source}, target={target}")

    # Normalize column mappings
    normalized_mappings = []
    for mapping in column_mappings:
        if isinstance(mapping, tuple) and len(mapping) == 2:
            src_col, tgt_col = mapping
        elif isinstance(mapping, str):
            src_col = tgt_col = mapping
        else:
            raise ValueError(f"Invalid column mapping format: {mapping}")
        normalized_mappings.append((src_col, tgt_col))

    # Build lineage
    column_lineage_list = [
        ColumnLineage(
            fromColumns=[f"{source_fqn}.{src}"],
            toColumn=f"{target_fqn}.{tgt}",
        )
        for src, tgt in normalized_mappings
    ]

    lineage_details = LineageDetails(columnsLineage=column_lineage_list)
    add_lineage_request = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(id=source.id, type="container"),
            toEntity=EntityReference(id=target.id, type="table"),
            lineageDetails=lineage_details,
        )
    )

    metadata.add_lineage(add_lineage_request)
    print(f"Column-level lineage emitted from {source_fqn} to {target_fqn}")
