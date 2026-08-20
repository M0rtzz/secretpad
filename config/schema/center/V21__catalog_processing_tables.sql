-- Catalog CSV assets are logical governance tables backed by immutable MinIO objects.
update ds_data_asset set datatable_id=id,updated_at=CURRENT_TIMESTAMP
where deleted=0 and modality='TABULAR' and (datatable_id is null or trim(datatable_id)='');
