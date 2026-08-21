update ds_sandbox_image
set enabled = 0, updated_at = datetime('now')
where id = 'img-secretflow';
