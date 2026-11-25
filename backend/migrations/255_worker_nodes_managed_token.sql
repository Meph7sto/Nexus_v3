-- Existing Worker nodes keep resolving their credentials through secret_ref.
ALTER TABLE worker_nodes
    ADD COLUMN IF NOT EXISTS encrypted_token TEXT NOT NULL DEFAULT '';
