-- Create task_states table for tracking task execution state
CREATE TABLE IF NOT EXISTS task_states (
    task_id VARCHAR(255) PRIMARY KEY,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index on updated_at for efficient cleanup
CREATE INDEX IF NOT EXISTS idx_task_states_updated_at ON task_states(updated_at);

-- Create function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_task_state_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_task_state_timestamp
    BEFORE UPDATE ON task_states
    FOR EACH ROW
    EXECUTE FUNCTION update_task_state_timestamp(); 