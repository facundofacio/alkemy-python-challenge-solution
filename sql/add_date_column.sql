ALTER TABLE {tab}
    ADD COLUMN {col} DATE;
UPDATE {tab}
    SET {col} = current_date;