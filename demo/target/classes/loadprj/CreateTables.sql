CREATE TABLE link_table (
    id serial PRIMARY KEY,
    group_value VARCHAR(255) NOT NULL
);

CREATE TABLE data_table (
    id serial PRIMARY KEY,
    col1 VARCHAR(255),
    col2 VARCHAR(255),
    col3 VARCHAR(255),
    col4 VARCHAR(255),
    -- Other columns...
    link_id INT,
    FOREIGN KEY (link_id) REFERENCES link_table(id)
);
