-- A DB Architect would likely argue whether this setup is the optimal :)
-- Some values in some fields (eg. color, product_type) are impressively big in the raw example, 
--if it can be even bigger in real life, this should be architected in a way that won't cause trouble. 

-- Columns in raw: variant_id product_id size_label product_name brand color age_group gender size_type product_type 

CREATE TABLE link_size_label (
    id serial PRIMARY KEY,
    group_value VARCHAR(20) NOT NULL
);

CREATE TABLE link_brand (
    id serial PRIMARY KEY,
    group_value VARCHAR(100) NOT NULL
);
CREATE TABLE link_color (
    id serial PRIMARY KEY,
    group_value VARCHAR(99) NOT NULL
);
CREATE TABLE link_age_group (
    id serial PRIMARY KEY,
    group_value VARCHAR(10) NOT NULL
);
CREATE TABLE link_gender (
    id serial PRIMARY KEY,
    group_value VARCHAR(10) NOT NULL
);
CREATE TABLE link_size_type (
    id serial PRIMARY KEY,
    group_value VARCHAR(10) NOT NULL
);

--variant_id product_id size_label product_name brand color age_group gender size_type product_type 
-- A potential improvement I played here with is storing product_id as INT, maybe it is more effective for searching? I am not sure, a DB expert would be able to say.
              
CREATE TABLE data_table (
    id serial PRIMARY KEY,
    variant_id VARCHAR(15),
    product_id INT,
    product_name VARCHAR(900),
    product_type VARCHAR(1000),
    -- Link columns...
    size_label INT,
    brand INT,
    color INT,
    age_group INT,
    gender INT,
    size_type INT,
    FOREIGN KEY (size_label) REFERENCES link_size_label(id),
    FOREIGN KEY (brand) REFERENCES link_brand(id),
    FOREIGN KEY (color) REFERENCES link_color(id),
    FOREIGN KEY (age_group) REFERENCES link_age_group(id),
    FOREIGN KEY (gender) REFERENCES link_gender(id),
    FOREIGN KEY (size_type) REFERENCES link_size_type(id)
);
