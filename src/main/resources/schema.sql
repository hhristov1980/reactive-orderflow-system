CREATE TABLE IF NOT EXISTS products (
                                        id SERIAL PRIMARY KEY,
                                        name VARCHAR(255),
    price DECIMAL,
    stock INT
    );

CREATE TABLE IF NOT EXISTS orders (
                                      id SERIAL PRIMARY KEY,
                                      user_id VARCHAR(255),
    status VARCHAR(50),
    total_amount DECIMAL,
    created_at TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS order_items (
                                           id SERIAL PRIMARY KEY,
                                           order_id BIGINT,
                                           product_id BIGINT,
                                           quantity INT,
                                           price DECIMAL
);