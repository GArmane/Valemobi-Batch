CREATE TABLE IF NOT EXISTS tb_customer_account(
id_customer INTEGER PRIMARY KEY AUTOINCREMENT, 
cpf_cnpj text NOT NULL, 
nm_customer varchar text NOT NULL, 
is_active int NOT NULL, 
vl_total real NOT NULL
);
