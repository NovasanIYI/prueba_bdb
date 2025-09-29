CREATE TABLE clientes (
    cliente_id INT PRIMARY KEY,
    nombre VARCHAR(100),
    ciudad VARCHAR(50),
    fecha_registro DATE
);

INSERT INTO clientes VALUES
(1, 'Ana Torres', 'Bogotá', '2024-03-01'),
(2, 'Juan Pérez', 'Medellín', '2023-12-15'),
(3, 'Carlos López', 'Cali', '2024-01-20');