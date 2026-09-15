INSERT INTO users (name, balance)
SELECT 'Soheil', 10000
WHERE NOT EXISTS (SELECT 1 FROM users WHERE name = 'Soheil');
