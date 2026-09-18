-- کاربر پیش‌فرض محلی؛ فقط اگر هنوز وجود نداشته باشد ساخته می‌شود.
INSERT INTO users (name, balance)
SELECT 'soheil', 0
WHERE NOT EXISTS (
    SELECT 1 FROM users WHERE lower(name) = 'soheil'
);
