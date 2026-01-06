import oracledb

# Thin mode (default) – no Instant Client needed
conn = oracledb.connect(
    user="system",
    password="YourPassword123",
    dsn="localhost/XEPDB1"
)

print("Connected to Oracle Database!")

cursor = conn.cursor()
cursor.execute("SELECT sysdate FROM dual")
print("Database time:", cursor.fetchone())

cursor.close()
conn.close()
