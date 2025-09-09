#!/usr/bin/env python
import pg8000

print("🧪 Probando conexión con pg8000...")

try:
    conn = pg8000.connect(
        host="localhost",
        port=5432,
        database="epidemiologia_tolima",
        user="tolima_admin",
        password="tolima2025",
    )

    print("✅ CONEXIÓN EXITOSA!")

    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    print("🎉 TODO FUNCIONA CON PG8000!")

    conn.close()

except Exception as e:
    print(f"❌ Error: {e}")
