#!/bin/bash
# diagnostic_postgres.sh - Diagnóstico completo PostgreSQL

echo "🔍 DIAGNÓSTICO POSTGRESQL EPIDEMIOLOGÍA TOLIMA"
echo "=============================================="

# 1. Verificar servicios PostgreSQL
echo -e "\n1️⃣ SERVICIOS POSTGRESQL:"
if command -v systemctl >/dev/null 2>&1; then
    echo "📊 Estado servicio PostgreSQL:"
    sudo systemctl is-active postgresql 2>/dev/null || echo "❌ PostgreSQL no está activo via systemctl"
fi

# 2. Verificar Docker
echo -e "\n2️⃣ CONTENEDORES DOCKER:"
if command -v docker >/dev/null 2>&1; then
    echo "📊 Contenedores PostgreSQL:"
    docker ps | grep postgres || echo "❌ No hay contenedores PostgreSQL corriendo"
    
    echo -e "\n📊 Contenedores detenidos:"
    docker ps -a | grep postgres || echo "ℹ️ No hay contenedores PostgreSQL"
fi

# 3. Procesos PostgreSQL
echo -e "\n3️⃣ PROCESOS POSTGRESQL:"
ps aux | grep postgres | grep -v grep || echo "❌ No hay procesos PostgreSQL"

# 4. Puertos en uso
echo -e "\n4️⃣ PUERTO 5432:"
netstat -tlnp | grep :5432 || ss -tlnp | grep :5432 || echo "❌ Puerto 5432 no está en uso"

# 5. Variables de entorno
echo -e "\n5️⃣ VARIABLES DE ENTORNO:"
if [ -f ".env" ]; then
    echo "📄 Archivo .env encontrado:"
    cat .env | grep -E "(DB_|POSTGRES_)" || echo "ℹ️ No hay variables de BD en .env"
else
    echo "❌ No se encontró archivo .env"
fi

# 6. Docker Compose
echo -e "\n6️⃣ DOCKER COMPOSE:"
if [ -f "docker-compose.yml" ]; then
    echo "📄 Docker Compose encontrado"
    echo "📊 Servicios definidos:"
    grep -A 2 "services:" docker-compose.yml
else
    echo "❌ No se encontró docker-compose.yml"
fi

# 7. Archivos de configuración PostgreSQL
echo -e "\n7️⃣ CONFIGURACIÓN POSTGRESQL:"
for pg_config in /etc/postgresql/*/main/postgresql.conf /var/lib/postgresql/data/postgresql.conf; do
    if [ -f "$pg_config" ]; then
        echo "📄 Encontrado: $pg_config"
    fi
done

echo -e "\n🔧 COMANDOS PARA SOLUCIONAR:"
echo "• Iniciar Docker: docker-compose up -d postgres"
echo "• Conectar directo: psql -h localhost -U tolima_admin -d epidemiologia_tolima"
echo "• Ver logs Docker: docker-compose logs postgres"
echo "• Reiniciar servicio: sudo systemctl restart postgresql"

echo -e "\n✅ Diagnóstico completado"