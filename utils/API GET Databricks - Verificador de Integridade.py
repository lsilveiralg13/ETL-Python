import requests

url = "https://status.azuredatabricks.net/1.0/status/5d49ec10226b9e13cb6a422e"
response = requests.get(url)
data = response.json()

# Status geral
status_overall = data["result"]["status_overall"]
print(f"Status: {status_overall['status']} (code: {status_overall['status_code']})")
print(f"Atualizado em: {status_overall['updated']}")

# Verificar cada serviço
for service in data["result"]["status"]:
    print(f"\n{service['name']}: {service['status']}")
    for region in service.get("containers", []):
        print(f"  - {region['name']}: {region['status']}")

# Incidentes ativos
incidents = data["result"]["incidents"]
if incidents:
    print("\n⚠️ Incidentes ativos:")
    for inc in incidents:
        print(f"  - {inc}")
else:
    print("\n✅ Nenhum incidente ativo")