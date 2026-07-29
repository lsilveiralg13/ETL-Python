from cryptography.fernet import Fernet

key = Fernet.generate_key()

with open("chave.key", "wb") as f:
    f.write(key)

print("Chave criada com sucesso!")