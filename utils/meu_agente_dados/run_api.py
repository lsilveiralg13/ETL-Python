"""Inicia a API em socket dual-stack (IPv4 + IPv6) para o Fly.io.

O health check do Fly conecta por IPv4 e o frontend acessa o backend pela rede
privada 6PN (IPv6). Um bind só em "0.0.0.0" ou só em "::" quebra um dos dois.
"""

import os
import socket

import uvicorn


def criar_socket(porta: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    sock.bind(("::", porta))
    sock.set_inheritable(True)
    return sock


def main() -> None:
    porta = int(os.getenv("PORT", "8000"))
    sock = criar_socket(porta)
    config = uvicorn.Config("utils.meu_agente_dados.main:app")
    uvicorn.Server(config).run(sockets=[sock])


if __name__ == "__main__":
    main()
