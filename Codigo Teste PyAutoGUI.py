import pyautogui
import time

print("Posicione o mouse SOBRE A SETINHA em 10 segundos...")
time.sleep(10)
print("SETINHA:", pyautogui.position())

print("\nPosicione o mouse SOBRE O EXCEL em 10 segundos...")
time.sleep(10)
print("EXCEL:", pyautogui.position())