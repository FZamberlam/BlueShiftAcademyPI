# -*- coding: utf-8 -*-
"""
Created on Sun Mar 28 22:37:01 2021

@author: Zamberlam
"""

# Programa pra converter os dados de cursos, código ou área do Anexo 5 - Cursos de Formação Superior em uma Lista

def readFile(fileName):
        fileObj = open(fileName, "r") # Abre o arquivo em Modo de Leitura
        words = fileObj.read().splitlines() # Coloca os dados do arquivo em um array
        fileObj.close()
        return words

fileLocation = ""
text_file = open(fileLocation, "r", encoding="utf8")
lines = text_file.readlines()

newLines = []
for element in lines:
    newLines.append(element.strip()) # Pega os dados da Lista lines e remove o '\n' de cada item

# Print da Lista criada e o tamanho para comparação
print(newLines)
print(len(newLines))
text_file.close()