# create_sqlite_db.py
from data_sqlite import dbCreateTable, dbInsertTrade, dbInsertPos, dbInsertPnl
import os

# cria o arquivo e tabelas
dbCreateTable()

# insere alguns dados de exemplo (opcionais)
# Exemplo: uma compra de SoyBean
dbInsertTrade("SoyBean", "FOB Vessel", "VSL", 2025, "Purchase", 100, 0.36, 1323.0)
dbInsertPos("SoyBean", "FOB Vessel", "VSL", 2025, 100)

# Exemplo MTM/PnL entry
dbInsertPnl(1, "SoyBean", "FOB Vessel", "VSL", 2025, 0.37, 500.0)

print("pnl.db criado/atualizado. Commit e push pnl.db para o repo se desejar testar o app hospedado.")
