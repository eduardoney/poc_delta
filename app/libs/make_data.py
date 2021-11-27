import random
import json
from faker import Faker
import numpy    as np
import pandas   as pd
from datetime import datetime, timedelta

def generate_data(qty_records: int):
    # Instancia a classe Faker
    fake = Faker()
    
    agencia = [np.random.choice(["BSB","CGH","GIG","SSA","FLN","POA","VCP","REC","CWB","BEL","VIX","SDU","CGB","CGR",
                                 "FOR","MCP","MGF","GYN","NVT","MAO","NAT","BPS","MCZ","PMW","SLZ","GRU","LDB","PVH",
                                 "RBR","JOI","UDI","CXJ","IGU","THE","AJU","JPA","PNZ","CNF","BVB","CPV","STM","IOS",
                                 "JDO","IMP","XAP","MAB","CZS","PPB","CFB","FEN","JTC","MOC"]) for i in range(qty_records)]
    dt_retirada = [fake.date_between_dates(date_start=datetime.now(), date_end=datetime.now()+timedelta(days=15)) for i in range(qty_records)]
    cod_reserva = [fake.ean(length=8) for i in range(qty_records)]
    cod_grupo = [np.random.choice(["A","B","C","F","FX","GC","GX"], p=[0.10,0.20,0.25,0.10,0.15,0.10,0.10]) for i in range(qty_records)]
    dt_geracao = [datetime.now() for i in range(qty_records)]
    
    df = pd.DataFrame({
            "cod_reserva":cod_reserva,
            "agencia": agencia,
            "dt_retirada": dt_retirada,
            "cod_grupo": cod_grupo,
            "dt_geracao": dt_geracao
        })
    df['dt_devolucao'] = df['dt_retirada']+timedelta(random.randrange(1,15,1))
 
    return df
