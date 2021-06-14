import requests, json, os, sys
import pytz
import pandas as pd
import dateutil.parser as dp
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import time
import configparser

from elasticsearch import Elasticsearch
from pandasticsearch import Select
from matplotlib.pyplot import figure
from scipy import stats
from scipy.stats import linregress
from datetime import datetime

def read_config(param):
    """
    Lee la configuracion del archivo pasado por parámetro
    """

    config = configparser.ConfigParser()
    config.read(param)
    print(">> Configuracion: {}".format(param))
    conf = {}

    
    try:
        conf["dias"] = config["filtro_temp"].get("NUM_DIAS")
        conf["medida"] = config["filtro_temp"].get("MEDIDA")
        conf["minutos"] = config["ventanas_temp"].getint("MINUTOS")
        conf["umbral_corr"] = config["correlacion"].getfloat("UMBRAL_CORRELACION")
        conf["umbral_p_value"] = config["correlacion"].getfloat("UMBRAL_P_VALUE")
        
    except KeyError as e:
        print("Error al leer la configuracion: {}".format(e))
        sys.exit(1)

    print(">> Filtro temporal de descarga de datos: {}".format(str(conf["dias"])+conf["medida"]))
    print(">> Ventana temporal de duracion (minutos): {}".format(conf["minutos"]))
    print(">> Umbral de correlacion: {}".format(conf["umbral_corr"]))
    print(">> Umbral de p_value: {}".format(conf["umbral_p_value"]))
    
    return conf

# Carga de la configuración
conf = read_config(sys.argv[1])

try:
    while True:  
        res = requests.get('http://138.4.7.132:9200')
        #print (res.content)
        es = Elasticsearch([{'host': '138.4.7.132', 'port': '9200'}])

        time_query = {
            "query": {
                "bool":{
                    "must":[
                        {
                            "range": {
                                "timestamp": {
                                    "gte": "now-{}".format(str(conf["dias"]+conf["medida"]))#GTE (greater than or equal)  
                                }
                            }
                        }
                    ]
                }
            }
        }
       
        try: 
            num_dias_total=int(conf["dias"])
            medida = conf["medida"]
            if medida=="d":
                factor_2=24
            if medida=="H":
                factor_2=1
                
            index1 = es.search(index='redes_moviles_bea',body=time_query, size=999)
            index2 =  es.search(index='radio_frecuencia_bea',body=time_query, size=999)
          
            len_index1=len(index1['hits']['hits'])#rm
            len_index2=len(index2['hits']['hits'])#rf
            if (len_index1 == 0)or(len_index2 ==0): 
                raise Exception(len_index1, len_index2)
              
        except Exception as inst:
            x, y = inst.args 
            print(">> No hay datos, descargados {} hits de redes moviles y {} de radio frecuencia".format(x,y))
            print(f"Delay de {num_dias_total*factor_2} segundos") #num_dias_total*3600
            time.sleep(num_dias_total*factor_2*3600)#num_dias_total*3600
            
        else:
            timestamp_init_index1 = index1['hits']['hits'][0]['_source']['timestamp']
            timestamp_init_index2 = index2['hits']['hits'][0]['_source']['timestamp']
            print(">> Descargados {} hits de redes moviles y {} de radio frecuencia".format(len_index1, len_index2))
            print(">> Timestamp redes moviles {} y timestamp radio frecuencia {}".format(timestamp_init_index1,timestamp_init_index2))
            
            index1_df = Select.from_dict(index1).to_pandas()
            index2_df = Select.from_dict(index2).to_pandas()
            
            ## LIMPIEZA Redes moviles
            ## GUARDAR COLUMNA EPOCHS Redes moviles
            i=0
            j=0
            k=0
            z=0


            for t in index1_df["timestamp"]:
                parsed_t = dp.parse(t)
                t_in_seconds = parsed_t.strftime('%s')
                index1_df.loc[i, "time_epoch"] = t_in_seconds
                #print(time)
                i+=1
                #print(i)

            index1_df["time_epoch"] = index1_df['time_epoch'].astype(int)
            
            # FILTRAR ANOMALIA TRUE
            index1_df = index1_df.loc[(index1_df['anomalia']==True)]

            #LIMPIAR DF Redes moviles
            index1_df = index1_df.drop(columns=['_index',
                                        '_type',
                                        '_id',
                                        '_score',
                                        'version',
                                        'id',
                                        'type',
                                        'event',
                                        'time',
                                        'anomalia'])

            ## LIMPIEZA Radio frecuencia
            ## GUARDAR COLUMNA EPOCHS radio frecuencia
            i=0
            j=0


            for t in index2_df["timestamp"]:
                parsed_t = dp.parse(t)
                t_in_seconds = parsed_t.strftime('%s')
                index2_df.loc[i, "time_epoch"] = t_in_seconds
                #print(time)
                i+=1
                #print(i)


            index2_df["time_epoch"] = index2_df['time_epoch'].astype(int)

            # FILTRAR ANOMALIA TRUE
            index2_df = index2_df.loc[(index2_df['anomalia']==True)]

            #LIMPIAR DF wifi
            index2_df = index2_df.drop(columns=['_index',
                                            '_type',
                                            '_id',
                                            '_score',
                                            'version',
                                            'id',
                                            'type',
                                            'event',
                                            'time',
                                            'anomalia'])

            nombres_index1=[]
            nombres_index2=[]
            
            for x in index1_df:
                if (x != "time_epoch"):
                    nombre_nuevo = "RM_"+str(x)
                    nombres_index1.append(nombre_nuevo)

                else:
                    nombre_nuevo = x
                    nombres_index1.append(nombre_nuevo)

            index1_df = index1_df.set_axis(nombres_index1, axis = 'columns')
            
            for x in index2_df:
                #print(x)
                if (x != "time_epoch"):
                    nombre_nuevo = "RF_"+str(x)
                    nombres_index2.append(nombre_nuevo)

                else:
                    nombre_nuevo = x
                    nombres_index2.append(nombre_nuevo)

            index2_df = index2_df.set_axis(nombres_index2, axis = 'columns')

            #unirlos antes de convertirlos a numerico
            index1_2 = index1_df.copy()
            index2_2 = index2_df.copy()
            

            #Ordenarlos por time epoch!
            df1_df2 = index2_2.append(index1_2).sort_values(by='time_epoch')
            #Reemplazo con 0 los NaN
            for x in df1_df2:
                df1_df2[x] = df1_df2[x].fillna(0)

            #Reseteo index y borro columna nueva de index
            df1_df2_final = df1_df2.reset_index().drop(columns=['index', 'RM_timestamp', 'RF_timestamp'])

            #print(wifi_bt_final.shape)
            #wifi_bt_final

            #Aplico ventanas

            t_0= df1_df2_final.loc[0,"time_epoch"]#dia 12 
            #print(t_0)

            minutos = conf["minutos"]
            factor_1 = float(60/minutos)
            #horas = 1
            # Duracion del tiempo de la ventana (segundos)
            # Ventana x minutos
            t_ventana = minutos*60

            # Ventana x horas
            #t_ventana = horas*60*60

            #num_dias_total=int(conf["dias"])
            #medida = conf["medida"]

            # CAMBIAR EN EL FOR 
            # Ej: ventana cada 15 min: poner en el for ventanas 15_min para que haga 4 iteraciones
            #ventanas_dia=num_dias_total*1
            
            '''if medida=="d":
                factor_2=24
            if medida=="H":
                factor_2=1'''
            
            ventanas_hora=num_dias_total*factor_2*factor_1
            print(f">> Hay {ventanas_hora} ventanas")


            ventana_actual={}

            for i in range(int(ventanas_hora)):

                ventana_actual[i] = df1_df2_final.loc[(df1_df2_final['time_epoch'] >= (t_0)) & (df1_df2_final['time_epoch'] <= (t_0+t_ventana))]
                t_0= t_0+t_ventana
                #i+=1
                #print(t_0)

            # CORRELACION
            num_ventanas = 0
            correlacion = {}
            # Filtro correlacion
            corr_threshold = conf["umbral_corr"]
            strong_08 = {}
            strong_08_05 = {}
            # Filtro parejas mismo dataset
            var1 = {}
            var2 = {}
            vars_1 = {}
            vars_2 = {}
            # Filtro p_value
            p_value_threshold = conf["umbral_p_value"]
            p_values_1={k : [] for k in ventana_actual}
            corr_scipy_1={k : [] for k in ventana_actual}


            for i in ventana_actual:

                if len(ventana_actual[i]) != 0:
                    print(f">> Ventanta {i}")
                    
                    # Inicio/Fin ventanas para campo en ES
                    ventana_actual[i] = ventana_actual[i].reset_index().drop(columns="index")
                    time_init = ventana_actual[i]["time_epoch"].loc[0]
                    time_end = ventana_actual[i]["time_epoch"].loc[len(ventana_actual[i]["time_epoch"])-1]
                    print(f" Time init ventana {i}: {datetime.utcfromtimestamp(time_init).isoformat()}, time end {datetime.utcfromtimestamp(time_end).isoformat()}")

                    # 1.One-hot encoding de cada ventana
                    ventana_actual[i] = pd.get_dummies(ventana_actual[i], drop_first=True, 
                                                                     columns=[
                                                                         'RM_rat',
                                                                         'RM_imei',
                                                                         'RM_imsi',
                                                                         'RF_mod',
                                                                         'RF_payload'
                                                                     ])
                    num_ventanas+=1

                    # 2. Correlacion de cada ventana
                    correlacion[i] = ventana_actual[i].corr(method='pearson')

                    # 3. Limpiar diagonal
                    print(f" Sin borrar la diagonal hay {len(correlacion[i].stack().reset_index())+1} filas")
                    # Mete zeros a la diagonal de abajo
                    correlacion[i] = correlacion[i].mask(np.tril(np.ones(correlacion[i].shape)).astype(np.bool))
                    # Resetea indices
                    correlacion[i] = correlacion[i].stack().reset_index()
                    print(f" Borrando la diagonal hay {len(correlacion[i])} filas")

                    correlacion[i] = correlacion[i].rename(columns={"level_0": "var1", "level_1": "var2", 0: "corr_pandas_pearson"})
                    #print(ventana_actual[i])

                    '''
                    # Para pintar matriz correlacion
                    mask = np.zeros_like(globals()["corr_ventana_"+str(i)])
                    mask[np.triu_indices_from(mask)] = True
                    with sns.axes_style("white"):
                        f, ax = plt.subplots(figsize=(30, 30))

                        ax = sns.heatmap(globals()["corr_ventana_"+str(i)], mask=mask, vmax=1, square=True, annot=True)
                        ax.set_xlabel(f'ventana{i}',fontsize=20)
                    '''
                    # 4. Filtrar por valor de correlacion (0.8)
                    #strong_5: 0.8-1 (sin incluir 1)
                    strong_08[i] = correlacion[i][abs(correlacion[i]["corr_pandas_pearson"]) > corr_threshold]
                    strong_08[i] = strong_08[i][abs(strong_08[i]["corr_pandas_pearson"]) < 1].reset_index().drop(columns=['index'])
                    print(f" Filtrando por correlacion mayor a {corr_threshold}, hay {len(strong_08[i])} parejas")

                    # 5. Filtrar parejas del mismo dataset
                    for j in range(len(strong_08[i])):

                        var1 = strong_08[i].loc[j, "var1"]
                        var2 = strong_08[i].loc[j, "var2"]
                        if (var1.startswith('RF') & var2.startswith('RF')):
                            strong_08[i] = strong_08[i].drop([j])
                        if (var1.startswith('RM') & var2.startswith('RM')):
                            strong_08[i] = strong_08[i].drop([j])

                    strong_08[i] = strong_08[i].reset_index().drop(columns="index")
                    print(f" Eliminando parejas del mismo dataset quedan {len(strong_08[i])} parejas")

                    # 6. Añadir p_value
                    var1 = strong_08[i]["var1"]
                    var2 = strong_08[i]["var2"]
                    #print(var1, var2)
                    for z, y in zip(var1, var2):
                        #print(z, y)
                        #print(nombre_var1, nombre_var2)
                        vars_1[i] = ventana_actual[i][z]
                        vars_2[i] = ventana_actual[i][y]
                        #print(vars_1, vars_2)
                        corr_value, p_value_i = stats.spearmanr(vars_1[i], vars_2[i])

                        p_values_1[i].append(p_value_i)
                        corr_scipy_1[i].append(corr_value)

                    strong_08[i]["p-value"] = p_values_1[i]
                    strong_08[i]["corr_scipy_spearman"] = corr_scipy_1[i]

                    # 7. Filtrar por valor de p_value (< 0,05) 
                    print(f" Hay {len(strong_08[i])} parejas sin filtrar p_value")
                    strong_08_05[i] = strong_08[i][abs(strong_08[i]["p-value"]) < p_value_threshold].reset_index().drop(columns=['index'])
                    print(f" Quedan {len(strong_08_05[i])} parejas con p_value < {p_value_threshold}")
                    #print((strong_08_05[0]))

                    # 8. Enviar a ES
                    related= {}

                    # Añadir todas las parejas restantes a related_events
                    for k in strong_08_05:
                        #print(strong_08_05[i])
                        j = 0
                        for var1, var2, corr1, p_value in zip(strong_08_05[k]["var1"], strong_08_05[k]["var2"], 
                                                              strong_08_05[k]["corr_pandas_pearson"], 
                                                              strong_08_05[k]["p-value"]):

                            related[str(j)]={"var1": var1, "var2":var2, "corr":corr1,"p_value": p_value}
                            #print(i)
                            j+=1

                    datos = {
                    "index_1": "RM",
                    "index_2": "RF",
                    "timestamp": pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone("Europe/Madrid")).isoformat(),
                    "time_init":datetime.utcfromtimestamp(time_init).isoformat(),
                    "time_end":datetime.utcfromtimestamp(time_end).isoformat(),
                    "time_window_secs":t_ventana,
                    "correlacion_threshold": corr_threshold,
                    "p_value_threshold": p_value_threshold,
                    "related_events": [related]
                    }
                    if len(related) !=0:
                        #print(len(related))
                        es.index(index='correlacion_estadistica', body=datos)
                        print(f">> Enviado un evento a ES con {len(related)} parejas correladas ")
                    else:
                        print(">> No se envía nada a ES por que hay {} parejas correladas que superen los umbrales".format(len(related)))



            print(f">> Ha habido en total {num_ventanas} ventanas con eventos")
            print(f"Delay de {num_dias_total*factor_2*3600} segundos") #num_dias_total*3600
            time.sleep(num_dias_total*factor_2*3600)#num_dias_total*3600
            
            
            

except KeyboardInterrupt:
    observer.stop()