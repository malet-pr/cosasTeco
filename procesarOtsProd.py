import pandas as pd
import os
from pathlib import Path
import random
import uuid
import http.client
import json
from datetime import datetime, date
from urllib.parse import urlparse


base_path = os.getcwd()
code_path = os.path.join(base_path,'codigoPython')
data_path = os.path.join(base_path,'datosUat')

closed = True
tech_list = ['21SAD110','00FPA0001']
origin = ['CSG','SFS','WFX']
dict_closed = {"CSG":"Cerrada","SFS":"Cerrada","WFX":"CE"}
dict_exec = {"CSG":"En Ejecucion","SFS":"En progreso","WFX":"EE"}

#df_actividades = pd.read_excel( os.path.join(data_path,'actividad.xlsx'),sheet_name='Hoja4', parse_dates=['FECHA_ULT_MOD'] )
#df_materiales = pd.read_excel( os.path.join(data_path,'material.xlsx'),sheet_name='Hoja4')
#df_materiales = df_materiales[df_materiales.MATERIAL_TIPO == 'SERI']
#df_ots = pd.read_excel(os.path.join(data_path,'OT.xlsx'),sheet_name='Hoja4', parse_dates=['FECHA_ULT_MOD_OT','FECHA_CREACION_OT'] )

df_actividades = pd.read_excel( os.path.join(data_path,'ots.xlsx'),sheet_name='Act', parse_dates=['FECHA_ULT_MOD'] )
df_materiales = pd.read_excel( os.path.join(data_path,'ots.xlsx'),sheet_name='Mat')
df_materiales = df_materiales[df_materiales.MATERIAL_TIPO == 'SERI']
df_ots = pd.read_excel(os.path.join(data_path,'ots.xlsx'),sheet_name='OT', parse_dates=['FECHA_ULT_MOD_OT','FECHA_CREACION_OT'] )

# MAP NRO_OT, ID_OT
id_nro_ot = df_ots[['ID_ORDEN_TRABAJO', 'NRO_OT']].drop_duplicates()
mapOT = dict()
for idx,row in id_nro_ot.iterrows():
    mapOT[str(row.ID_ORDEN_TRABAJO)] = row.NRO_OT

# MAP ID_MATERIAL, COD_MATERIAL
id_mat_cod = df_materiales[['ID_MATERIAL_DESCARGA_OT','MATERIAL_CODIGO','ID_TIPO_DESCARGA']]
mapM_add = dict()
mapM_remove = dict()
for idx,row in id_mat_cod.iterrows():
    if row.ID_TIPO_DESCARGA == 2:
        mapM_add[str(row.ID_MATERIAL_DESCARGA_OT)] = str(row.MATERIAL_CODIGO)
    if row.ID_TIPO_DESCARGA == 1:
        mapM_remove[str(row.ID_MATERIAL_DESCARGA_OT)] = str(row.MATERIAL_CODIGO)
#print('\n\n############################################################################\n\n')
#print(str(mapM_add))
#print(str(mapM_remove))
#print('\n\n############################################################################\n\n')

# PROCESS MATERIALES
df_mat = df_materiales[['ID_ORDEN_TRABAJO','MATERIAL_CODIGO','MATERIAL_NOMBRE','MATERIAL_TIPO','CANTIDAD','ID_TIPO_DESCARGA']]
df_mat.rename(columns={
    'ID_ORDEN_TRABAJO':'idOt',
    'MATERIAL_CODIGO':'codigo',
    'MATERIAL_NOMBRE':'nombre',
    'MATERIAL_TIPO':'tipoMaterial',
    'CANTIDAD':'umbralSelecionado',
    'ID_TIPO_DESCARGA':'tipoDescarga'
    }, inplace=True)
df_mat['idOt'] = df_mat['idOt'].astype(str).str.split('.').str[0]
df_mat['codigo'] = df_mat['codigo'].astype(str).str.split('.').str[0]
df_mat['umbralSelecionado'] = df_mat['umbralSelecionado'].astype(str).str.split('.').str[0]
ot_ids = df_mat.idOt.unique()
grouped = df_mat.groupby('idOt')
materiales = dict()
df_mat.replace({'nan': None, pd.NA: None, pd.NaT: None}, inplace=True)
for id in ot_ids:
    df_temp = grouped.get_group(id)
    df_temp = df_temp.drop(columns=['idOt'])
    json_list = []
    for idx, row in df_temp.iterrows():
        data = row.to_dict()
        if row.tipoDescarga == 1:
            data['tipoDescarga'] = 'Recupero'
        elif row.tipoDescarga == 2:
            data['tipoDescarga'] = 'Consumo'
        else:
            data['tipoDescarga'] = None
        json_list.append(data)
    materiales[str(id)] = json_list
#print(materiales)    

# PROCESS ACTIVIDADES
df_act = df_actividades[['ID_ORDEN_TRABAJO','ACTIVIDAD_CODIGO','ACTIVIDAD_NOMBRE','CANTIDAD','ID_OT_ACTIVIDAD_PADRE','SEQ_ACTIVIDAD','SEQ_SUBACTIVIDAD','LEGAJO_NOLDAP','FECHA_ULT_MOD','ID_MATERIAL_DESCARGA_OT_ADD','ID_MATERIAL_DESCARGA_OT_REMOVE']]
df_act.rename(columns={
    'ID_ORDEN_TRABAJO':'idOt',
    'ACTIVIDAD_CODIGO':'idSubactividad',
    'ACTIVIDAD_NOMBRE':'nombreActividad',
    'CANTIDAD':'cantidad',
    'ID_OT_ACTIVIDAD_PADRE':'idActividadPadre',
    'SEQ_ACTIVIDAD':'seqNumActividad',
    'SEQ_SUBACTIVIDAD':'seqNumSubactividad',
    'LEGAJO_NOLDAP':'legajoTecnico',
    'FECHA_ULT_MOD':'fechaUltimaModificacion',
    'ID_MATERIAL_DESCARGA_OT_ADD':'serialConsumo',
    'ID_MATERIAL_DESCARGA_OT_REMOVE':'serialRecupero'
    }, inplace=True)
df_act['idOt'] = df_act['idOt'].astype(str).str.split('.').str[0]
df_act['idSubactividad'] = df_act['idSubactividad'].astype(str).str.split('.').str[0]
df_act['idActividadPadre'] = df_act['idActividadPadre'].astype(str).str.split('.').str[0]
df_act['serialConsumo'] = df_act['serialConsumo'].astype(str).str.split('.').str[0]
df_act['serialRecupero'] = df_act['serialRecupero'].astype(str).str.split('.').str[0]
df_act['seqNumActividad'] = df_act['seqNumActividad'].astype(str).str.split('.').str[0]
df_act['seqNumSubactividad'] = df_act['seqNumSubactividad'].astype(str).str.split('.').str[0]
df_act['fechaUltimaModificacion'] = (pd.to_datetime(df_act['fechaUltimaModificacion'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                  .fillna(pd.to_datetime(df_act['fechaUltimaModificacion'], errors='coerce')))
ot_ids = df_act.idOt.unique()
grouped = df_act.groupby('idOt')
actividades = dict()
df_act.replace({'nan': None, pd.NA: None, pd.NaT: None}, inplace=True)
for id in ot_ids:
    df_temp = grouped.get_group(id)
    json_list = []
    for idx, row in df_temp.iterrows():
        data = row.to_dict()
        data['nroOt'] = mapOT[data['idOt']]
        if data['serialConsumo'] in mapM_add:
            data['serialConsumo'] = mapM_add[data['serialConsumo']]
        else:
            data['serialConsumo'] = ''
        if data['serialRecupero'] in mapM_remove:
            data['serialRecupero'] = mapM_remove[data['serialRecupero']]
        else:
            data['serialRecupero'] = ''
        del data['idOt']
        data['actividadesHijas'] = []
        json_list.append(data)
    actividades[str(id)] = json_list
#print(actividades)

# PROCESS MAIN OT DATA
df_ots = df_ots.drop(columns=['ID_OPERADOR_TECNICO_1_OT','ID_LOGI_ESTRUC','ID_NOTIFICA_SISTEMA','FECHA_CREACION_GM','FECHA_ULT_MOD_GM','GESTIONADA','ID_OPERADOR_ULT_MOD_GM','ID_TICKET'])
df_ots.rename(columns={
    'ID_ORDEN_TRABAJO':'idOrdenTrabajo',
    'NRO_OT':'identificador',
    'TAREA_CODIGO_OT':'codigoTarea',
    'TAREA_DESC_OT':'descripTarea',
    'FECHA_ULT_MOD_OT':'fechaUltModif',
    'USUARIO_ULT_MOD_OT':'usuarioCierre',
    'LEGAJO_NOLDAP':'tecnicoAsociado',
    'DOMI_CALLE':'calle',
    'DOMI_ALTURA':'altura',
    'DOMI_PISO':'piso',
    'DOMI_PUERTA':'puerta',
    'DOMI_CODIGO_POSTAL':'codPostal',
    'DOMI_DESC_LOCALIDAD':'descLocalidad',
    'ID_ESTADO_OT':'codigoEstadoOt',
    'SISTEMA_ORIGEN':'sistemaOrigen',
    'DOMI_DIRECCION':'direccion',
    'DOMI_PAIS':'pais',
    'DOMI_LONGITUD':'longitud',
    'DOMI_LATITUD':'latitud',
    'SISTEMA_ENTRADA':'entrySystemId',
    'SECTOR':'sector',
    'AREA':'routeCriteria',
    'CONTRATISTA':'contratista',
    'TIPO_EMPLEADO':'tipoEmpleado',
    'FECHA_CREACION_OT':'fechaCreacionWfx',
    'CLASE_OT':'workOrderClass',
    'ID_CLIENTE':'customerId',
    'CONVENIO':'convenio',
    'DOMI_PROVINCIA':'provincia',
    'DOMI_PARTIDO':'partido',
    'DOMI_CODIGO_DIRECCION':'domiCodigoDireccion',
    'RESOLUCION_CODIGO':'resolutionCode',
    'RESOLUCION_DESC':'resolutionCodeDesc'
    }, inplace=True)
df_ots['usuarioCierre'] = df_ots['usuarioCierre'].astype(str).str.split('.').str[0]
df_ots['tecnicoAsociado'] = df_ots['tecnicoAsociado'].astype(str).str.split('.').str[0]
df_ots['idOrdenTrabajo'] = df_ots['idOrdenTrabajo'].astype(str).str.split('.').str[0]
df_ots['identificador'] = df_ots['identificador'].astype(str).str.split('.').str[0]
df_ots['codigoTarea'] = df_ots['codigoTarea'].astype(str).str.split('.').str[0]
df_ots['piso'] = df_ots['piso'].astype(str).str.split('.').str[0]
df_ots['puerta'] = df_ots['puerta'].astype(str).str.split('.').str[0]
df_ots['codPostal'] = df_ots['codPostal'].astype(str).str.split('.').str[0]
df_ots['longitud'] = df_ots['longitud'].astype(str)
df_ots['latitud'] = df_ots['latitud'].astype(str)
df_ots['customerId'] = df_ots['customerId'].astype(str).str.split('.').str[0]
df_ots['contratista'] = df_ots['contratista'].astype(str).str.split('.').str[0]
df_ots['sector'] = df_ots['sector'].astype(str).str.split('.').str[0]
df_ots['routeCriteria'] = df_ots['routeCriteria'].astype(str).str.split('.').str[0]
df_ots['convenio'] = df_ots['convenio'].astype(str).str.split('.').str[0]
df_ots['resolutionCode'] = df_ots['resolutionCode'].astype(str).str.split('.').str[0]
df_ots['resolutionCodeDesc'] = df_ots['resolutionCodeDesc'].astype(str).str.split('.').str[0]
df_ots['fechaUltModif'] = (pd.to_datetime(df_ots['fechaUltModif'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                  .fillna(pd.to_datetime(df_ots['fechaUltModif'], errors='coerce')))
df_ots['fechaCreacionWfx'] = (pd.to_datetime(df_ots['fechaCreacionWfx'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                  .fillna(pd.to_datetime(df_ots['fechaCreacionWfx'], errors='coerce')))
json_data = []
df_ots.replace({'nan': None, pd.NA: None, pd.NaT: None}, inplace=True)
for idx, row in df_ots.iterrows():
    tech = random.choice(tech_list)
    main_object = row.to_dict()
    main_object['identificador'] = main_object['identificador'] + '_test_3'
    main_object['tecnicoAsociado'] = tech
    main_object['usuarioCierre'] = tech
    if main_object['idOrdenTrabajo'] in actividades:
        for act in actividades[main_object['idOrdenTrabajo']]:
            act['legajoTecnico'] = tech
        main_object['actividades'] = actividades[main_object['idOrdenTrabajo']]
    else:
        main_object['actividades'] = []
    if main_object['idOrdenTrabajo'] in materiales:
        main_object['materiales'] = materiales[main_object['idOrdenTrabajo']]
    else:
        main_object['materiales'] = []
    main_object['idOrdenTrabajo'] = None
    main_object['sistemaOrigen'] = random.choice(origin)
    if closed:
        main_object['codigoEstadoOt'] = dict_closed.get(main_object['sistemaOrigen'])
    else:
        main_object['codigoEstadoOt'] = dict_exec.get(main_object['sistemaOrigen'])
    json_data.append(main_object)

class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime,date)):
                return obj.isoformat()
    
#json_object = json.dumps(json_data, default=convert_timestamp, indent=4) 
#print(json_object)

final_payload = {
    "idProceso": str(uuid.uuid4()),
    "listaAdapter": json_data
}

serialized_data = json.dumps(final_payload, cls=DateTimeEncoder, indent=4)
#print(serialized_data)

print('\n\n############################################################################\n')
print('Enviando ots')
print('\n############################################################################\n\n')


#url = urlparse("http://192.168.244.208:8081/gm/ordenTrabajo/saveOrUpdateGot") 
url = urlparse("http://localhost:8081/gm/ordenTrabajo/saveOrUpdateGot") 
connection = http.client.HTTPConnection(url.netloc)
headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Accept': 'application/json',
}
connection.request("POST", url.path, body=serialized_data, headers=headers)
response = connection.getresponse()
response_data = response.read()
print("Status Code:", response.status)
print("Response Body:", response_data.decode('utf-8'))
connection.close()


