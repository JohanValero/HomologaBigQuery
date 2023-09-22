from flask import Flask, request
from google.cloud import bigquery
import json
import os

# Construct a BigQuery client object.

app = Flask(__name__)

gQUERYS = {
    "find_columns": """
SELECT  column_name
FROM    seti-spark.conjunto_de_datos_seti.INFORMATION_SCHEMA.COLUMNS
WHERE   table_catalog = @table_catalog_param
    AND table_schema = @table_schema_param
    AND table_name = @table_name_param
ORDER BY ordinal_position ASC"""
}

def getFields(iBigQueryClient, iCatalog : str, iSchema : str, iTable):
    vParams = [
        bigquery.ScalarQueryParameter('table_catalog_param', 'STRING', iCatalog),
        bigquery.ScalarQueryParameter('table_schema_param', 'STRING', iSchema),
        bigquery.ScalarQueryParameter('table_name_param', 'STRING', iTable),
    ]
    vJobConfig = bigquery.QueryJobConfig()
    vJobConfig.query_parameters = vParams
    vQueryJob = iBigQueryClient.query(gQUERYS["find_columns"], job_config = vJobConfig)
    
    vResult = [x[0].upper() for x in vQueryJob.result()]
    return vResult

def execute_query(iBQClient, iSql : str):
    vQueryJob = iBQClient.query(iSql)
    vResult = [x for x in vQueryJob.result()]
    return vResult

def homologa_tabla(iCatalog : str, iSchema, iTable : str, iHomologations : list, iNewTable : str):
    vClient = bigquery.Client()
    vFields = getFields(vClient, iCatalog, iSchema, iTable)
    
    existsFields = all(x["field_source"].upper() in vFields for x in iHomologations)
    if not existsFields:
        raise "Campos invalidos para la tabla."
    # Validación pendiente para no tener campos con más de una homologación.
    vHom = [{**x, "id": i} for i, x in enumerate(iHomologations)]
    vFields = [{"field_name": x, "id": i} for i, x in enumerate(vFields)]
    hom_dict = {item['field_source'].upper(): item for item in vHom}
    
    for field in vFields:
        # Si el field_name está en hom_dict, combinamos los diccionarios
        if field['field_name'].upper() in hom_dict:
            field["hom_id"] = hom_dict[field['field_name'].upper()]["id"]
            field["hom_tag"] = hom_dict[field['field_name'].upper()]["hom_tag"]
            field["sql_field"] = "h" + str(field["hom_id"]) + ".VALOR_DESTINO " + field['field_name']
            field["sql_join"] = "LEFT JOIN seti-spark.conjunto_de_datos_seti.TB_HOMOLOGACION_VALUE h" \
                + str(field["hom_id"]) + " ON t." + field['field_name'] + " = h" + str(field["hom_id"]) \
                + ".VALOR_ORIGEN"
        else:
            field["sql_field"] = "t." + field["field_name"]
            field["sql_join"] = None
    
    vSqlSelect = ", ".join([f["sql_field"] for f in vFields])
    vSqlJoins = "\n".join([f["sql_join"] for f in vFields if f["sql_join"] is not None])
    
    vTruncate = "TRUNCATE TABLE " + iNewTable
    execute_query(vClient, vTruncate)
    print("SQL:", vTruncate)
    
    vSql = "INSERT INTO " + iNewTable + "\n" +\
        "SELECT " + vSqlSelect + "\nFROM " + iCatalog + "." + iSchema + "." + iTable + " t\n" + vSqlJoins
    print("SQL:", vSql)
    execute_query(vClient, vSql)
    

@app.route('/')
def index():
    vStringData = "Hola mundo"
    return vStringData

@app.route('/control_framework/v0.1/homologa')
def homologa():
    # Recibiendo parámetros desde la solicitud GET
    vCatalogSource = request.args.get('vCatalogSource', default="seti-spark")
    vSchemaSource = request.args.get('vSchemaSource', default="conjunto_de_datos_seti")
    vTableSource = request.args.get('vTableSource', default="STG_EMPLEADO")
    vTableDestino = request.args.get('vTableDestino', default="DST_CLIENTE")
    vHomologations_str = request.args.get('vHomologations', default='[{"field_source": "Genero", "hom_tag": "HOM-GEN"}]')
    vHomologations = json.loads(vHomologations_str)
    
    print(vCatalogSource)
    print(vSchemaSource)
    print(vTableSource)
    print(vTableDestino)
    print(vHomologations)
    
    homologa_tabla(
        vCatalogSource,
        vSchemaSource,
        vTableSource,
        vHomologations,
        vTableDestino
    )
    return "OK"

gPORT = os.getenv('PORT', default=None)
app.run(host = '0.0.0.0', port = gPORT, debug = True)