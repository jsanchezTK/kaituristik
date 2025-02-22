import logging
import json
import azure.functions as func
import os
from openai import OpenAI
import tiktoken
# import datetime
# from datetime import timedelta
import pyodbc as odbc
import traceback
# import pytz

# Función principal

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_SECRET = os.getenv("SQL_SECRET")
client = OpenAI()

sql_driver = "ODBC Driver 17 for SQL Server"
connection_string = f"driver={sql_driver}; server={SQL_SERVER}; database={SQL_DATABASE}; UID={SQL_USER}; PWD={SQL_SECRET}"
gpt_model = "gpt-4o-mini"
encoding = tiktoken.encoding_for_model(gpt_model)
total_tokens = 8000


def get_row(key, data):
    c = 1
    d = {}
    for i in data:
        if i[0] == key:
            for j in i.cursor_description[1:]:
                d[j[0]] = i[c]
                c += 1
    return d


def check_tiempo_atencion(userid: str) -> str:
    """Hace una consulta con el userid para obtener el tiempo que ha ocurrido desde el primer mensaje del usuario en las últimas 12 horas."""\
    """Si el tiempo es mayor o igual a 5 minutos, entrega True, en caso contrario, entrega False"""
    global connection_string
    query = f"SELECT cast(DATEDIFF(s,MIN([timestamp]),GETDATE()) as float)/60 as minutos_atencion\n"\
            f"FROM [kai].[ChatGPT_Mensajes]\n"\
            f"where userid = '{userid}' and [timestamp] >= dateadd(hh,-12, GETDATE())"
    conx = odbc.connect(connection_string)
    cursor = conx.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    if data[0][0] is None:
        derivar = "False"
    else:
        min_atencion = float(data[0][0])
        if min_atencion >= 5:
            derivar = "True"
        else:
            derivar = "False"
    return derivar


def recuperar_mensajes(conversacion: list) -> list:
    tokens_prompt = len(encoding.encode(content))
    tokens_reservados = 500
    tokens_disponibles = 8000 - tokens_prompt - tokens_reservados
    mensajes_recuperados = []
    conversacion.sort(key=lambda x: x[3], reverse=True)
    limite = len(conversacion)
    index = 0
    tokens_mensaje_actual = 0
    while tokens_disponibles >= tokens_mensaje_actual and index < limite:
        msg_row = conversacion[index]
        mensaje_actual = conversacion[index][2]
        tokens_mensaje_actual = len(encoding.encode(mensaje_actual))
        mensajes_recuperados.append([msg_row[2], msg_row[3], msg_row[0], tokens_mensaje_actual])
        index += 1
        tokens_disponibles -= tokens_mensaje_actual
    mensajes_recuperados.sort(key=lambda x: x[2])
    return mensajes_recuperados


def generar_respuesta(msg: str, userid: str, lang: str, canal: str, intencion: str) -> str:
    global content
    if msg == "#Reiniciar":
        conx = odbc.connect(connection_string)
        cursor = conx.cursor()
        query = "delete [kai].[ChatGPT_Mensajes] where userid = '"+userid+"'"
        cursor.execute(query)
        conx.commit()
        conx.close()
        reply = "*Se ha borrado el contexto anterior de la conversación. De aquí en adelante Kai responderá como si fuera el primer mensaje*"
    elif "__image__" in msg:
        # Iniciar conexión y cursor
        conx = odbc.connect(connection_string)
        cursor = conx.cursor()
        # Insertar Fila
        query = "INSERT INTO [kai].[ChatGPT_Mensajes]\n"+ \
                "VALUES ('"+userid+"', 'user', '"+msg+"', GETDATE(),'"+canal+"', '"+lang+"', '"+intencion+"', 0, 0, 0)"
        cursor.execute(query)
        conx.commit()
        # Recuperar Conversación Completa
        query = "SELECT * FROM [kai].[ChatGPT_Mensajes] where userid = '"+userid+"'"
        cursor.execute(query)
        conversacion = cursor.fetchall()
        # Agregar Prompt y contar tokens
        messages = [{"role": "system", "content": content}]
        tokens_content = len(encoding.encode(content))
        # Recuperar Mensajes y contar tokens
        recuperados = recuperar_mensajes(conversacion)
        tokens_conversacion = 0
        # Agregar mensajes al bot
        for i in recuperados[:-1]:
            tokens_conversacion += i[3]
            messages.append({"role": i[0], "content": i[1]})
        txt = recuperados[-1][1]
        messages.append({"role": "user", "content": txt})
        # Generar respuesta y contar tokens
        reply = "Como asistente virtual, no puedo ver imágenes. Puedes explicarme de que se trata, o escribir \"Agente\" para ser derivado a un ejecutivo"
        tokens_reply = len(encoding.encode(reply))
        # Insertar Respuesta en base de datos
        query = "INSERT INTO [kai].[ChatGPT_Mensajes]\n"+ \
                "VALUES ('"+userid+"', 'user', '"+msg+"', GETDATE(),'"+canal+"', '"+lang+"', '"+intencion+"', 0, 0, 0)"
        cursor.execute(query)
        conx.commit()
        # Cerrar conexión SQL
        conx.close()
        # Agregar respuesta al bot
        messages.append({"role": "assistant", "content": reply})
        total_tokens = tokens_content + tokens_conversacion + tokens_reply
    elif msg == "?"*len(msg) or msg in ["Share"]:
        reply = ""
    else:
        # Iniciar conexión y cursor
        conx = odbc.connect(connection_string)
        cursor = conx.cursor()
        # Insertar Fila
        query = "INSERT INTO [kai].[ChatGPT_Mensajes]\n"+ \
                "VALUES ('"+userid+"', 'user', '"+msg+"', GETDATE(),'"+canal+"', '"+lang+"', '"+intencion+"', 0, 0, 0)"
        cursor.execute(query)
        conx.commit()
        # Recuperar Conversación Completa
        query = "SELECT * FROM [kai].[ChatGPT_Mensajes] where userid = '"+userid+"'"
        cursor.execute(query)
        conversacion = cursor.fetchall()
        # Agregar Prompt y contar tokens
        messages = [{"role": "system", "content": content}]
        tokens_content = len(encoding.encode(content))
        # Recuperar Mensajes y contar tokens
        recuperados = recuperar_mensajes(conversacion)
        tokens_conversacion = 0
        # Agregar mensajes al bot
        for i in recuperados[:-1]:
            tokens_conversacion += i[3]
            messages.append({"role": i[0], "content": i[1]})
        txt = msg
        if "hola" not in txt.lower() and "gracias" not in txt.lower() and len(txt)>5:
            txt = f"Reply kindly and in {lang} to the message in quotes: \"{txt}\""
        messages.append({"role": "user", "content": txt})
        # Generar respuesta y contar tokens
        completion = client.chat.completions.create(
            model=gpt_model,
            messages=messages,
            temperature=0.1)
        reply = completion.choices[0].message.content
        prompt_tokens = completion.usage.prompt_tokens
        completion_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        # Insertar Respuesta en base de datos
        query = "INSERT INTO [kai].[ChatGPT_Mensajes]\n"+ \
                "VALUES ('"+userid+"', 'assistant', '"+reply+"', GETDATE(),'"+canal+"', '"+lang+"', '"+intencion+"', "+str(prompt_tokens)+", "+str(completion_tokens)+", "+str(total_tokens)+")"
        cursor.execute(query)
        conx.commit()
        # Cerrar conexión SQL
        conx.close()
        # Agregar respuesta al bot
        messages.append({"role": "assistant", "content": reply})
        # reply = userid+": "+msg
    if reply:
        if reply[0] == "\"" and reply[-1] == "\"":
            reply = reply[1:-1]
    return reply


def guardar_error(userid: str, msg: str, canal: str, lang: str, intencion: str, tipo: str, tb: str) -> None:
    # Iniciar conexión y cursor
    conx = odbc.connect(connection_string)
    cursor = conx.cursor()
    # Insertar Fila
    query = "INSERT INTO [kai].[ChatGPT_ErroresApi]\n"+ \
            "VALUES (GETDATE(), '"+userid+"', '"+msg+"', '"+canal+"', '"+lang+"', '"+intencion+"', '"+tipo+"', '"+tb+"')"
    cursor.execute(query)
    conx.commit()
    conx.close()


txt_summer_camp = "Turistik Summer Camp ofrece una semana de aventuras al aire libre en el cerro San Cristóbal de Santiago. El campamento se lleva a cabo de lunes a viernes, con punto de reunión en la Estación Oasis del Teleférico de Santiago (Av. El Cero 750, Providencia), de 9:00 a.m. a 5:00 p.m., durante los meses de Enero y Febrero de 2023. El programa se centra en la naturaleza, con actividades al aire libre, aprendizaje ecológico y trabajo en equipo.\n"\
"Cada día presenta un tema único:\n"\
"- Lunes: Bienvenida con actividades de integración, deportivas y un taller de educación medioambiental.\n"\
"- Martes: \"Salvaje\", dedicado a la fauna, incluye viajes panorámicos en Teleférico y Funicular, más un recorrido por el zoo.\n"\
"- Miércoles: Deportes y exploración con actividades en el Parque Aventura y una caminata hasta el histórico Observatorio Manuel Foster.\n"\
"- Jueves: \"Refrescante\", combina senderismo, diversión en la piscina y helados artesanales.\n"\
"- Viernes: Concluye con \"Excursión y ecología\", enfocado en descubrimiento y educación ambiental en el Bosque Santiago.\n"\
"La seguridad es primordial, con guías expertos, agua, snacks saludables y protector solar durante todo el campamento. Incluido está el acceso a actividades guiadas, entradas a atracciones, kit de bienvenida, transporte sustentable hasta varios destinos, y acceso a la Pérgola Saludable que ofrece agua, fruta y protector solar.\n"\
"El precio de preventa es de $97.500 hasta el 30 de noviembre de 2023 (sujeto a disponibilidad de cupos), posteriormente el precio normal es de $130.000. Las opciones de pago incluyen transferencia electrónica, Webpay, Mercado Público o Senegocia. Para reservaciones o más detalles, se puede contactar a Turistik en summercamp@turistik.com o al +56 931 998307.\n"\
"Para más detalles, términos y condiciones, visita http://www.turistik.com/summercamp\n"

reglas_kai = "- When asked about our services you must assume people are asking about tours only. Don't mention other services unless you are explicitly asked. \n"\
             "- When asked about prices and tours in general, you have to mention each of the different tours categories that we offer, in order to guide the user and narrow the options.\n"\
             "- If the user is interested in one category in particular asks about tour options, mention only the top 2 options of higher price first, and mention the others only if asked about more options.\n"\
             "- Introduce yourself on your first interaction.\n"\
             "- Be kind and helpful.\n"\
             "- Do not recommend other tour operators.\n"\
             "- Services provided by Turistik are detailed in the \"Turistik Tours\" table.\n"\
             "- Users can type \"Agent\" to be assisted by a real person. "

content = "You are a kind assistant named Kai, assigned to reply the company's chat (Turistik). " \
              "Next is a list of rules you have to follow, they are in order of priority and delimited with XML tags:" \
          "<rules>" \
          + reglas_kai + \
          "</rules>\n"


def servicios_txt():
    conx = odbc.connect(connection_string)
    cursor = conx.cursor()
    query = "SELECT * FROM [dbo].[V_ServiciosTurismoKai]"
    cursor.execute(query)
    data = cursor.fetchall()
    tabla = ""
    for i in data:
        fila = "("
        for j in i:
            fila += j + "; "
        fila = fila[:-2] + ");\n"
        tabla += fila
    tabla = tabla[:-2] + "]"
    enunciado = "There is a table that has all the information about our tours and services. This is the \"Turistik Tours\" table." \
                "Each row of the table is represented as a python tuple, but values are separated by semicolon instead of comma. " \
                "The entire table has the structure of a python list containing the tuples that represent rows. " \
                "Each row ends with a comma and a line break after the tuple (\",\\n\")." \
                "All the data in the table is in spanish. " \
                "This, this table has 11 columns which are from first to last:" \
    "[service], [category], [days], [schedule], [modality], [adult price], [child price], [duration], [languages], [url], [summary]." \
                "This is the table data: "+tabla
    conx.close()
    return enunciado


def redactar_contenido(unidad: str, idioma="english") -> str:
    # Contenido general
    global content
    global adicional
    redacted_content = content
    conx = odbc.connect(connection_string)
    cursor = conx.cursor()
    query_template = "SELECT [texto],[txt_index],[idioma] FROM [kai].[RAW_Embeddings] where unidad_negocio = '{0}' and idioma = '{1}'"
    query = query_template.format(unidad, idioma)
    cursor.execute(query)
    data = cursor.fetchall()
    data.sort(key=lambda x: x[1])
    redacted_content += servicios_txt()+".\n"
    for i in data:
        redacted_content += i[0] + "\n"
    conx.close()
    redacted_content += adicional
    return redacted_content

adicional = ""
# adicional += "El Tour de Halloween en Funicular (también llamado Experiencia Nocturna o Tour de Halloween) es un evento especial que se llevará a cabo el 31/10/2024 y 01/11/2024, sin otras fechas disponibles. Los cupos para el Tour de Halloween en Funicular se encuentran agotados para los días 31/10/2024 y 01/11/2024."
# adicional += "A continuación está la información del Summer Camp delimitado por tags XML: <\summercamp>"+txt_summer_camp+"<summercamp>"
adicional += """El servicio Hop On Hop Off Valdivia tiene lugar entre el 2 de enero de 2025, hasta el 2 de marzo de 2025. Este servicio consta de un circuito turístico en bus panorámico por la ciudad de Valdivia, con 6 paradas por distintos lugares y atractivos turísticos.
El circuito estará disponible desde las 11:00 hasta las 20:30 hrs y realiza el siguiente recorrido:
1. Cerveceria Kunstmann
2. Isla teja
3. Feria Fluvial
4. Plaza de la República
5. Costanera
6. Plaza Obelisco
Los visitantes pueden acceder a un 100% de descuento utilizando los siguientes cupones:
- kunstmann2025
- CopecPay2025
- Valdivia2025
Pronto se publicará un enlace en la web para reservar tickets.
"""
content = redactar_contenido("turismo")


def clasificar(msg, idioma):
    instructions = "You are assigned to analyze messages from our business chats. " \
               "Our company is called Turistik and we offer a variety of tours in Santiago, Chile. " \
               "The messages to analyze are in {0}. When analyzing these messages, you have to classify them in one of the following categories, each of them delimited by XML Tags: " \
               "\n<category>\"Information\": Messages asking for directions, schedules, prices, payment methods, and any other questions regarding our services.</category>" \
               "\n<category>\"Agent required\": People asking to be assisted by a real person, reschedule requests, and other problems thar require customer service.</category>" \
               "\n<category>\"Reservation\": Manifesting interest in making a reservation or get tickets.</category>" \
               "\n<category>\"Other\": Greetings, question marks or anything that can't be classified in the previous categories, or any message that you cannot process (images, audio, links, etc.).</category>" \
               "\nYou must reply only with the category to which the message belongs without the XML tags and quotes and nothing else."
    messages = [{"role": "system",
                 "content": instructions.format(idioma)
                 },
                {"role": "user",
                 "content": msg}
                ]
    # response = openai.ChatCompletion.create(
    #     model=modelo,
    #     messages=messages,
    #     max_tokens=300)
    # reply = response.choices[0]["message"]["content"]
    # prompt_tokens = response.usage.prompt_tokens
    # completion_tokens = response.usage.completion_tokens
    # total_tokens = response.usage.total_tokens
    completion = client.chat.completions.create(
        model=gpt_model,
        messages=messages,
        temperature=0.1,
        max_tokens=300)
    reply = completion.choices[0].message.content
    prompt_tokens = completion.usage.prompt_tokens
    completion_tokens = completion.usage.completion_tokens
    total_tokens = completion.usage.total_tokens

    dic = {"reply": reply, "pt": prompt_tokens, "ct": completion_tokens, "tt": total_tokens}
    return dic


def main(req: func.HttpRequest) -> func.HttpResponse:
    # cambio input req
    # print(redactar_contenido("turismo"))
    j = req.get_json()
    msg = j["text"]
    userid = j["userid"]
    canal = j["canal"]
    intencion = j["derivar"]
    lang = j["idioma"]
    logging.info("ID Usuario: "+userid)
    logging.info("Canal: "+canal)
    logging.info("Intención: "+intencion)
    logging.info("Idioma: "+lang)
    logging.info("Mensaje: "+msg)
    try:
        cls = clasificar(msg, lang)
        logging.info("Clasificación: "+cls["reply"])
        derivar = check_tiempo_atencion(userid)
        if cls["reply"] in ["Agent required", "Reservation"] and derivar == "True":
            msj = cls["reply"]
            pt = str(cls["pt"])
            ct = str(cls["ct"])
            tt = str(cls["tt"])
            conx = odbc.connect(connection_string)
            cursor = conx.cursor()
            # Insertar Fila
            query = "INSERT INTO [kai].[ChatGPT_Mensajes]\n"+ \
                    "VALUES ('{0}', '{1}', '{2}', GETDATE(), '{3}', '{4}', '{5}', {6}, {7}, {8})"
            cursor.execute(query.format(userid, "user", msg, canal, lang, intencion, "0", "0", "0"))
            conx.commit()
            cursor.execute(query.format(userid, "assistant", msj, canal, lang, intencion, pt, ct, tt))
            conx.commit()
            conx.close()
        else:
            msj = generar_respuesta(msg, userid, lang, canal, intencion)
        reply = {"msj": msj, "time_check": derivar}
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        tb = traceback.format_exc()
        tipo_error = type(e).__name__
        try:
            guardar_error(userid, msg, canal, lang, intencion, tipo_error, tb)
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
        finally:
            cod_status = 500
            reply = "Se ha generado una excepción del tipo "+tipo_error
    else:
        cod_status = 200
        reply = {"msj": msj, "time_check": derivar}
    finally:
        logging.info('Función ejecutada con código '+str(cod_status))
        return func.HttpResponse(
                json.dumps(reply),
                status_code=cod_status,
                mimetype="application/json"
            )