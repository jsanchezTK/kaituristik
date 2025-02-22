import logging
import json
import azure.functions as func
import os
from openai import OpenAI
import tiktoken
import datetime
from datetime import timedelta
import pyodbc as odbc
import traceback
import pytz

OPENAI_APIKEY = os.getenv("OPENAI_APIKEY")
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_SECRET = os.getenv("SQL_SECRET")
client = OpenAI()

sql_driver = "ODBC Driver 17 for SQL Server"
connection_string = f"driver={sql_driver}; server={SQL_SERVER}; database={SQL_DATABASE}; UID={SQL_USER}; PWD={SQL_SECRET}"
gpt_model = "gpt-4o-mini"
encoding = tiktoken.encoding_for_model(gpt_model)


txt_summer_camp = """Para información sobre campamentos de verano (summercamp), enviar consultas a summercamp@turistik.com o gshinya@turistik.com"""

def importar_calendario():
    global connection_string
    conx = odbc.connect(connection_string)
    cursor = conx.cursor()
    cursor.execute(("{CALL Get_CalendarioCerro}"))
    data = cursor.fetchall()
    conx.close()
    return data


def get_row(key, data):
    c = 1
    d = {}
    for i in data:
        if i[0] == key:
            for j in i.cursor_description[1:]:
                d[j[0]] = i[c]
                c += 1
    return d


def redactar_apertura(rol):
    d = {"Teleférico de Santiago": 4, "Funicular de Santiago": 5, "Parque Aventura": 6, "Kids": 7}
    col = d[rol]
    calendario = importar_calendario()
    txt = f"Esta es la información de apertura de esta semana y la próxima en {rol}:\n"
    for i in calendario:
        txt += f"{i[10]} {i[col]}, \n"
    return txt[:-2]+". "


def apertura_ahora(rol):
    tz = pytz.timezone('Chile/Continental')
    cierre = datetime.timedelta(hours=18, minutes=45)
    hoy = datetime.datetime.now(tz=tz).date()
    cierre_hoy = datetime.datetime(hoy.year, hoy.month, hoy.day, tzinfo=tz) + cierre
    d = {"Teleférico de Santiago": "AperturaTeleferico", "Funicular de Santiago": "AperturaFuni", "Parque Aventura": "AperturaParque", "Kids": "AperturaKids"}
    col = d[rol]
    estado = get_row(int(datetime.datetime.now(tz=tz).date().strftime("%Y%m%d")), importar_calendario())[col]
    if estado == "Abierto":
        if datetime.datetime.now(tz=tz) < cierre_hoy:
            estado = "Abierto"
        else:
            estado = "Cerrado"
    return f"En este momento {rol} se encuentra {estado}"


mapa = "http://mapasancristobal.turistik.com/"

atracciones_parque = {
    "Jardín Japonés": "Su entrada es liberada y está abierto de 9:00 a 18:00 hrs. La mejor forma de llegar es por la entrada al Parque Metropolitano por calle Pedro de Valdivia Norte, caminar hacia estación Oasis y tomar el sendero ubicado a un costado del Restaurant Divertimento Chileno. Envía el link del mapa: "+mapa,
    "Zoológico Metropolitano:": "Para reservar y asegurar su entrada: https://reservas.parquemet.cl/zoologico. Para visitas educativas contacta a la Unidad Educativa al correo electrónico: zooeducacion@parquemet.cl. Ya no hay restricciones por COVID en el zoológico. Envía el link del mapa: "+mapa+". No hay una forma directa de llegar desde la entrada de Pedro de Valdivia, ni desde el Teleférico, la única opción sería llegar a Pío Nono y tomar el Funicular hasta la Estación Zoológico."
}
lista_atracciones = ""
descripcion_atracciones = ""

for i in atracciones_parque.keys():
    lista_atracciones += i+", "
    descripcion_atracciones += i+": "+atracciones_parque[i]+". "
lista_atracciones = lista_atracciones[:-2]+". "

# Reglas
reglas_kai = "1. Siempre saludar y presentarte en la primera interacción"\
             "2. Responder con amabilidad"\
             "3. Dar respuestas breves y en el mismo idioma que te hablaron"\
             "4. Sólo dar detalles cuando sean solicitados"\
             "5. No recomendar otros operadores turísticos"\
             "6. Debes considerar estrictamente la información presentada acá."\
             "7. No existen otros servicios fuera de los mencionados acá."\
             "8. Si no tienes suficiente información, debes sugerir al usuario que escriba la palabra “Agente” para ser atendido por una persona."

reglas_especificas = "9. Si te preguntan por tours, no respondas y sugiere ingresar a www.turistik.com para obtener más información."

# Datos Generales
comentarios = "Para consultas por reclamos y problemas, el contacto es experiencia@turistik.com. Para ventas corporativas y grupos grandes, contactar a Paula Ibarra: pibarra@turistik.com"
contacto = "Para consultas profesionales, colaboraciones, proveedores de productos y/o servicios, pueden contactarse a través de nuestro formulario de contacto en https://turistik.com/contacto/."
comer = "En todas las estaciones del teleférico hay locales de las cafeterías Delicatto, donde hay helados artesanales, café de grano, bebidas y más. También se puede visitar el Café Tudor, ubicado cerca de la estación cumbre de Funicular, para llegar se debe tomar el Funicular desde Estación Pío Nono y bajar en Cumbre, o para llegar desde Teleférico se puede tomar en Estación Oasis y bajar en Cumbre. Café Tudor ofrece café de grano, bebidas, pastelería, entre otros. "
atracciones = "Dentro del Parque Metropolitano se encuentran distintas atracciones, tales como: "+lista_atracciones+""\
                "Descripción de las atracciones: "+descripcion_atracciones

# Parque Aventura
rol = "Parque Aventura"
url = "https://aventuraonline.cl/"
precios = "Ticket 2 Aventuras: $8.900 (consiste de 2 jugadas), Ticket 3 Aventuras: $11.900 (consiste de 3 jugadas) y Parque Full Aventura: $15.900 (consiste de 3 jugadas + Ticket Vive el Parque). Ticket Aventura Kids desde $6.000, según tiempo de juego. El ticket Vive el Parque es un ticket ilimitado por un día para Teleférico, Funicular y Buses Ecológicos. Los tickets admiten un acompañante dentro de Parque Aventura que no puede hacer uso de los juegos, pero en el caso del Ticket Parque Full Aventura, el acompañante debe comprar su propio ticket para Teleférico, Funicular y Buses Ecológicos. Para más información, visitar el enlace: "+url+". "
descripcion_general = "Parque Aventura es un parque de actividades outdoor en la ciudad de Santiago, dentro de la comuna de Providencia en el cerro San Cristóbal, en el cual podrás encontrar múltiples actividades dependiendo de la estatura del jugador, dentro de ellos puentes colgantes, tirolesas, circuitos de cuerdas, muro de escalada, salto al vacío y un gran Canopy de 200 mts. "
descripcion_servicio = "Parque Aventura cuenta con dos principales estructuras: Mega Tótem y Gran Canopy. El Mega Tótem cuenta con las actividades para aventureros más grandes, donde se puede usar el muro de escalada, Salto al Vacío y Puente Colgante. Por otro lado, en el Gran Canopy, también para aventureros más grandes, podrás lanzarte por una tirolesa de 200 m."\
                        "Para los aventureros más pequeños tenemos Aventura Kids, el cual cuenta con 3 pisos y más de 20 obstáculos, entre ellos: muro de escalada, puente colgante, tobogán, piscina de pelotas y muchos más."\
                        "Dependiendo de su estatura, los visitantes se clasificarán en Pumas (desde 1.30 metros hasta 1.54 metros de estatura) y Cóndores (desde 1.55 hasta 1.95 metros de estatura). Visitantes de categoría Puma pueden acceder a Gran Canopy y Muro de Escalada. Visitantes de Categoría Condor pueden acceder a Gran Canopy, Muro de Escalada, Puente Colgante, Salto al Vacío."\
                        "Aventura Kids está disponible para aventureros de entre 100 cm y 150 cm "
ubicacion = "Nos encontramos ubicados en avenida el cerro #750 en la comuna de providencia, a un costado de la estación Oasis de teleférico Santiago. La estación de metro más cercana es Pedro de Valdivia."
lugares = "Estación Oasis del Parque Metropolitano de Santiago."
compra = "Para adquirir su ticket lo debes realizar mediante la página web de https://aventuraonline.cl/. El pago por la pagina web es realizado mediante tarjeta, ya sea de debito o crédito. Tickets de Aventura Kids sólo se venden de manera presencial."
entrada = "Si vienes ya con tu ticket de compra online debes validarlo directamente en la caja de parque aventura, donde te entregaremos el brazalete donde te asociarán y validaran tus jugadas. recuerda llegar 10 min antes de tu horario de ingreso para poder validar tu ticket sin inconvenientes."
# horario_regular = "Parque aventura abre sábados, domingo y festivos de 10:00 a 19:00 hrs. En temporada de invierno parque aventura opera de 10:00 a 18:15hrs. en caso de condiciones climáticas que no nos permitan operar el parque cerrará sus puertas por seguridad de los aventureros y trabajadores."
if datetime.date.today() < datetime.date(2024,3,1):
    horario_regular = "El horario de funcionamiento es de miércoles a viernes desde 13:45 a 20:00 hrs y días Sábado y Domingo de 10:00 a 20:00 hrs. "
else:
    horario_regular = "Parque Aventura atiende miércoles, jueves y viernes entre 12:00 y 19:00 hrs, y Sábado, Domingo y festivos de 10:00 a 19:00 hrs, mientras que Aventura Kids está disponible de Martes a Domingo de 10:00 a 18:45 hrs. "
combinaciones = "con el ticket vive el parque Full Aventura, pueden acceder a los servicios de Parque Aventura, teleférico, buses ecológicos y Funicular."
sitios_de_interes = "Jardín japonés, Jardín Mapulemu."
servicios_cercanos = "Estacionamientos públicos en avenida el cerro con calle el rey, estos son gratuitos, solo con propina al cuidador."
validez_tickets = "El ticket es valido solo para el día y horario agendado."
condiciones = "Por la seguridad de nuestros visitantes la estatura mínima en Parque Aventura es 1,30 m. y la estatura máxima 1,95 m, mientras que en Aventura Kids, la estatura mínima es 100 cm y la máxima es 150 cm. al igual que el peso máximo para los juegos, el peso máximo es de 115kg. En caso de encontrarse embarazada por seguridad del aventurero en camino no puede realizar las actividades, pero si ingresar como acompañante. Si eres menor de edad, debes venir siempre acompañado por un adulto responsable. La cantidad de adultos acompañantes es máximo 1 por jugador."\
                "No existe el Ticket Abierto (sin fecha). La única forma de adquirir un ticket sin fecha es para grupos grandes (ventas corporativas), contactandose con Paula Ibarra: pibarra@turistik.com. "\
"Los tickets son válidos para la fecha  y hora reservada, si la persona llega después de la hora indicada, no podrá hacer uso de su ticket. Sólo se podrá hacer una excepción, por medio de un supervisor de teleférico, dependiendo del flujo de visitantes en dicho momento."
servicios_adicionales = "Estos son servicios adicionales, los cuales se deben agendar y son independientes del horario normal de funcionamiento:"\
"- Para celebrar cumpleaños: contactar a mlaguna@turistik.com o al número: + +56 9 6441 3723. "\
"- Para eventos de empresas: contactar a pibarra@turistik.com. "\
"- Para eventos de colegios y grupos grandes: contactar a gshinya@tursitik.com y/o educacion@turistik.com."
servicios_no_disponibles = "salto al vacío se encuentra cerrado por mantención. Para obtener información más actualizada solicite hablar con un agente."
beneficios = "El beneficio de Senadis para personas con credencial de discapacidad no incluye acceso a Parque Aventura, este beneficio solo puede ser utilizado en Teleférico, Funicular y Buses Ecológicos."\
                "Promoción Cuponatic Parque Aventura: Para canjear el ticket correspondiente, se debe enviar un correo a parqueaventura@turistik.com para proporcionar sus datos y agendar su cupo"
emergencias = "en caso de algún incidente o información por emergencia, correo de contacto es experiencia@turistik.com."
servicio_cliente = "En caso de algún problema o reclamo te debes contactar al correo parqueaventura@turistik.cl."
contacto_profesional = "si deseas comunicarte para colaboraciones, publicidad, proveedor enviar correo a parqueaventura@turistik.cl."
adicional = "En caso de venir con sus mascotas deben permanecer siempre con ustedes."\
"En caso de querer trabajar con nosotros debes enviar correo a seleccion@turistik.com o parqueaventura@turistik.cl.\n Los cupos para compra online se van actualizando semana a semana, por lo que no es posible comprar con mucha anticipación."\
"El ingreso con ticket está permitido entre las 10:00 hrs, hasta las 19:00 hrs como último ingreso, independiente de lo que indique el ticket reservado."
adicional += "A continuación está la información del Summer Camp delimitado por tags XML: <\summercamp>"+txt_summer_camp+"<summercamp>"
adicional += "\nEn Parque Aventura contamos con un beneficio para cumpleañeros: El cumpleañero debe venir con dos acompañantes que compren su ticket, y el cumpleañero entra gratis (Es decir, pagan 2 y entran 3). El beneficio es válido comprando Ticket 3 Aventuras o Parque Full Aventura, para ser utilizado una vez durante el mes del cumpleaños, presentando su carnet. "
adicional += "\nContamos con lockers en nuestras instalaciones de parque aventura para que puedas guardar tus pertenencias mientras realizas las actividades, funciona con moneda de 100 pesos. "
adicional += "\n Dentro del Cerro San Cristobal se encuentran las piscinas Antilén y Tupahue. A continuación se detalla su información de apertura:"
adicional += "\nLa piscina antilén se encuentra temporalmente fuera de servicio y no estará funcionando para esta temporada de verano 2024-2025. "
adicional += "\nLa piscina tupahue se encuentra abierta, funcionando de miércoles a domingo entre 10:30 y 17:00 hrs, con venta presencial solamente."
adicional += """\nLa nueva atracción MiniGolf de ParqueAventura es un juego para toda la familia y tiene un costo de $6.900 por persona. Funciona de martes a domingo de 10:00 a 19:45 hrs. El ticket incluye palo y pelota. Se puede jugar desde 1 a 4 personas. El juego cuenta con 9 hoyos y no tiene un límite de tiempo. El recorrido aproximado de los 9 hoyos es de 30 minutos."""

apertura = redactar_apertura(rol)+"\n"+redactar_apertura("Kids")

content = "Tu nombre es Kai, eres un asistente virtual de Turistik, asignado a responder en el chat de redes sociales de " + rol\
            + "Como asistente virtual, no puedes ayudar a comprar tickets ni a ayudar con problemas para reservar. Si un usuario tiene problemas particulares, debe escribir \"Agente\" para ser atendido de mejor manera."\
            + "Si no hay cupos disponibles en el sitio web, se debe tener en cuenta que los cupos se abren con una semana de anticipación, por lo que no es posible comprar para fechas posteriores. Si está intentando comprar dentro de esta semana y no aparecen cupos, entonces significaría que se han agotado."\
            + "Debes considerar las siguientes reglas para tu comportamiento: "+ reglas_kai \
            + "Esta es la información que debes considerar: "\
            + "Precios: " + precios \
            + rol + apertura_ahora(rol)+" y "+apertura_ahora("Kids")+apertura+"Horario: " + horario_regular\
            + "Ubicación: " + ubicacion \
            + " Lugares: " + lugares + "Como comprar: "+compra+" Ingreso: "+entrada+" Otros Servicios: "+combinaciones + \
            "Sitios de interés: "+sitios_de_interes+" Servicios: "+servicios_cercanos+" Validez: "+validez_tickets+\
            " Beneficios: "+beneficios+\
            "Servicios adicionales: "+servicios_adicionales+\
            " Información adicional: "  + descripcion_general + descripcion_servicio + comer + atracciones + adicional+\
            "Contacto profesional: "+contacto_profesional + \
            comentarios + contacto + "El nombre Kai, se compone de la letra K, la cual es la última letra en Turistik, junto con las letras AI de Artificial Intelligence (Inteligencia Artificial), en otras palabras su significado sería \"Inteligencia Artificial de Turistik\"."\
            "Turistik es un operador turistico ubicado en Santiago de Chile. Turistik opera servicios concesionados del Cerro San Cristobal: Teleférico, Funicular, Buses Ecológicos, Parque Aventura, Cafeterías Delicatto y Café Tudor (Salón Tudor)."


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
            txt = "Responde amable, brevemente y en el mismo idioma al siguiente texto: \""+txt+"\""
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


def main(req: func.HttpRequest) -> func.HttpResponse:
     # cambio input req
    j = req.get_json()
    msg = j["text"]
    userid = j["userid"]
    canal = j["canal"]
    intencion = j["derivar"]
    lang = j["idioma"]
    try:
        reply = generar_respuesta(msg, userid, lang, canal, intencion)
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
    finally:
        logging.info('Código ejecutado con código '+str(cod_status))
        return func.HttpResponse(
                json.dumps(reply),
                status_code=cod_status,
                mimetype="application/json"
            )