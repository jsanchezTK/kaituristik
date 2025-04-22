import logging
import json
import azure.functions as func
import os
from openai import OpenAI
import datetime
from datetime import timedelta
import pytz
import pyodbc as odbc
import traceback
import tiktoken

tabla_precios = "[\
(Ida 1 Tramo; 2.100; 1.360; 2.520; 1.630; «Sólo 1 tramo (Ejemplo: Desde estación base (Oasis) hasta estación intermedia (Tupahue)). »), \
(Ida 2 Tramos; 3.000; 1.950; 3.600; 2.340; «Dos tramos: Desde estación base (Oasis) a estación final (Cumbre) y viceversa. »), \
(Ida y Vuelta; 3.850; 2.500; 4.620; 3.000; «Ticket de ida y vuelta entre estación base y final (Oasis - Cumbre). »), \
(Vive El Parque; 8.850; 6.750; 8.850; 6.750; «Ticket ilimitado durante el día para Teleférico, Funicular y Buses Ecológicos.»), \
"

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
# 
# txt_piscina = "Servicio de buses sólo estará disponible para traslado a piscinas y no realizará el recorrido hacia Cumbre. El recorrido hacia piscinas es en específico a la Piscina Tupahue."\
# "Para llegar a la piscina existen dos opciones: Desde Pío Nono se debe tomar el ticket de bus ecológico y desde Pedro de Valdivia Norte se debe tomar el Teleférico en Estación Oasis. Se recomienda comprar ticket de ida y vuelta, para evitar quedar sin cupo de regreso. Para adquirir ticket de regreso se debe hacer de manera presencial en Cafetería Delicatto o en los Tótems habilitados."\
# "El medio de pago para tickets de piscina presencial es sólo efectivo de manera presencial, mientras que para comprar entradas online, se debe ingresar a https://parquemet.cl/."\
# "El horario de funcionamiento de las piscinas es desde 10:30 a 18:00 hrs de martes a domingo."\
# "El valor de tickets de piscina es $4.000 para niños entre 3 a 13 años, $7.000 para adultos y para tercera edad el ticket es gratis (Se considera tercera edad desde 60 años). Visitantes con carnet de SENADIS pueden ingresar de manera gratuita junto a un acompañante incluído."

def importar_calendario() -> list:
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


def apertura_ahora(rol) -> str:
    tz = pytz.timezone('Chile/Continental')
    cierre = datetime.timedelta(hours=18, minutes=45)
    hoy = datetime.datetime.now(tz=tz).date()
    cierre_hoy = datetime.datetime(hoy.year, hoy.month, hoy.day, tzinfo=tz) + cierre
    d = {"Teleférico de Santiago": "AperturaTeleferico", "Funicular de Santiago": "AperturaFuni", "Parque Aventura": "AperturaParque"}
    col = d[rol]
    estado = get_row(int(datetime.datetime.now(tz=tz).date().strftime("%Y%m%d")), importar_calendario())[col]
    if estado == "Abierto":
        if datetime.datetime.now(tz=tz) < cierre_hoy:
            estado = "Abierto"
        else:
            estado = "Cerrado"
    return estado


def redactar_apertura(rol):
    d = {"Teleférico de Santiago": 4, "Funicular de Santiago": 5, "Parque Aventura": 6, "Kids": 7}
    col = d[rol]
    calendario = importar_calendario()
    txt = f"Esta es la información de apertura de esta semana y la próxima en {rol}:\n"
    for i in calendario:
        txt += f"{i[10]} {i[col]}, \n"
    return txt[:-2]+". "


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
contacto = "Para consultas profesionales, colaboraciones y otros: https://turistik.com/contacto/. No existe ningún número de contacto."
comer = "En todas las estaciones del teleférico hay locales de las cafeterías Delicatto, donde hay helados artesanales, café de grano, bebidas y más. También se puede visitar el Café Tudor, ubicado cerca de la estación cumbre de Funicular, para llegar se debe tomar el Funicular desde Estación Pío Nono y bajar en Cumbre, o para llegar desde Teleférico se puede tomar en Estación Oasis y bajar en Cumbre. Café Tudor ofrece café de grano, bebidas, pastelería, entre otros. "
proximos_feriados = "21 de Mayo de 2023"
atracciones = "Dentro del Parque Metropolitano se encuentran distintas atracciones, tales como: "+lista_atracciones+""\
                "Descripción de las atracciones: "+descripcion_atracciones

# Teleférico
rol = "Teleférico de Santiago"
url = "https://telefericoonline.cl/"
enunciado_precios = "There is a table that has all the information about our tours and services. This is the \"Tickets Teleferico\" table." \
                "Each row of the table is represented as a python tuple, but values are separated by semicolon instead of comma. " \
                "The entire table has the structure of a python list containing the tuples that represent rows. " \
                "Each row ends with a comma and a line break after the tuple (\",\\n\")." \
                "All the data in the table is in spanish. When you give the ticket prices, you have to point out if the price is for week day or for weekends. " \
                "This, this table has 6 columns which are from first to last:" \
    "[nombre del ticket], [precio adulto dia semana], [precio niños y 3ra edad semana], [precio adulto dia fin de semana], [precio niños y 3ra edad fin de semana], [descripción]." \
                "This is the table data: "+tabla_precios
precios = enunciado_precios + "Los precios mostrados acá son solo para residentes nacionales. Precios para extranjeros tienen otra tarifa, la cual se puede consultar en el sitio web: "+url+". Para efecto de cobro y emisión de tickets se consideran niños desde los 2 y hasta los 11 años inclusive. "
# horarios = "De lunes abierto desde las 13:00 hrs hasta las 19:00 hrs. Martes a domingo abierto desde 10:00 hrs hasta las 19:00 hrs. Cerrado el primer lunes de cada mes por mantenimiento, excepto feriados."
# ubicacion = "Calle Pío Nono, Barrio Bellavista. Para llegar, se debe caminar desde la estación Baquedano del Metro por la calle Pío Nono hacia el norte, hasta llegar al final de la calle. Como referencia, al norte del Río Mapocho se encuentra la Facultad de Derecho de la Universidad de Chile."
descripcion_general = "Teleférico de Santiago es un medio de transporte aéreo que inicia en el sector de Pedro de Valdivia, cuenta con 47 cabinas para 6 ocupantes cada una, que están en contínuo movimiento y llegan a la parte más alta del parque, a pasos del santuario y la estatua de la Virgen de la inmaculada Concepción, la cual es uno de los miradores más altos de la ciudad. cuenta con una estación intermedia llamada Tupahue a un costado de la piscina del mismo nombre"
descripcion_servicio = "Teleférico cuenta con 3 estaciones: Oasis, Tupahue y Cumbre. La primera estación es Oasis y la última es Cumbre. El recorrido de extremo a extremo es de 10 minutos aproximadamente y en las estaciones terminales todos deben descender de las cabinas, no se puede dar la vuelta en la misma. Niños de cualquier edad y mascotas cuentan como un ocupante en la cabina. "\
                        "Ticket Sólo Ida y Ticket Ida y Vuelta conectan las estaciones Oasis y Cumbre (se puede bajar en Tupahue y luego retomar el recorrido). Ticket Vive el Parque puede iniciar recorrido en cualquier estación de Teleférico y Funicular."
ubicacion = "La estación principal de Teleférico llamada Oasis se encuentra en el acceso de Pedro de Valdivia norte a Parquemet, a unos 15 minutos caminando desde la estación de metro Pedro de Valdivia de la linea 1, la dirección exacta es Av. El Cerro 750, Providencia. hay estacionamientos a pasos de la entrada del parque en av. el cerro 750, providencia y en av. el cerro esquina Carlos Casanueva, Providencia."
lugares = "Estación Oasis: Acceso Pedro de Valdivia Norte, es la estación inicial; Estación Tupahue: al costado de piscina Tupahue en la mitad del cerro, es la estación intermedia; Estación Cumbre: sector más alto del parque a pasos de Santuario de la Inmaculada Concepción, es la última estación de Teleférico y es el lugar donde se puede hacer combinación al Funicular. Estaciones Pío Nono y Zoológico son parte del Funicular, no de Teleférico. Estaciones Oasis y Tupahue son sólo de Teleférico y no se puede abordar el Funicular en ellas."
compra = "La compra se puede realizar de manera Online en telefericoonline.cl y de manera presencial según disponibilidad de tickets ya que contamos con cupos limitados. el pago online se puede realizar a través de webpay y paypal. el pago presencial es con tarjeta de crédito, débito y efectivo. los reembolsos y/o reagendamientos son solo posibles cuando teleférico no pueda entregar el servicio o por motivos de salud previamente informados por el visitante. hay precios diferentes dependiendo del día el tipo de ticket y si es niño, adulto o tercera edad."
entrada = "El tiempo de fila dependerá de la cantidad de visitantes que exista en el momento, los tickets se validan escaneando el codigo QR que aparece en el ticket Online y presencial al llegar a la estación y luego antes de embarcar a la cabina. Para el ingreso de mascotas por seguridad es necesario que esta siempre lleve puesta su correa y esté a cargo de un adulto. personas con movilidad reducida o en situación de discapacidad no deben realizar filas, solo acercarse a uno de nuestros ejecutivos que los guiarán hacia la subida a la cabina. Personas usuarias de sillas de rueda pueden ingresar en la misma a la cabina, el procedimiento lo realiza nuestro equipo y pueden ir con un máximo de 2 acompañantes al interior de la cabina."
horario_regular = "el horario de apertura del servicio completo es a las 10 de la mañana de martes a domingo y festivos, si un día festivo cae día lunes entonces se abrirá a público y se cerrará otro día previamente informado. los horarios de cierre dependen de la temporada; de septiembre a marzo a las 19:45 horas y de abril a agosto a las 19:00 horas, Siendo el ultimo ingreso a las 18:45 hrs. Ademas los Horarios de Los buses panoramicos es de Lunes a Viernes desde las 10:00 hasta las 18:30 hrs, sabado y domingo desde 13:00 hasta las 18:30 hrs. condiciones climaticas como fuertes vientos o tormentas eléctricas podrían ocacionar un cierre anticipado del servicio."
combinaciones = "Teleférico dependiendo del tipo de ticket puede combinar con otros servicios en cada estación. En estación Oasis se puede combinar con el bus Hop On Hop Off, en la Parada N°4, ubicada en Pedro de Valdivia Norte con Los Conquistadores. En estación Tupahue puede combinar con los Buses Ecológicos que van desde el acceso Pio nono hasta la Cumbre, y en estación Cumbre se puede combinar con Funicular y con los Buses Ecológicos."
sitios_de_interes = "Los atractivos cercanos a la estación Oasis son el jardín Japonés, el Parque de las Esculturas y a unos 15 minutos se encuentra el Costanera Center"\
"en estación Tupahue se encuentra la piscina Tupahue, la casa de la Cultura Anahuac, el Torreón Victoria, el centro de eventos Vista Santiago, la plaza de Juegos Gabriela Mistral, el jardín Botánico Mapulemu y la plaza Centenario."\
"en la Cumbre del parque se encuentra el santuario de la Inmaculada Concepción, la estatua de la Virgen, la terraza Bellavista, el Observatorio Foster, el Vivero Cumbre, la plaza México, el Funicular y la cafetería del salón Tudor."
servicios_cercanos = "Los estacionamientos solo se encuentran fuera del parque a un costado del acceso de Pedro de Valdivia, no pueden ingresar vehiculos particulares al parque, el pago del estacionamiento es con propina para el cuidador."
validez_tickets = "El ticket es valido para la fecha y hora que se adquirió y el tiempo de uso varía según el tipo de ticket; el ticket vive el parque funciona durante todo el día de 10 a 18:45 en invierno y de 10 a 19:45 en verano, el ticket ida y vuelta tiene una duración máxima de 4 horas o hasta el cierre de la operación para hacer el viaje de ida y regreso, los tickets de solo ida son validos para la hora que aparece en el ticket."
condiciones = "Los cupos de las cabinas son compartidos con más usuarios que contraten el servicio ese día, alcanzando un máximo de 6 personas o 480 kilos, considerando como ocupantes a bebés, menores de edad, coches y mascotas."\
"Es obligación descender en estaciones terminales; Oasis y Cumbre" \
"En la cabina siempre debe ir un adulto responsable, mayor de 18 años" \
"personas usuarias de silla de ruedas pueden ir con un máximo de 2 acompañantes dentro de la cabina" \
"no se puede fumar o beber alcohol dentro de las cabinas" \
"personas en estado de ebriedad o bajo los efectos de estupefacientes o psicotrópicos no pueden utilizar el servicio"\
"No existe el Ticket Abierto (sin fecha). La única forma de adquirir un ticket sin fecha es para grupos grandes (ventas corporativas), contactandose con Paula Ibarra: pibarra@turistik.com. "\
"Los tickets son válidos para la fecha  y hora reservada, si la persona llega después de la hora indicada, no podrá hacer uso de su ticket, sólo se podrá hacer una excepción por medio de la autorización de un supervisor, dependiendo del flujo de visitantes en dicho momento."
"Se puede ingresar con coche de bebé, pero este debe estar plegado al momento del embarque para evitar accidentes."
servicios_adicionales = "Para visitas de grupos corporativos o eventos especiales contactar a Pibarra@turistik .com"\
"Visitas de grupos en situación de vulnerabilidad contactar a info@parquemet.cl"\
"visitas de colegios contactar a gshinya@turistik.com"
servicios_no_disponibles = "Transporte de bicicletas no disponible en Teleférico."
beneficios = "Personas con credencial de Senadis pueden adquirir un Ticket para ellos y un acompañante de manera gratuita presentando su credencial de senadis y su carnet en nuestras boleterías (beneficio solo disponible una vez en el día). El beneficio de Senadis para personas con credencial de discapacidad no incluye acceso a Parque Aventura."\
                "Promoción Metropuntos de Metrogas: Para hacer válido el canje, sólo se deben presentar en alguna estación de Teleférico o Funicular y mostrar el código QR que ha recibido."
emergencias = "En caso de emergencia o accidente contactar al número de Emergencia de Parquemet 1466."
servicio_cliente = "en caso de consultas o reclamos escribir a través de mensaje directo en nuestras redes sociales como Facebook, Instragram y Whatsapp. serán derivados al áera responsable por uno de nuestros ejecutivos."
contacto_profesional = "Contacto equipo de marketing para publicidad, activaciones de marca y otros: jedwards@turistik.com"\
"En caso de querer trabajar con nosotros debes enviar correo a seleccion@turistik.com, indicando si buscas trabajo o práctica profesional, área de interés y disponibilidad."
adicional = "En los días de alto flujo como fines de semana o festivos se recomienda agendar con antelación de manera online para asegurar su ticket ya que los cupos son limitados. Link a mapa interactivo: https://app.zapt.tech/#/map?placeId=-mvomttnndlxoct1ssic&floorId=0&bottomNavigation=false&splash=false. Los cupos para compra online se van actualizando semana a semana, por lo que no es posible comprar con mucha anticipación; para obtener ayuda sobre los cupos el usuario debe escribir \"Agente\" y será derivado."
adicional += "A continuación está la información del Summer Camp delimitado por tags XML: <\summercamp>"+txt_summer_camp+"<summercamp>"
# adicional += "A continuación está la información de Piscinas, delimitado por tags XML: <\piscina>"+txt_piscina+"<piscina>"
adicional += "\n Dentro del Cerro San Cristobal se encuentran las piscinas Antilén y Tupahue. A continuación se detalla su información de apertura:"
adicional += "\nLa piscina antilén se encuentra temporalmente fuera de servicio y no estará funcionando para esta temporada de verano 2024-2025. "
adicional += "\nLa piscina tupahue se encuentra abierta, funcionando de miércoles a domingo entre 10:30 y 17:00 hrs, con venta presencial solamente."
adicional += "\nPara poder acceder al beneficio de guía liberado, las entradas deben ser compradas presencialmente, presentando credencial de guía turístico y debe ir acompañado de mínimo 2 visitantes. "
adicional += "\nEste 14 de Febrero de 2025, Funicular de Santiago los invita a todos a celebrar el día del amor y la amistad con una experiencia única: Sunset en Funicular. Esta experiencia inicia en el Castillo Pío Nono para tomar el Funicular hasta la cumbre del Cerro San Cristóbal. Ahí serán recibidos en el Café Tudor, el café más alto de Santiago y un lugar imperdible por sus vistas panorámicas de la ciudad. Ahí nuestros visitantes podrán disfrutar de un cóctel y maridaje de vinos, acompañados de música en vivo, además de muchas sorpresas para las parejas. El valor por persona es de $30.000 y tendrá lugar desde las 19:00 hasta las 23:00 hrs del viernes 14 de febrero. Para más información y reservas: https://turistik.com/tours/tours-compartidos/sunset-funicular-santiago-dia-enamorados/."

apertura = redactar_apertura(rol)

content = "Tu nombre es Kai, eres un asistente virtual de Turistik, asignado a responder en el chat de redes sociales de " + rol\
            + "Como asistente virtual, no puedes ayudar a comprar tickets ni a ayudar con problemas para reservar. Si un usuario tiene problemas particulares, debe escribir \"Agente\" para ser atendido de mejor manera."\
            + "Si no hay cupos disponibles en el sitio web, se debe tener en cuenta que los cupos se abren con una semana de anticipación, por lo que no es posible comprar para fechas posteriores. Si está intentando comprar dentro de esta semana y no aparecen cupos, entonces significaría que se han agotado."\
            + "Debes considerar las siguientes reglas para tu comportamiento: "+ reglas_kai \
            + "Esta es la información que debes considerar: "\
            + "Precios: " + precios \
            + rol + ". En este momento se encuentra "+apertura_ahora(rol)+". "+apertura+"Horario: " + horario_regular \
            + "Ubicación: " + ubicacion \
            + " Lugares: " + lugares + "Como comprar: "+compra+" Ingreso: "+entrada+" Otros Servicios: "+combinaciones + \
            "Sitios de interés: "+sitios_de_interes+" Servicios: "+servicios_cercanos+" Validez: "+validez_tickets+\
            " Beneficios: "+beneficios+\
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