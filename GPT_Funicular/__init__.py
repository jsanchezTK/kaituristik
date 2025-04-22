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


tabla_precios = """[
(Ida 2 Tramos; 1.600; 800; 2.050; 1.050; 3.150; 1.600; 4.100; 2.050; «Dos tramos: Desde estación base (Pío Nono) a estación final (Cumbre) y viceversa. »),
(Ida y Vuelta; 2.250; 1.150; 2.950; 1.450; 4.500; 2.250; 5.850; 2.950; «Ticket de ida y vuelta entre estación base y final (Pío Nono - Cumbre). »),
(Combinado; 4.600; 2.750; 5.650; 3.390; 6.150; 3.550; 7.700; 4.390; «Ticket desde estación base de Funicular (Pío Nono), subiendo hasta estación final (Cumbre) y luego bajando a estación base de Teleférico (Oasis). »),
(Redondo; 6.100; 3.650; 7.570; 4.450; 8.350; 4.750; 10.470; 5.950; «Recorrido completo ida y vuelta en Funicular y Teleférico. Desde Pío Nono hasta Oasis, pasando por Cumbre, y de regreso a Pío Nono.»),
(Vive El Parque; 8.850; 6.750; 8.850; 6.750; 8.850; 6.750; 8.850; 6.750; «Ticket ilimitado durante el día para Teleférico, Funicular y Buses Ecológicos.»)
]"""

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

# txt_piscina = "Servicio de buses sólo estará disponible para traslado a piscinas y no realizará el recorrido hacia Cumbre. El recorrido hacia piscinas es en específico a la Piscina Tupahue."\
# "Para llegar a la piscina existen dos opciones: Desde Pío Nono se debe tomar el ticket de bus ecológico y desde Pedro de Valdivia Norte se debe tomar el Teleférico en Estación Oasis. Se recomienda comprar ticket de ida y vuelta, para evitar quedar sin cupo de regreso. Para adquirir ticket de regreso se debe hacer de manera presencial en Cafetería Delicatto o en los Tótems habilitados."\
# "El medio de pago para tickets de piscina presencial es sólo efectivo de manera presencial, mientras que para comprar entradas online, se debe ingresar a https://parquemet.cl/."\
# "El horario de funcionamiento de las piscinas es desde 10:30 a 18:00 hrs de martes a domingo."\
# "El valor de tickets de piscina es $4.000 para niños entre 3 a 13 años, $7.000 para adultos y para tercera edad el ticket es gratis (Se considera tercera edad desde 60 años). Visitantes con carnet de SENADIS pueden ingresar de manera gratuita junto a un acompañante incluído."


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
    # cierre = datetime.timedelta(hours=18, minutes=45)
    cierre = datetime.timedelta(hours=19, minutes=45)
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


mapa = "http://mapasancristobal.turistik.com/"

atracciones_parque = {
    "Jardín Japonés": "Su entrada es liberada y está abierto de 9:00 a 18:00 hrs. La mejor forma de llegar es por la entrada al Parque Metropolitano por calle Pedro de Valdivia Norte, caminar hacia estación Oasis y tomar el sendero ubicado a un costado del Restaurant Divertimento Chileno. Envía el link del mapa: "+mapa,
    "Zoológico Metropolitano:": "Para reservar y asegurar su entrada: https://reservas.parquemet.cl/zoologico. Para visitas educativas contacta a la Unidad Educativa al correo electrónico: zooeducacion@parquemet.cl. Ya no hay restricciones por COVID en el zoológico. Envía el link del mapa: "+mapa+". Para llegar al zoológico desde la entrada de Pedro de Valdivia (Teleférico), recomendamos tomar el bus ecológico hasta Pío Nono y subir caminando desde ahí. Actualmente el Funicular no se detiene en el Zoológico."
#     ,"Piscinas": "Las piscinas se encuentran fuera de termporada hasta 2024, puedes ver su ubicación en el mapa: "+mapa
}
lista_atracciones = ""
descripcion_atracciones = ""

for i in atracciones_parque.keys():
    lista_atracciones += i+", "
    descripcion_atracciones += i+": "+atracciones_parque[i]+". "
lista_atracciones = lista_atracciones[:-2]+". "

# Reglas
reglas_kai = """1. Siempre saludar y presentarte en la primera interacción
"2. Responder con amabilidad
"3. Dar respuestas breves y en el mismo idioma que te hablaron
"4. Sólo dar detalles cuando sean solicitados
"5. No recomendar otros operadores turísticos
"6. Debes considerar estrictamente la información presentada acá.
"7. No existen otros servicios fuera de los mencionados acá.
"8. Si no tienes suficiente información, debes sugerir al usuario que escriba la palabra “Agente” para ser atendido por una persona."""

reglas_especificas = "9. Si te preguntan por tours, no respondas y sugiere ingresar a www.turistik.com para obtener más información."

# Datos Generales
comentarios = "Para consultas por reclamos y problemas, el contacto es experiencia@turistik.com. Para ventas corporativas y grupos grandes, contactar a Paula Ibarra: pibarra@turistik.com"
contacto = "Para consultas profesionales, colaboraciones y otros: https://turistik.com/contacto/. No existe ningún número de contacto."
comer = "En todas las estaciones del teleférico hay locales de las cafeterías Delicatto, donde hay helados artesanales, café de grano, bebidas y más. También se puede visitar el Café Tudor, ubicado cerca de la estación cumbre de Funicular, para llegar se debe tomar el Funicular desde Estación Pío Nono y bajar en Cumbre, o para llegar desde Teleférico se puede tomar en Estación Oasis y bajar en Cumbre. Café Tudor ofrece café de grano, bebidas, pastelería, entre otros. "
# proximos_feriados = "21 de Mayo de 2023"
atracciones = "Dentro del Parque Metropolitano se encuentran distintas atracciones, tales como: "+lista_atracciones+""\
                "Descripción de las atracciones: "+descripcion_atracciones


# Funicular
rol = "Funicular de Santiago"
url = "https://funicularonline.cl/"
enunciado_precios = "There is a table that has all the information about our tours and services. This is the \"Tickets Teleferico\" table." \
                "Each row of the table is represented as a python tuple, but values are separated by semicolon instead of comma. " \
                "The entire table has the structure of a python list containing the tuples that represent rows. " \
                "Each row ends with a comma and a line break after the tuple (\",\\n\")." \
                "All the data in the table is in spanish. When you give the ticket prices, you have to point out if the price is for week day or for weekends. " \
                "This, this table has 6 columns which are from first to last:" \
    "[nombre del ticket], [precio adulto dia semana], [precio niños y 3ra edad semana], [precio adulto dia fin de semana], [precio niños y 3ra edad fin de semana], [precio adulto extranjero dia semana], [precio niños y 3ra extranjero edad semana], [precio adulto extranjero dia fin de semana], [precio niños y 3ra edad extranjero fin de semana], [descripción]." \
                "This is the table data: "+tabla_precios
precios = enunciado_precios+ " Para efecto de cobro y emisión de tickets se consideran niños desde los 2 y hasta los 11 años inclusive. Turistik no opera el Zoológico, por lo que no puedes entregar información sobre precios."
# horarios = "De lunes abierto desde las 13:00 hrs hasta las 19:00 hrs. Martes a domingo
#  abierto desde 10:00 hrs hasta las 19:00 hrs. Cerrado el primer lunes de cada mes por mantenimiento, excepto feriados."
# ubicacion = "Calle Pío Nono, Barrio Bellavista. Para llegar, se debe caminar desde la estación Baquedano del Metro por la calle Pío Nono hacia el norte, hasta llegar al final de la calle. Como referencia, al norte del Río Mapocho se encuentra la Facultad de Derecho de la Universidad de Chile."
descripcion_general = "Un servicio de transporte seguro, recién remodelado y restaurado, que te permite disfrutar de un viaje por la historia hasta la cumbre del cerro San Cristóbal. Declarado Monumento Histórico el año 2000, por el valor de su complejo sistema de transporte por cables y su importancia como elemento patrimonial de Santiago, presente en la memoria colectiva de sus habitantes. Cuenta con un motor eléctrico el cual transmite la potencia a través de poleas al cable que sostiene cada uno de los carros. Tiene capacidad para 35 personas en cada carro."
descripcion_servicio = "El servicio cuenta con 3 estaciones: Pío Nono, Zoológico (Temporalmente deshabilitada) y Cumbre." \
                        "Pío Nono es la primera estación y la última es Cumbre, donde se puede hacer combinación al Teleférico." \
                        "El trayecto desde la estación base hasta la estación Zoológico demora aproximadamente 2 minutos. Sin embargo, esta parada se encuentra deshabilitada." \
                        "El trayecto desde la estación base hasta la estación Cumbre demora aproximadamente 8 minutos. " \
                        "Estaciones Tupahue y Oasis pertenecen al Teleférico y no se puede abordar el funicular en ninguna de ellas."\
                        "Para llegar al zoológico se debe caminar desde la Plaza Caupolicán, ubicada en el ingreso desde Pío Nono."\
                        "Los boletos para acceder al Zoo se deben comprar de manera independiente. "\
                        "Ticket Sólo Ida y Ticket Ida y Vuelta conectan las estaciones Pío Nono y Cumbre. Ticket Vive el Parque puede iniciar recorrido en cualquier estación de Teleférico y Funicular."
ubicacion = "La dirección de Funicular Santiago es Pionono 445. " \
            "Puedes acceder desde la estación de metro Baquedano y caminar hacia el norte hasta el ingreso de pionono del Parque Metropolitano. " \
            "Si vienes en vehiculo deberás estacionar en los alrededores y caminar hasta la entrada del parque. " \
            "No esta permitido el acceso de vehículos al Parque Metropolitano."
lugares = "Estación Pionono - Es la estación base ubicada en el acceso Pionono del Parque Metropolitano " \
            "Estación Zoológico - Segunda estación la cual da acceso al Zoológico Metropolitano. Sin embargo, esta parada se encuentra deshabilitada y la alternativa para llegar al zoológico es caminar desde la Plaza Caupolicán, ubicada en el ingreso desde Pío Nono. " \
            "Estación Cumbre - Estación ubicada en la cumbre del cerro San Cristóbal, donde podrás encontrar la Terraza Bellavista, Salón Tudor, y el santuario de la Inmaculada Concepción (Estatua de la Virgen). Estaciones Oasis y Tupahue pertenecen a Teleférico, no a Funicular, por lo que no deben ser mencionadas aquí."
compra = "La compra de los tickets la puedes realizar a través de la pagina web www.funicularsantiago.cl con tarjeta de crédito o débito mediante el servicio de Webpay. " \
            "También de manera presencial en las boleterías de Funicular Santiago. Puedes realizar el pago con efectivo en peso chileno, o tarjetas de crédito o débito. " \
            "Recuerda que Funicular Santiago tiene tarifas diferenciadas entre residentes y turistas extranjeros.0"\
            "Si tienes problemas para pagar con tus tarjetas, puedes comprar presencialmente en nuestros tótems de autoatención. "
entrada = "Te recomendamos llegar temprano ya que al ser un atractivo turístico y un patrimonio de chile, es un destino que todos quieren visitar. " \
            "Si vienes con tu boleto comprado a través de la web, no debes realizar la fila de compra presencial. " \
            "Los tickets son validados por nuestro personal antes de abordar el carro. " \
            "Esta permitido el ingreso de mascotas con su correa o caja transportadora, en caso de los perros medianos a grandes deben ingresar con bozal de seguridad. " \
            "Se puede ingresar con coche de bebé, pero este debe estar plegado al momento del embarque para evitar accidentes."
horario_regular = "Abierto en días feriados" \
                    "El horario de funcionamiento regular es: " \
                    "Invierno: de 10:00 a 19:00 hrs. Siendo el último Ingreso a las 18:45 hrs. Estación Zoo hasta las 16:30 hrs " \
                    "Verano: de 10:00 a 19:45 hrs. Estación Zoo hasta las 17:30 hrs. " \
                    "Los días lunes el servicio comienza a las 13:00 hrs " \
                    "El primer lunes de cada mes se encuentra cerrado por mantenimiento, excepto si es feriado. " \
                    "Los diferentes boletos disponibles para compra también tienen horarios límite, esto para que puedas disfrutar de la experiencia completa que decidas adquirir."
combinaciones = "En Funicular podrás adquirir boletos de Funicular, y además algunos servicios combinados con Teleférico Santiago y con los Buses Ecológicos. " \
                "Además podrás encontrar el Infocenter Funicular donde puedes comprar paseos de la empresa Turistik. Tales como cordillera, viñedos, bus Hop on - hop off, entre otros."
sitios_de_interes = "Cerca de Funicular Santiago puedes encontrar la casa museo de Pablo Neruda \"La Chascona\", el barrio Bellavista, Patio Bellavista, Zoológico Metropolitano, Salón Tudor, Santuario Inmaculada Concepción, entre otros."
servicios_cercanos = "En el sector de Pionono podrás encontrar una gran variedad de restaurantes y bares donde disfrutar de la gastronomía chilena. " \
                        "Encontrarás cajero automático al ingreso del Parque Metropolitano. " \
                        "Estacionamiento puedes encontrar en las calles aledañas las cuales cuentan con parquímetros por horas."
validez_tickets = "Los tickets son válidos por el día y horario comprado. Puedes comprar de manera diferida si deseas planificar tu paseo para una fecha especifica."
condiciones = "Funicular Santiago, declara expresamente que no se realizarán cambios, re-agendamientos ni devoluciones una vez finalizado el proceso de compra, basándose en lo establecido en el artículo 3 bis, letra b) de la ley número 19.496. Una vez comprado los tickets, el usuario declara cumplir con toda las normas y condiciones que exigen los servicios del Funicular Santiago.  " \
                "En caso de cierre de Funicular Santiago por razones de fuerza mayor, donde no podamos prestar el servicio contratado, se procederá a reagendar los tickets de nuestros visitantes, previa autorización y coordinación con el comprador y si él mismo no desea reagendar, se procederá a una devolución que ocurre dentro de 10 a 15 días hábiles. " \
                "Los niños menores de dos años tienen acceso liberado, y el precio de valor niño es hasta los 11 años, 11 meses y 30 días. " \
                "Se considerará público de tercera edad a los adultos mayores que tengan sobre 60 años, presentando su cedula de identidad en boleterías. Al comprar online, la venta será con" \
                " opciones libres, no obstante al momento de ingresar a cualquier estación un ejecutivo comercial verificará la información solicitando cedula de identidad."\
                "No existe el Ticket Abierto (sin fecha). La única forma de adquirir un ticket sin fecha es para grupos grandes (ventas corporativas), contactandose con Paula Ibarra: pibarra@turistik.com. "\
"Los tickets son válidos para la fecha  y hora reservada, si la persona llega después de la hora indicada, no podrá hacer uso de su ticket, sólo se podrá hacer una excepción por medio de la autorización de un supervisor, dependiendo del flujo de visitantes en dicho momento."
servicios_adicionales = "Ninguno"
servicios_no_disponibles = "Ninguno"
beneficios = "Los niños menores de dos años tienen acceso liberado, y el precio de valor niño es hasta los 11 años, 11 meses y 30 días. " \
                "Las personas mayores de 60 años también optaran a una tarifa rebajada presentando documento que acredite edad. " \
                "Los visitantes que tengan algún tipo de discapacidad tendrán acceso totalmente liberado al " \
                "Funicular Santiago mostrando en boletería su credencial de discapacidad, la cual es emitida por el Servicio de Registro Civil e Identificación o Senadis. El beneficio de Senadis para personas con credencial de discapacidad no incluye acceso a Parque Aventura."\
                "Promoción Metropuntos de Metrogas: Para hacer válido el canje, sólo se deben presentar en alguna estación de Teleférico o Funicular y mostrar el código QR que ha recibido."
emergencias = "En caso de incidente o suspensión del servicio, el usuario puede renunciar a continuar en el sistema y a que se le reconozca un viaje o la devolución de su dinero."
informacion_adicional = "Hay un tour gratuito por la de Sala de Máquinas de Funicular en el Día del Patrimonio, las personas solo deben llenar el formulario y luego acercarse el día y hora indicada, el cupo está asegurado, no es necesaria una confirmación. La inscripción del día del patrimonio es solo para el tour por la sala de máquinas, no incluye ticket de funicular. Este es el enlace de inscripción: https://forms.office.com/Pages/ResponsePage.aspx?id=JBozhIE56kuLAAb2VaaX09yGNHmRPxtGnvjbp-N1Y2dUOFJPVENOTFgzM1ZLQzBGWjFJRDgySDhSWS4u"
contacto_profesional = "Contacto equipo de marketing para publicidad, activaciones de marca y otros: jedwards@turistik.com. "\
"En caso de querer trabajar con nosotros debes enviar correo a seleccion@turistik.com, indicando si buscas trabajo o práctica, área de interés y disponibilidad. "
adicional = "En los días de alto flujo como fines de semana o festivos se recomienda agendar con antelación de manera online para asegurar su ticket ya que los cupos son limitados. Link a mapa interactivo: https://app.zapt.tech/#/map?placeId=-mvomttnndlxoct1ssic&floorId=0&bottomNavigation=false&splash=false. Los cupos para compra online se van actualizando semana a semana, por lo que no es posible comprar con mucha anticipación; para obtener ayuda sobre los cupos el usuario debe escribir \"Agente\" y será derivado."
adicional += "A continuación está la información del Summer Camp delimitado por tags XML: <\summercamp>"+txt_summer_camp+"<summercamp>"
# adicional += "A continuación está la información de Piscinas, delimitado por tags XML: <\piscina>"+txt_piscina+"<piscina>"
adicional += "\n Dentro del Cerro San Cristobal se encuentran las piscinas Antilén y Tupahue. A continuación se detalla su información de apertura:"
adicional += "\nLa piscina antilén se encuentra temporalmente fuera de servicio y no estará funcionando para esta temporada de verano 2024-2025. "
adicional += "\nLa piscina tupahue se encuentra abierta, funcionando de miércoles a domingo entre 10:30 y 17:00 hrs, con venta presencial solamente."
adicional += "\nPara poder acceder al beneficio de guía liberado, las entradas deben ser compradas presencialmente, presentando credencial de guía turístico y debe ir acompañado de mínimo 2 visitantes. "
adicional += "\nEste 14 de Febrero de 2025, Funicular de Santiago los invita a todos a celebrar el día del amor y la amistad con una experiencia única: Sunset en Funicular. Esta experiencia inicia en el Castillo Pío Nono para tomar el Funicular hasta la cumbre del Cerro San Cristóbal. Ahí serán recibidos en el Café Tudor, el café más alto de Santiago y un lugar imperdible por sus vistas panorámicas de la ciudad. Ahí nuestros visitantes podrán disfrutar de un cóctel y maridaje de vinos, acompañados de música en vivo, además de muchas sorpresas para las parejas. El valor por persona es de $20.000 y tendrá lugar desde las 20:00 hasta las 23:00 hrs del viernes 14 de febrero. Hay 4 horarios para subir en Funicular: 20:00, 20:20, 20:40 y 21:00 hrs. Para más información y reservas: https://turistik.com/tours/tours-compartidos/sunset-funicular-santiago-dia-enamorados/."
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
            " Información adicional: "  + descripcion_general + descripcion_servicio + comer + atracciones + adicional +\
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
        # tokens_content = openai_summarizer.count_tokens(content)
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