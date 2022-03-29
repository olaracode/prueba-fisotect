#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Controlador Base de Datos

    Archivo creado para el creacion de BD y elementos relacionados con esta

     :Copyright: 2019, FISOTEC
    :Version:   1.0

    :Director:      Jose Ruiviejo Gutiérrez
    :Jefe Proyecto: Rafael Francisco Yeguas López
    :Autores:       Sara Valverde Padilla
    :Autores:       Rafael Francisco Yeguas López

"""
# Importamos las clases de nuestro plugin

from credenciales import *

# Importamos las clases externas
import psycopg2
from psycopg2 import extras
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class FisotecBaseDatos:

    """
        Esta clase contienene métodos para la conexion con la base de datos, para crearlas, inserción.
        Además e crear la base de datos dispone de metodos para crear tablespaces y esquemas
        Y métodos para cargar un fichero sql y ejecutarlo
    """

    # CONEXION

    @staticmethod
    def conectarBaseDatos():
        """
        Abre una conexión con la base de datos y devuelve el cursor de la misma.

        Utiliza la librería psycopg2

        :return:    Conexión a la base de datos
        :type:      psycopg2.extras.RealDictCursor

        """

        # Conectamos con la base de datos
        con = connect(user=DBUSER, host=DBHOST, password=DBPASSWORD, port=DBPORT, dbname=DBNAME)

        # Fijamos el nivel de aislamiento para la transaccion de la sesión actual, fijándolo a un nivel de autocommit
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Devolvemos un cursor a la conexión, fijándolo como una subclase de extras.RealDictCursor para obtener los
        # resultados en forma de diccionario
        cur = con.cursor(cursor_factory=extras.RealDictCursor)

        return cur

    @staticmethod
    def cerrarBaseDatos(conexion):
        """
        Cierra la conexión a la base de datos.

        :param conexion:    Conexión abierta a la base de datos
        :type conexion:     psycopg2.extras.RealDictCursor

        :return:    None
        """
        conexion.close

    @staticmethod
    def consultaSQL(conexion, consulta):

        """
        Ejecuta la consulta SQL recibida por parámetro y devuelve el resultado

        :param conexion:    Conexión a la base de datos
        :type conexion:     psycopg2.extras.RealDictCursor

        :param consulta: consulta que se debe ejecutar
        :type consulta: str

        :return: lista con los resultados de la consulta
        :rtype: list

        """

        #realiza la consulta recibida por parámetro y devuelve el resultado
        try:
            conexion.execute(consulta)
            result = conexion.fetchall()

            return result

        #si falla muestra mensaje de error y devuelve lista vacia
        except Exception as error:
            print(error)
            return []