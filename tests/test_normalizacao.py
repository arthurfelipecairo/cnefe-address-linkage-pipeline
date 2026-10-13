import os
import sys
import unittest
from pathlib import Path


if os.name == "nt":
    # O launcher do PySpark procura "python3" por padrao no Windows. Garante
    # que os testes usem o mesmo Python que iniciou a suite (por exemplo, a
    # .venv), mesmo quando ela nao foi ativada no terminal.
    python_directory = str(Path(sys.executable).parent)
    os.environ["PATH"] = python_directory + os.pathsep + os.environ["PATH"]
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.normalizacao import (
    is_valid_cnefe_municipality_code,
    is_valid_postal_code,
    is_valid_search_municipality_code,
    normalize_cnefe_dataframe,
    normalize_complement,
    normalize_municipality_code,
    normalize_neighborhood,
    normalize_number,
    normalize_postal_code,
    normalize_search_dataframe,
    normalize_street,
    normalize_text,
)


class NormalizationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("normalization-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def evaluate(self, values, expression, columns=("value",)):
        dataframe = self.spark.createDataFrame(values, columns)
        return [row.result for row in dataframe.select(expression.alias("result")).collect()]

    def test_text_street_neighborhood_and_complement(self):
        values = [
            ("  Av. Antônio   Carlos  ",),
            ("JD. PRIMAVERA",),
            ("BAIRRO",),
            ("None",),
            (None,),
        ]

        self.assertEqual(
            self.evaluate(values, normalize_text(F.col("value"))),
            ["av antonio carlos", "jd primavera", "bairro", None, None],
        )
        self.assertEqual(
            self.evaluate(values, normalize_street(F.col("value"))),
            ["avenida antonio carlos", "jd primavera", "bairro", None, None],
        )
        self.assertEqual(
            self.evaluate(values, normalize_neighborhood(F.col("value"))),
            ["av antonio carlos", "jardim primavera", None, None, None],
        )
        self.assertEqual(
            self.evaluate(values, normalize_complement(F.col("value"))),
            ["av antonio carlos", "jd primavera", "bairro", None, None],
        )

        self.assertEqual(
            self.evaluate(
                [("None EVILASIO FIRMATO",)],
                normalize_street(F.col("value")),
            ),
            ["evilasio firmato"],
        )

    def test_number_and_modifier(self):
        values = [
            ("S/N", None),
            ("102 E", None),
            (None, "SN"),
            ("0", None),
            ("0", "SN"),
            (None, None),
        ]
        result = self.evaluate(
            values,
            normalize_number(F.col("number"), F.col("modifier")),
            ("number", "modifier"),
        )
        self.assertEqual(result, ["sn", "102e", "sn", "0", "sn", None])

    def test_postal_code_preserves_leading_zero_and_rejects_bad_lengths(self):
        values = [("48.030-260",), ("01234-567",), ("1234567",), ("123456789",), (None,)]

        self.assertEqual(
            self.evaluate(values, normalize_postal_code(F.col("value"))),
            ["48030260", "01234567", None, None, None],
        )
        self.assertEqual(
            self.evaluate(values, is_valid_postal_code(F.col("value"))),
            [True, True, False, False, False],
        )

    def test_municipality_formats_and_source_validators(self):
        values = [
            ("290070",),
            ("2900702",),
            ("290070-2",),
            ("0123456",),
            ("12345",),
            (None,),
        ]

        self.assertEqual(
            self.evaluate(values, normalize_municipality_code(F.col("value"))),
            ["290070", "290070", "290070", "012345", None, None],
        )
        self.assertEqual(
            self.evaluate(values, is_valid_search_municipality_code(F.col("value"))),
            [True, False, False, False, False, False],
        )
        self.assertEqual(
            self.evaluate(values, is_valid_cnefe_municipality_code(F.col("value"))),
            [False, True, True, True, False, False],
        )

    def test_search_dataframe_adds_fields_without_losing_rows(self):
        dataframe = self.spark.createDataFrame(
            [
                ("Av. Antônio Carlos", "S/N", "JD. PRIMAVERA", "01234-567", None, "290070"),
                ("Rua B", "102 E", "BAIRRO", "1234567", "Casa 2", "12345"),
                ("Rua C", None, "Centro", "40000000", "SN", "292740"),
            ],
            (
                "consulta_logradouro",
                "consulta_numero",
                "consulta_bairro",
                "consulta_cep",
                "consulta_complemento",
                "consulta_municipio",
            ),
        )

        rows = normalize_search_dataframe(dataframe).orderBy("consulta_logradouro").collect()

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].logradouro_normalizado, "avenida antonio carlos")
        self.assertEqual(rows[0].numero_normalizado, "sn")
        self.assertEqual(rows[0].bairro_normalizado, "jardim primavera")
        self.assertEqual(rows[0].cep_normalizado, "01234567")
        self.assertTrue(rows[0].cep_valido)
        self.assertEqual(rows[0].municipio_normalizado, "290070")
        self.assertTrue(rows[0].municipio_valido)
        self.assertIsNone(rows[1].bairro_normalizado)
        self.assertFalse(rows[1].cep_valido)
        self.assertFalse(rows[1].municipio_valido)
        self.assertEqual(rows[2].numero_normalizado, "sn")

    def test_cnefe_dataframe_uses_modifier_and_all_complement_fields(self):
        columns = (
            "NOM_TIPO_SEGLOGR",
            "NOM_TITULO_SEGLOGR",
            "NOM_SEGLOGR",
            "NUM_ENDERECO",
            "DSC_MODIFICADOR",
            "DSC_LOCALIDADE",
            "CEP",
            "COD_MUNICIPIO",
            "NOM_COMP_ELEM1",
            "VAL_COMP_ELEM1",
            "NOM_COMP_ELEM2",
            "VAL_COMP_ELEM2",
            "NOM_COMP_ELEM3",
            "VAL_COMP_ELEM3",
            "NOM_COMP_ELEM4",
            "VAL_COMP_ELEM4",
            "NOM_COMP_ELEM5",
            "VAL_COMP_ELEM5",
        )
        schema = ", ".join(f"`{column}` string" for column in columns)
        dataframe = self.spark.createDataFrame(
            [
                (
                    "AV", None, "ANTÔNIO CARLOS", "0", "SN", "CENTRO",
                    "48.030-260", "2900702", "CASA", "2", None, None,
                    None, None, None, None, None, None,
                )
            ],
            schema=schema,
        )

        row = normalize_cnefe_dataframe(dataframe).first()

        self.assertEqual(row.logradouro_normalizado, "avenida antonio carlos")
        self.assertEqual(row.numero_normalizado, "sn")
        self.assertEqual(row.localidade_normalizada, "centro")
        self.assertEqual(row.cep_normalizado, "48030260")
        self.assertTrue(row.cep_valido)
        self.assertEqual(row.municipio_normalizado, "290070")
        self.assertTrue(row.municipio_valido)
        self.assertEqual(row.complemento_normalizado, "casa 2")


if __name__ == "__main__":
    unittest.main()
