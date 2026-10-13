"""Funcoes de normalizacao das bases de busca e CNEFE.

As funcoes deste modulo constroem expressoes PySpark. Elas preservam as
colunas originais e acrescentam colunas normalizadas para comparacao.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


ACCENTED = "áàâãäéèêëíìîïóòôõöúùûüç"
UNACCENTED = "aaaaaeeeeiiiiooooouuuuc"

STREET_PREFIXES = (
    (r"^r\s+", "rua "),
    (r"^av\s+", "avenida "),
    (r"^rod\s+", "rodovia "),
    (r"^tv\s+", "travessa "),
    (r"^pc\s+", "praca "),
    (r"^estr\s+", "estrada "),
    (r"^al\s+", "alameda "),
)

NEIGHBORHOOD_PREFIXES = (
    (r"^jd\s+", "jardim "),
    (r"^jdim\s+", "jardim "),
    (r"^vl\s+", "vila "),
    (r"^pq\s+", "parque "),
    (r"^cj\s+", "conjunto "),
    (r"^conj\s+", "conjunto "),
    (r"^res\s+", "residencial "),
    (r"^lot\s+", "loteamento "),
    (r"^dist\s+", "distrito "),
    (r"^pov\s+", "povoado "),
)

NO_NUMBER_MARKERS = (
    "sn",
    "s n",
    "sem numero",
    "sem nro",
)

NULL_MARKERS = (
    "none",
    "null",
    "nan",
    "n a",
)


def _null_string() -> Column:
    """Retorna um literal nulo tipado como string."""
    return F.lit(None).cast("string")


def _null_if_empty(column: Column) -> Column:
    """Converte strings vazias e marcadores de ausencia em nulo."""
    return F.when(
        column.isNull()
        | (column == "")
        | column.isin(*NULL_MARKERS),
        _null_string(),
    ).otherwise(column)


def normalize_text(column: Column) -> Column:
    """Normaliza caixa, acentos, pontuacao e espacos de uma coluna textual."""
    value = F.lower(F.trim(column.cast("string")))
    value = F.translate(value, ACCENTED, UNACCENTED)
    value = F.regexp_replace(value, r"[^a-z0-9 ]", " ")
    value = F.regexp_replace(value, r"\s+", " ")
    value = F.trim(value)

    return _null_if_empty(value)


def normalize_street(column: Column) -> Column:
    """Normaliza um logradouro e expande abreviacoes no inicio do texto."""
    value = normalize_text(column)
    value = F.regexp_replace(value, r"^(?:none|null|nan)\s+", "")

    for pattern, replacement in STREET_PREFIXES:
        value = F.regexp_replace(value, pattern, replacement)

    return _null_if_empty(F.trim(value))


def normalize_neighborhood(column: Column) -> Column:
    """Normaliza bairro/localidade sem remover termos que distinguem lugares."""
    value = normalize_text(column)
    value = F.regexp_replace(value, r"^bairro(?:\s+|$)", "")

    for pattern, replacement in NEIGHBORHOOD_PREFIXES:
        value = F.regexp_replace(value, pattern, replacement)

    value = F.regexp_replace(value, r"\s+", " ")
    value = F.trim(value)

    return _null_if_empty(value)


def normalize_number(
    number_column: Column,
    modifier_column: Column | None = None,
) -> Column:
    """Normaliza numero, preserva alfanumericos e identifica sem-numero."""
    value = normalize_text(number_column)
    compact_value = F.regexp_replace(value, r"\s+", "")
    is_without_number = value.isin(*NO_NUMBER_MARKERS)

    if modifier_column is not None:
        modifier = normalize_text(modifier_column)
        is_without_number = is_without_number | modifier.isin(
            *NO_NUMBER_MARKERS
        )

    return (
        F.when(is_without_number, F.lit("sn"))
        .when(value.isNull(), _null_string())
        .otherwise(compact_value)
    )


def normalize_complement(column: Column) -> Column:
    """Normaliza complemento de forma conservadora."""
    return normalize_text(column)


def only_digits(column: Column) -> Column:
    """Remove caracteres nao numericos sem converter o valor para numero."""
    return F.regexp_replace(
        F.trim(column.cast("string")),
        r"[^0-9]",
        "",
    )


def normalize_postal_code(column: Column) -> Column:
    """Retorna um CEP de oito digitos ou nulo quando o formato for invalido."""
    value = only_digits(column)

    return F.when(
        value.isNotNull() & (F.length(value) == 8),
        value,
    ).otherwise(_null_string())


def is_valid_postal_code(column: Column) -> Column:
    """Indica se o CEP possui exatamente oito digitos."""
    value = only_digits(column)

    return value.isNotNull() & (F.length(value) == 8)


def normalize_municipality_code(column: Column) -> Column:
    """Cria a chave municipal comum de seis digitos.

    A base de busca usa seis digitos. O CNEFE usa os mesmos seis digitos mais
    um digito verificador; nesse caso, os seis primeiros sao preservados.
    """
    value = only_digits(column)
    value_length = F.length(value)

    return (
        F.when(value_length == 6, value)
        .when(value_length == 7, F.substring(value, 1, 6))
        .otherwise(_null_string())
    )


def is_valid_search_municipality_code(column: Column) -> Column:
    """Valida o codigo municipal de seis digitos da base de busca."""
    value = only_digits(column)
    return value.isNotNull() & (F.length(value) == 6)


def is_valid_cnefe_municipality_code(column: Column) -> Column:
    """Valida o codigo municipal completo de sete digitos do CNEFE."""
    value = only_digits(column)
    return value.isNotNull() & (F.length(value) == 7)


def normalize_search_dataframe(dataframe: DataFrame) -> DataFrame:
    """Acrescenta campos normalizados a base de busca."""
    return (
        dataframe.withColumn(
            "logradouro_normalizado",
            normalize_street(F.col("consulta_logradouro")),
        )
        .withColumn(
            "numero_normalizado",
            normalize_number(
                F.col("consulta_numero"),
                F.col("consulta_complemento"),
            ),
        )
        .withColumn(
            "bairro_normalizado",
            normalize_neighborhood(F.col("consulta_bairro")),
        )
        .withColumn(
            "cep_normalizado",
            normalize_postal_code(F.col("consulta_cep")),
        )
        .withColumn(
            "cep_valido",
            is_valid_postal_code(F.col("consulta_cep")),
        )
        .withColumn(
            "municipio_normalizado",
            normalize_municipality_code(F.col("consulta_municipio")),
        )
        .withColumn(
            "municipio_valido",
            is_valid_search_municipality_code(
                F.col("consulta_municipio")
            ),
        )
        .withColumn(
            "complemento_normalizado",
            normalize_complement(F.col("consulta_complemento")),
        )
    )


def normalize_cnefe_dataframe(dataframe: DataFrame) -> DataFrame:
    """Acrescenta campos normalizados ao CNEFE sem alterar os originais."""
    street = F.concat_ws(
        " ",
        F.col("NOM_TIPO_SEGLOGR"),
        F.col("NOM_TITULO_SEGLOGR"),
        F.col("NOM_SEGLOGR"),
    )
    complement = F.concat_ws(
        " ",
        F.col("NOM_COMP_ELEM1"),
        F.col("VAL_COMP_ELEM1"),
        F.col("NOM_COMP_ELEM2"),
        F.col("VAL_COMP_ELEM2"),
        F.col("NOM_COMP_ELEM3"),
        F.col("VAL_COMP_ELEM3"),
        F.col("NOM_COMP_ELEM4"),
        F.col("VAL_COMP_ELEM4"),
        F.col("NOM_COMP_ELEM5"),
        F.col("VAL_COMP_ELEM5"),
    )

    return (
        dataframe.withColumn(
            "logradouro_normalizado",
            normalize_street(street),
        )
        .withColumn(
            "numero_normalizado",
            normalize_number(
                F.col("NUM_ENDERECO"),
                F.col("DSC_MODIFICADOR"),
            ),
        )
        .withColumn(
            "localidade_normalizada",
            normalize_neighborhood(F.col("DSC_LOCALIDADE")),
        )
        .withColumn(
            "cep_normalizado",
            normalize_postal_code(F.col("CEP")),
        )
        .withColumn(
            "cep_valido",
            is_valid_postal_code(F.col("CEP")),
        )
        .withColumn(
            "municipio_normalizado",
            normalize_municipality_code(F.col("COD_MUNICIPIO")),
        )
        .withColumn(
            "municipio_valido",
            is_valid_cnefe_municipality_code(
                F.col("COD_MUNICIPIO")
            ),
        )
        .withColumn(
            "complemento_normalizado",
            normalize_complement(complement),
        )
    )
