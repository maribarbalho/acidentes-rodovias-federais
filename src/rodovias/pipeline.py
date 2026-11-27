"""Pipeline medalhão em Polars: bronze (CSV da PRF) → silver (Parquet tipado) → gold (JSON da página).

Uso: python -m rodovias.pipeline

- bronze: data/raw/{datatran,pessoas}/*.csv, como publicados pela PRF (latin-1, ";", decimal com vírgula)
- silver: data/silver/acidentes.parquet (1 linha por acidente) e data/silver/vitimas.parquet
  (1 linha por pessoa envolvida), com tipos, padronização e flags de qualidade
- gold:   docs/data/rodovias.json, os agregados usados na página
"""

from __future__ import annotations

import io
import json
import time
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
SILVER = ROOT / "data" / "silver"
OUT = ROOT / "docs" / "data"

# A PRF passou a usar uma lista de causas mais detalhada ao longo de 2020; de 2021 em
# diante a taxonomia é estável. Análises por causa, tipo e condição usam esse recorte.
ANOS_CAUSA = (2021, 2025)
BBOX_BRASIL = {"lat": (-34.0, 5.5), "lon": (-74.5, -32.0)}
DIAS = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]


# ----------------------------------------------------------------------------- bronze
def read_prf_csv(path: Path) -> pl.DataFrame:
    """Lê um CSV da PRF como texto (latin-1). Tipos ficam para a camada silver."""
    text = path.read_bytes().decode("latin-1")
    return pl.read_csv(io.StringIO(text), separator=";", infer_schema=False, null_values=["NA", ""])


def bronze(kind: str) -> pl.DataFrame:
    files = sorted((RAW / kind).glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"Nenhum CSV em {RAW / kind}. Rode python -m rodovias.ingest antes.")
    return pl.concat([read_prf_csv(f).with_columns(pl.lit(f.name).alias("arquivo")) for f in files], how="diagonal")


# ----------------------------------------------------------------------------- silver
def _num(col: str) -> pl.Expr:
    return pl.col(col).str.replace(",", ".").cast(pl.Float64, strict=False)


def _int(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Int64, strict=False)


def _txt(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars().str.replace_all(r"\s+", " ")


def silver_acidentes(df: pl.DataFrame) -> pl.DataFrame:
    out = df.select(
        # Alguns ids redondos vêm em notação científica no CSV ("1e+05" = 100000).
        pl.col("id").cast(pl.Float64, strict=False).cast(pl.Int64).alias("id"),
        pl.col("data_inversa").str.to_date("%Y-%m-%d", strict=False).alias("data"),
        _txt("dia_semana").str.to_lowercase().alias("dia_semana"),
        pl.col("horario").str.slice(0, 2).cast(pl.Int8, strict=False).alias("hora"),
        _txt("uf").alias("uf"),
        _int("br").alias("br"),
        _num("km").alias("km"),
        _txt("municipio").alias("municipio"),
        _txt("causa_acidente").alias("causa"),
        _txt("tipo_acidente").alias("tipo"),
        _txt("classificacao_acidente").alias("classificacao"),
        _txt("fase_dia").alias("fase_dia"),
        _txt("condicao_metereologica").alias("condicao_meteo"),
        _txt("tipo_pista").alias("tipo_pista"),
        # A partir de 2024 o traçado pode ter vários valores ("Reta;Declive"); fica o primeiro.
        _txt("tracado_via").str.split(";").list.first().alias("tracado"),
        (pl.col("uso_solo") == "Sim").alias("urbano"),
        *[_int(c).alias(c) for c in ["pessoas", "mortos", "feridos_leves", "feridos_graves", "ilesos", "veiculos"]],
        _num("latitude").alias("lat"),
        _num("longitude").alias("lon"),
    ).with_columns(
        pl.col("data").dt.year().alias("ano"),
        pl.col("data").dt.month().alias("mes"),
        (pl.col("lat").is_between(*BBOX_BRASIL["lat"]) & pl.col("lon").is_between(*BBOX_BRASIL["lon"]))
        .fill_null(False).alias("coord_valida"),
    )
    # Dedupe defensivo por id (a PRF publica um id por ocorrência).
    return out.unique(subset="id", keep="first").sort("id")


def silver_vitimas(df: pl.DataFrame) -> pl.DataFrame:
    """Uma linha por pessoa: o arquivo "todas as causas e tipos" repete cada pessoa
    para cada combinação de causa e tipo; fica a linha da causa principal."""
    out = df.select(
        pl.col("id").cast(pl.Float64, strict=False).cast(pl.Int64).alias("id_acidente"),
        pl.col("pesid").cast(pl.Float64, strict=False).cast(pl.Int64).alias("pesid"),
        pl.col("data_inversa").str.to_date("%Y-%m-%d", strict=False).dt.year().alias("ano"),
        _txt("uf").alias("uf"),
        (pl.col("causa_principal") == "Sim").alias("causa_principal"),
        _int("ordem_tipo_acidente").alias("ordem_tipo"),
        _txt("tipo_veiculo").alias("tipo_veiculo"),
        _txt("tipo_envolvido").alias("tipo_envolvido"),
        _txt("estado_fisico").alias("estado_fisico"),
        _int("idade").alias("idade"),
        _txt("sexo").alias("sexo"),
    )
    linhas = out.height
    out = (
        out.filter(pl.col("pesid").is_not_null())
        .sort(["pesid", "causa_principal", "ordem_tipo"], descending=[False, True, False])
        .unique(subset="pesid", keep="first")
        .with_columns(
            pl.when((pl.col("idade") < 0) | (pl.col("idade") > 110)).then(None).otherwise(pl.col("idade")).alias("idade"),
        )
        .drop(["causa_principal", "ordem_tipo"])
    )
    out = out.with_columns(pl.lit(linhas).alias("_linhas_bronze"))
    return out


# ----------------------------------------------------------------------------- gold
def _letalidade(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("mortos") / pl.col("acidentes") * 100).round(2).alias("mortos_por_100"),
    )


def _agg() -> list[pl.Expr]:
    return [
        pl.len().alias("acidentes"),
        pl.col("mortos").sum().alias("mortos"),
        pl.col("feridos_graves").sum().alias("feridos_graves"),
        (pl.col("mortos") > 0).sum().alias("acidentes_fatais"),
    ]


def gold(ac: pl.DataFrame, vi: pl.DataFrame) -> dict:
    recorte = ac.filter(pl.col("ano").is_between(*ANOS_CAUSA))

    anual = _letalidade(ac.group_by("ano").agg(_agg()).sort("ano"))

    causas = _letalidade(recorte.group_by("causa").agg(_agg())).filter(pl.col("acidentes") >= 300)
    tipos = _letalidade(recorte.group_by("tipo").agg(_agg())).filter(pl.col("acidentes") >= 300)

    condicoes = []
    for dim in ["fase_dia", "condicao_meteo", "tipo_pista", "tracado", "urbano"]:
        g = _letalidade(recorte.group_by(dim).agg(_agg())).filter(pl.col("acidentes") >= 300)
        condicoes += [{"dimensao": dim, "categoria": str(r[dim]), **{k: r[k] for k in ["acidentes", "mortos", "mortos_por_100"]}}
                      for r in g.iter_rows(named=True)]

    hora_dia = (
        recorte.filter(pl.col("hora").is_not_null())
        .group_by(["dia_semana", "hora"]).agg(_agg())
        .with_columns(pl.col("dia_semana").replace_strict({d: i for i, d in enumerate(DIAS)}, default=None).alias("dia_idx"))
        .sort(["dia_idx", "hora"])
    )

    # Trechos críticos: segmentos de 10 km de cada BR em cada UF.
    trechos = (
        recorte.filter(pl.col("br").is_not_null() & pl.col("km").is_not_null())
        .with_columns((pl.col("km") // 10 * 10).cast(pl.Int32).alias("km_ini"))
        .group_by(["uf", "br", "km_ini"])
        .agg(*_agg(), pl.col("lat").filter(pl.col("coord_valida")).median().alias("lat"),
             pl.col("lon").filter(pl.col("coord_valida")).median().alias("lon"),
             pl.col("municipio").mode().first().alias("municipio"))
        .pipe(_letalidade)
        .sort(["mortos", "acidentes"], descending=True)
    )

    por_uf = _letalidade(recorte.group_by("uf").agg(_agg()).sort("uf"))

    # Grade de 0,1° para o mapa (a malha das rodovias aparece pelos próprios pontos).
    mapa = (
        recorte.filter(pl.col("coord_valida"))
        .with_columns((pl.col("lat") * 10).round(0).cast(pl.Int32).alias("gy"),
                      (pl.col("lon") * 10).round(0).cast(pl.Int32).alias("gx"))
        .group_by(["gx", "gy"]).agg(pl.len().alias("a"), pl.col("mortos").sum().alias("m"))
        .sort(["gx", "gy"])
    )

    # Vítimas fatais (2021 a 2025), uma linha por pessoa.
    obitos = vi.filter(pl.col("ano").is_between(*ANOS_CAUSA) & (pl.col("estado_fisico") == "Óbito"))
    faixa = (
        pl.when(pl.col("idade") < 18).then(pl.lit("0 a 17"))
        .when(pl.col("idade") < 30).then(pl.lit("18 a 29"))
        .when(pl.col("idade") < 45).then(pl.lit("30 a 44"))
        .when(pl.col("idade") < 60).then(pl.lit("45 a 59"))
        .when(pl.col("idade").is_not_null()).then(pl.lit("60 ou mais"))
        .otherwise(pl.lit("Não informada"))
    )
    vitimas = {
        "total_obitos": obitos.height,
        "por_veiculo": obitos.group_by("tipo_veiculo").agg(pl.len().alias("obitos")).sort("obitos", descending=True).to_dicts(),
        "por_envolvido": obitos.group_by("tipo_envolvido").agg(pl.len().alias("obitos")).sort("obitos", descending=True).to_dicts(),
        "por_idade_sexo": obitos.with_columns(faixa.alias("faixa")).group_by(["faixa", "sexo"]).agg(pl.len().alias("obitos")).sort(["faixa", "sexo"]).to_dicts(),
    }

    qualidade = {
        "acidentes": ac.height,
        "anos": [int(ac["ano"].min()), int(ac["ano"].max())],
        "sem_coordenada_valida": int((~ac["coord_valida"]).sum()),
        "sem_br_ou_km": int(ac.filter(pl.col("br").is_null() | pl.col("km").is_null()).height),
        "sem_hora": int(ac["hora"].null_count()),
        "linhas_bronze_vitimas": int(vi["_linhas_bronze"][0]) if vi.height else 0,
        "pessoas_silver": vi.height,
        # Reconciliação: mortos do arquivo de ocorrências x pessoas com estado "Óbito".
        "mortos_ocorrencias": int(recorte["mortos"].sum()),
        "obitos_pessoas": obitos.height,
    }

    return {
        "meta": {"fonte": "PRF, dados abertos de acidentes (datatran e acidentes por pessoa)",
                 "anos_serie": [2017, 2025], "anos_recorte": list(ANOS_CAUSA)},
        "qualidade": qualidade,
        "anual": anual.to_dicts(),
        "causas": causas.sort("mortos", descending=True).to_dicts(),
        "tipos": tipos.sort("mortos", descending=True).to_dicts(),
        "condicoes": condicoes,
        "hora_dia": hora_dia.select(["dia_semana", "dia_idx", "hora", "acidentes", "mortos"]).to_dicts(),
        "trechos": trechos.head(40).to_dicts(),
        "por_uf": por_uf.to_dicts(),
        "mapa": {"passo": 0.1, "colunas": ["gx", "gy", "acidentes", "mortos"], "dados": mapa.rows()},
        "vitimas": vitimas,
    }


def main() -> None:
    t0 = time.time()
    SILVER.mkdir(parents=True, exist_ok=True)
    print("bronze → silver: acidentes")
    ac = silver_acidentes(bronze("datatran"))
    ac.write_parquet(SILVER / "acidentes.parquet")
    print(f"  {ac.height:,} acidentes")
    print("bronze → silver: vítimas")
    vi = silver_vitimas(bronze("pessoas"))
    vi.drop("_linhas_bronze").write_parquet(SILVER / "vitimas.parquet")
    print(f"  {vi.height:,} pessoas")
    print("silver → gold")
    g = gold(ac, vi)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "rodovias.json").write_text(json.dumps(g, ensure_ascii=False, separators=(",", ":"), default=str) + "\n",
                                       encoding="utf-8", newline="\n")
    print(f"Pronto em {time.time() - t0:.0f} s · {OUT / 'rodovias.json'}")


if __name__ == "__main__":
    main()
