import json
from pathlib import Path

import polars as pl
import pytest

from rodovias.pipeline import silver_acidentes, silver_vitimas

GOLD = Path(__file__).resolve().parents[1] / "docs" / "data" / "rodovias.json"

ACIDENTE = {
    "id": "1e+05", "data_inversa": "2024-05-12", "dia_semana": "Domingo ", "horario": "19:30:00",
    "uf": "PR", "br": "277", "km": "12,5", "municipio": " CURITIBA ", "causa_acidente": "Transitar na contramão",
    "tipo_acidente": "Colisão frontal", "classificacao_acidente": "Com Vítimas Fatais", "fase_dia": "Plena Noite",
    "sentido_via": "Crescente", "condicao_metereologica": "Céu Claro", "tipo_pista": "Simples",
    "tracado_via": "Reta;Declive", "uso_solo": "Não", "pessoas": "3", "mortos": "2", "feridos_leves": "1",
    "feridos_graves": "0", "ilesos": "0", "ignorados": "0", "feridos": "1", "veiculos": "2",
    "latitude": "-25,43", "longitude": "-49,27",
}


def test_silver_acidentes_trata_formatos_da_prf():
    df = pl.DataFrame([ACIDENTE, {**ACIDENTE, "id": "7", "latitude": "-99,0", "br": None, "km": None}])
    s = silver_acidentes(df).sort("id")
    r = s.filter(pl.col("id") == 100000).row(0, named=True)
    assert r["km"] == 12.5 and r["lat"] == -25.43                # vírgula decimal
    assert r["dia_semana"] == "domingo" and r["municipio"] == "CURITIBA"
    assert r["hora"] == 19 and r["ano"] == 2024 and r["urbano"] is False
    assert r["tracado"] == "Reta"                                 # traçado com vários valores
    assert r["coord_valida"] is True
    assert s.filter(pl.col("id") == 7)["coord_valida"].item() is False


def test_silver_acidentes_remove_ids_duplicados():
    df = pl.DataFrame([ACIDENTE, ACIDENTE])
    assert silver_acidentes(df).height == 1


def test_silver_vitimas_uma_linha_por_pessoa_na_causa_principal():
    base = {"id": "1", "data_inversa": "2023-01-01", "uf": "SP", "tipo_veiculo": "Motocicleta",
            "tipo_envolvido": "Condutor", "estado_fisico": "Óbito", "idade": "34", "sexo": "Masculino"}
    df = pl.DataFrame([
        {**base, "pesid": "10", "causa_principal": "Não", "ordem_tipo_acidente": "1"},
        {**base, "pesid": "10", "causa_principal": "Sim", "ordem_tipo_acidente": "2"},
        {**base, "pesid": "10", "causa_principal": "Sim", "ordem_tipo_acidente": "1"},
        {**base, "pesid": "11", "causa_principal": "Sim", "ordem_tipo_acidente": "1", "idade": "250"},
    ])
    v = silver_vitimas(df)
    assert v.height == 2
    assert v.filter(pl.col("pesid") == 11)["idade"].item() is None  # idade impossível vira nulo


@pytest.fixture(scope="module")
def gold():
    if not GOLD.exists():
        pytest.skip("Rode python -m rodovias.pipeline antes.")
    return json.loads(GOLD.read_text(encoding="utf-8"))


def test_obitos_reconciliam_entre_arquivos(gold):
    q = gold["qualidade"]
    assert q["mortos_ocorrencias"] == q["obitos_pessoas"]


def test_series_e_agregados_consistentes(gold):
    anos = [r["ano"] for r in gold["anual"]]
    assert anos == list(range(2017, 2026))
    assert sum(r["acidentes"] for r in gold["anual"]) == gold["qualidade"]["acidentes"]
    recorte = [r for r in gold["anual"] if 2021 <= r["ano"] <= 2025]
    assert sum(r["mortos"] for r in recorte) == sum(r["mortos"] for r in gold["por_uf"])
    assert sum(c[3] for c in gold["mapa"]["dados"]) <= gold["qualidade"]["mortos_ocorrencias"]
