"""Camada bronze: baixa os dados abertos de acidentes da PRF (2017 a 2025).

Uso: python -m rodovias.ingest

Fonte: https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf
Os arquivos ficam no Google Drive oficial da PRF; os IDs abaixo são os links
publicados nessa página. Cada download é registrado com tamanho e SHA-256 em
data/raw/manifest.json. Arquivos já baixados são reaproveitados.
"""

from __future__ import annotations

import hashlib
import json
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
DRIVE = "https://drive.usercontent.google.com/download?id={id}&export=download&confirm=t"

# Uma linha por acidente (ocorrência).
DATATRAN = {
    2017: "1HPLWt5f_l4RIX3tKjI4tUXyZOev52W0N",
    2018: "1cM4IgGMIiR-u4gBIH5IEe3DcvBvUzedi",
    2019: "1pN3fn2wY34GH6cY-gKfbxRJJBFE0lb_l",
    2020: "1esu6IiH5TVTxFoedv6DBGDd01Gvi8785",
    2021: "12xH8LX9aN2gObR766YN3cMcuycwyCJDz",
    2022: "1PRQjuV5gOn_nn6UNvaJyVURDIfbSAK4-",
    2023: "1-WO3SfNrwwZ5_l7fRTiwBKRw7mi1-HUq",
    2024: "14lB0vqMFkaZj8HZ44b0njYgxs9nAN8KO",
    2025: "1-G3MdmHBt6CprDwcW99xxC4BZ2DU5ryR",
}
# Uma linha por pessoa envolvida, causa e tipo de acidente.
PESSOAS = {
    2017: "1Kv5mNgZvxtl0xwqsmDrxcaLY2KELxR-3",
    2018: "1J-012nSnIafOASNFvIYY_vDKKpM51w5_",
    2019: "1DAJYKVfkTcPhQodSmHp9rsG1Q8XJW-m3",
    2020: "1yQtVOsAlupPHQVVTmbJo0NR3XMzgHANO",
    2021: "1Gk3U6cMOZIevsDZHLi6J503xoCRS_lnI",
    2022: "1wskEgRC3ame7rncSDQ7qWhKsoKw1lohY",
    2023: "1-caam_dahYOf2eorq4mez04Om6DD5d_3",
    2024: "14qBOhrE1gioVtuXgxkCJ9kCA8YtUGXKA",
    2025: "1-PJGRbfSe7PVjU37A3wTCls_NRXyVGRD",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(file_id: str, name: str) -> Path:
    dest = RAW / name
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  já existe: {name}")
        return dest
    print(f"  baixando {name}")
    req = urllib.request.Request(DRIVE.format(id=file_id), headers={"User-Agent": "acidentes-rodovias-federais"})
    tmp = dest.with_suffix(".part")
    with urllib.request.urlopen(req, timeout=180) as r, tmp.open("wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    if not zipfile.is_zipfile(tmp):
        tmp.unlink()
        raise RuntimeError(f"{name}: o download não é um ZIP (o link da PRF pode ter mudado)")
    tmp.replace(dest)
    return dest


def extract(zpath: Path, folder: Path) -> list[str]:
    folder.mkdir(parents=True, exist_ok=True)
    names = []
    with zipfile.ZipFile(zpath) as z:
        for info in z.infolist():
            name = Path(info.filename).name  # descarta caminhos internos (proteção contra zip slip)
            if not name.lower().endswith(".csv"):
                continue
            target = folder / name
            if not target.exists():
                with z.open(info) as src, target.open("wb") as dst:
                    dst.write(src.read())
            names.append(name)
    return names


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for kind, ids, template in [
        ("datatran", DATATRAN, "datatran{y}.zip"),
        ("pessoas", PESSOAS, "acidentes{y}_todas_causas_tipos.zip"),
    ]:
        for year, file_id in ids.items():
            z = fetch(file_id, template.format(y=year))
            manifest[z.name] = {
                "conjunto": kind,
                "ano": year,
                "drive_id": file_id,
                "bytes": z.stat().st_size,
                "sha256": sha256(z),
                "csv": extract(z, RAW / kind),
            }
    (RAW / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
                                       encoding="utf-8", newline="\n")
    print(f"Pronto: {len(manifest)} arquivos em {RAW}")


if __name__ == "__main__":
    main()
