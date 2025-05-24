<h2 align="center">Inauthentic trends</h2>

<p align="center">
<a href="https://www.python.org/"><img alt="code" src="https://img.shields.io/badge/Python-3.12-blue?logo=Python"></a>
<a href="https://https://pola.rs/"><img alt="code" src="https://img.shields.io/badge/Polars-1.29-white?logo=Polars"></a>
<a href="https://https://pola.rs/"><img alt="code" src="https://img.shields.io/badge/packaging-uv-pink?logo=Uv"></a>
</p>

## Folder overview

```
./src
├── mango_blog
│   ├── analysis.py     | main analysis routine producing the figures
│   ├── app_marimo.py   | marimo app
│   ├── app.py          | Shiny app (dashboard)
│   ├── constants.py    | constant variables
│   ├── hashtags.py     | main analysis functions
│   └── plots.py        | plotting functions
```

## Set up

Install the code
```
pip install -e .
```

## Run the analysis

```python
python -m mango_blog.analysis # stores results to ./data/outputs
```
