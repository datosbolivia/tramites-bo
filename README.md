Datos de trámites en entidades del gobierno boliviano tomados directamente del [portal oficial](https://gob.bo) y publicados como datos abiertos y de libre uso.

Incluye:

- [Datos de cada trámite en su forma original en formato jsonl](tramites.jsonl)
  - Especificación de los datos en [datapackage.json](datapackage.json)
- [Una bitácora de trámites que aparecen o desaparecen en corridas consecutivas](adiciones.csv)
- [Una bitácora de trámites que cambian entre corridas consecutivas](modificaciones.csv)

Estos datos son actualizados semanalmente cada domingo.

Puedes inspeccionar cómo exactamente estos datos son descargados y correr tu propia versión localmente:


```sh
pip install requests httpx[http2] asyncio tqdm jsonlines pandas
python update.py
```
