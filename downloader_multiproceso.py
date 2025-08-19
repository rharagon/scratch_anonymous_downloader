"""
Downloader multiproceso (seguro, tolerante a fallos y rápido) para proyectos Scratch.
Mantiene la esencia del programa original: parámetro para indicar el **primer ID** desde el que descargar **de forma secuencial**.
Además, permite "aleatorio" como valor para empezar en un ID aleatorio.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import signal
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from typing import Iterable, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zipfile import ZipFile

import requests
from scratchclient import ScratchSession

# ----------------- Paths y constantes -----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_DIR = os.path.join(BASE_DIR, "downloads")
UTEMP_DIR = os.path.join(BASE_DIR, "utemp")
SESSION = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
SESSION_DIR = os.path.join(DOWNLOADS_DIR, SESSION)
SUMMARY_DIR = os.path.join(SESSION_DIR, "summaries")
SUCCESS_LIST = os.path.join(SUMMARY_DIR, "projects_downloaded")
FAILED_LIST = os.path.join(SUMMARY_DIR, "projects_failed")
DATASET_CSV_PATH = os.path.join(SESSION_DIR, "dataset.csv")

PROXIES = {"http": "socks5h://tor_proxy:9050", "https": "socks5h://tor_proxy:9050"}

# --------------- Logging padre -----------------
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(SUMMARY_DIR, exist_ok=True)
os.makedirs(UTEMP_DIR, exist_ok=True)

LOG_PATH = os.path.join(SUMMARY_DIR, "session.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# --------------- Args -----------------

def positive_int(v: str) -> int:
    try:
        iv = int(v)
        if iv <= 0:
            raise ValueError
        return iv
    except Exception:
        raise argparse.ArgumentTypeError("Debe ser entero > 0")


def non_negative_int(v: str) -> int:
    try:
        iv = int(v)
        if iv < 0:
            raise ValueError
        return iv
    except Exception:
        raise argparse.ArgumentTypeError("Debe ser entero >= 0")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Downloader multiproceso de proyectos Scratch (secuencial desde un ID)")
    p.add_argument("--start-id", type=str, default="", help="ID inicial (entero). Usa 'random' para empezar en un ID aleatorio")
    p.add_argument("--amount", type=non_negative_int, default=0, help="Nº de descargas EXITOSAS objetivo. 0 = ilimitado (solo con explore o start-id sin límite)")
    p.add_argument("--ids-file", type=str, default="", help="Fichero con IDs (uno por línea). Si se usa, ignora --start-id y explore")
    # Modo explore (opcional, por compatibilidad con versiones previas)
    p.add_argument("--query", type=str, default="*", help="Búsqueda para explore (si no hay --ids-file ni --start-id)")
    p.add_argument("--mode", type=str, choices=["popular", "trending", "recent"], default="popular", help="Modo explore")
    p.add_argument("--language", type=str, default="en", help="Idioma explore")
    p.add_argument("--workers", type=positive_int, default=os.cpu_count() or 4, help="Procesos (por defecto = núm. CPU)")
    p.add_argument("--retry", type=positive_int, default=1, help="Reintentos por proyecto")
    p.add_argument("--timeout", type=positive_int, default=1, help="Timeout de red (s)")
    p.add_argument("--no-tor", action="store_true", help="No usar proxy TOR en explore")
    return p


# --------------- Utilidades padre -----------------

def append_line(path: str, line: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def init_csv(path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Title", "Project ID", "Author", "Creation date", "Modified date", "Remix parent id", "Remix root id"])


def append_csv_row(path: str, row: list[str]) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(row)


# --------------- Productores de IDs -----------------

def ids_from_file(path: str) -> Iterable[int]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                yield int(line)
            except ValueError:
                logging.warning(f"ID inválido en {path}: {line}")


def ids_from_start(start_id: int, amount: int) -> Iterable[int]:
    """Genera IDs secuenciales desde start_id. Si amount=0, genera indefinidamente."""
    current = start_id
    produced = 0
    while True:
        yield current
        current += 1
        produced += 1
        if amount and produced >= amount:
            return


def ids_from_explore(query: str, mode: str, language: str, amount: int, timeout: int, use_tor: bool) -> Iterable[int]:
    base = "https://api.scratch.mit.edu/explore/projects"
    proxies = PROXIES if use_tor else None
    limit = 40
    offset = 0
    yielded = 0
    backoff = 0.75

    while True:
        url = f"{base}?q={requests.utils.quote(query)}&mode={mode}&language={language}&limit={limit}&offset={offset}"
        try:
            r = requests.get(url, timeout=timeout, proxies=proxies)
            r.raise_for_status()
            arr = r.json() or []
            if not isinstance(arr, list):
                raise ValueError("Respuesta inesperada de explore")
            if not arr:
                offset = 0
                time.sleep(1.0)
                continue
            for proj in arr:
                try:
                    pid = int(str(proj.get("id", "")).strip())
                except Exception:
                    continue
                yield pid
                yielded += 1
                if amount and yielded >= amount:
                    return
            offset += 30  # paso conservador
            if offset > 9900:
                offset = 0
        except (requests.Timeout, requests.RequestException, ValueError) as e:
            logging.warning(f"Explore error: {e}")
            time.sleep(backoff)
            backoff = min(8.0, backoff * 2)


# --------------- Worker (proceso hijo) -----------------
# Nota: debe ser función de nivel superior para ser "picklable" por multiprocessing

def worker_download_pack(project_id: int, timeout: int, retry: int, session_dir: str, utemp_dir: str) -> Tuple[int, bool, Optional[list[str]], Optional[str]]:
    """Descarga JSON del proyecto y lo empaqueta como .sb3.
    Devuelve: (id, success, csv_row, error_msg)
    El CSV se rellena con mejor esfuerzo vía ScratchSession.
    """
    import consts_scratch as consts  # mantiene esencia

    last_err: Optional[Exception] = None
    backoff = 0.75

    for attempt in range(1, retry + 1):
        try:
            # 1) obtener URL (token o fallback)
            sess = ScratchSession()
            try:
                info = sess.get_project(project_id)
                url = f"{consts.URL_SCRATCH_SERVER}/{project_id}?token={info.project_token}"
            except Exception:
                url = f"{consts.URL_GETSB3}/{project_id}"
                info = None

            # 2) descargar
            with urlopen(url, timeout=timeout) as resp:
                status = getattr(resp, "getcode", lambda: 200)()
                if status != 200:
                    raise RuntimeError(f"HTTP {status}")
                raw = resp.read()

            # 3) validar JSON
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("latin-1", errors="replace")
            if not text.strip() or text.lstrip()[:1] not in ("{", "["):
                raise ValueError("Respuesta no JSON")
            json.loads(text)

            # 4) escribir .sb3 (zip con project.json) en directorio de sesión
            out_path = os.path.join(session_dir, f"{project_id}.sb3")
            with tempfile.TemporaryDirectory(dir=utemp_dir) as tmp:
                pj = os.path.join(tmp, "project.json")
                with open(pj, "wb") as f:
                    f.write(text.encode("utf-8"))
                with ZipFile(out_path, "w") as zf:
                    zf.write(pj, arcname="project.json")

            # 5) preparar fila CSV (best-effort)
            row = None
            try:
                meta = info or sess.get_project(project_id)
                title = str(getattr(meta, "title", "")).replace(",", " ")
                author = str(getattr(meta, "author", "")).replace(",", " ")
                creation = getattr(meta, "creation_date", "")
                modified = getattr(meta, "modified_date", "")
                remix_parent = getattr(meta, "remix_parent", "")
                remix_root = getattr(meta, "remix_root", "")
                row = [title, str(project_id), author, creation, modified, remix_parent, remix_root]
            except Exception:
                pass

            return (project_id, True, row, None)
        except (HTTPError, URLError, ValueError, RuntimeError) as e:
            last_err = e
            if attempt < retry:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
            else:
                break
        except Exception as e:
            last_err = e
            break

    return (project_id, False, None, str(last_err) if last_err else "unknown error")


# --------------- Main (padre) -----------------

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    init_csv(DATASET_CSV_PATH)
    # Fuente de IDs según prioridad: ids-file > start-id > explore
    if args.ids_file:
        id_iter = ids_from_file(args.ids_file)
        max_total = args.amount if args.amount else None
    elif args.start_id:
        start_str = args.start_id.strip().lower()
        if start_str in ("random", "rand", "aleatorio"):
            start_id = random.randint(500_000_000, 1_500_000_000)
            logging.info(f"Usando start-id ALEATORIO: {start_id}")
        else:
            try:
                start_id = int(start_str)
            except ValueError:
                raise SystemExit("--start-id debe ser un entero o la palabra 'random'")
        id_iter = ids_from_start(start_id, args.amount if args.amount else 0)
        max_total = None
    else:
        id_iter = ids_from_explore(args.query, args.mode, args.language, args.amount, args.timeout, not args.no_tor)
        max_total = args.amount if args.amount else None

    workers = max(1, min(args.workers, 256))
    logging.info(f"Multiproceso: {workers} procesos")

    submitted = 0
    futures = []
    start = time.time()

    stop = False
    def handle_sigint(signum, frame):
        nonlocal stop
        stop = True
        logging.warning("Señal recibida, esperando a que terminen los trabajos en curso…")
    try:
        signal.signal(signal.SIGINT, handle_sigint)
        signal.signal(signal.SIGTERM, handle_sigint)
    except Exception:
        pass

    with ProcessPoolExecutor(max_workers=workers) as ex:
        try:
            window = max(1, workers * 4)
            futures = {}
            successes = 0
            submitted = 0
            id_exhausted = False

            def submit_more(n:int):
                nonlocal submitted, id_exhausted
                for _ in range(n):
                    if stop or id_exhausted:
                        return
                    try:
                        pid = next(id_iter)
                    except StopIteration:
                        id_exhausted = True
                        return
                    futures[ex.submit(worker_download_pack, int(pid), args.timeout, args.retry, SESSION_DIR, UTEMP_DIR)] = pid
                    submitted += 1

            # Prefill
            submit_more(window)

            # Consume until we reach target successes (if any), or no more work
            while futures and not stop and (args.amount == 0 or successes < args.amount):
                for f in as_completed(list(futures.keys())):
                    pid = futures.pop(f)
                    project_id, ok, row, err = f.result()
                    if ok:
                        append_line(SUCCESS_LIST, str(project_id))
                        if row:
                            append_csv_row(DATASET_CSV_PATH, row)
                        logging.info(f"[{project_id}] OK")
                        successes += 1
                    else:
                        append_line(FAILED_LIST, str(project_id))
                        logging.warning(f"[{project_id}] FAIL: {err}")

                    # Si ya alcanzamos el objetivo de éxitos, dejamos de enviar más
                    if stop or (args.amount and successes >= args.amount):
                        continue

                    # Rellenar cola para mantener ventana
                    if not id_exhausted:
                        need = max(0, window - len(futures))
                        if need:
                            submit_more(need)

                # Si no quedan futures pero aún necesitamos más éxitos y quedan IDs, volver a rellenar
                if not futures and not id_exhausted and (args.amount == 0 or successes < args.amount):
                    submit_more(window)

            # Cancelar trabajos en vuelo si se alcanzó objetivo
            if args.amount and successes >= args.amount:
                logging.info(f"Objetivo de descargas exitosas alcanzado: {successes}")
        except KeyboardInterrupt:
            logging.warning("Interrupción por teclado. Cancelando tareas pendientes…")

    elapsed = time.time() - start
    ok_count = sum(1 for _ in open(SUCCESS_LIST, "r", encoding="utf-8")) if os.path.exists(SUCCESS_LIST) else 0
    fail_count = sum(1 for _ in open(FAILED_LIST, "r", encoding="utf-8")) if os.path.exists(FAILED_LIST) else 0

    logging.info(
        f"""
###############################################################
##
##   SESSION {SESSION} SUMMARY
##   - {ok_count} projects downloaded.
##   - {fail_count} projects failed.
##   - Took {elapsed:.2f} seconds.
##
##   Downloads: {SESSION_DIR}
##   Summaries: {SUMMARY_DIR}
##
#################################################################
"""
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"ERROR FATAL: {e}")
        sys.exit(1)
