import base64
import csv
import io
import json
import mimetypes
import os
import zipfile
from defusedxml import ElementTree
from typing import Any

from pypdf import PdfReader

MAX_UPLOAD_BYTES_CONFIG = int(
    os.environ.get("VETRA_MAX_UPLOAD_BYTES", 10 * 1024 * 1024)
)
MAX_BACKEND_ATTACHMENT_BYTES = int(
    os.environ.get("VETRA_MAX_ATTACHMENT_BYTES", MAX_UPLOAD_BYTES_CONFIG)
)
MAX_UPLOAD_BYTES = min(MAX_UPLOAD_BYTES_CONFIG, MAX_BACKEND_ATTACHMENT_BYTES)
MAX_PDF_PAGES = int(os.environ.get("VETRA_MAX_PDF_PAGES", "500"))
MAX_ATTACHMENT_TEXT_CHARS = int(
    os.environ.get("VETRA_MAX_ATTACHMENT_TEXT_CHARS", "100000")
)
MAX_ARCHIVE_UNPACKED_BYTES = 50 * 1024 * 1024
MAX_TABLE_ROWS = 1000
MAX_TABLE_COLUMNS = 100
MAX_TABLE_CELLS = 20_000
if min(MAX_UPLOAD_BYTES, MAX_PDF_PAGES, MAX_ATTACHMENT_TEXT_CHARS) < 1:
    raise ValueError("Os limites de anexos devem ser maiores que zero.")

MIME_BY_EXTENSION = {
    ".aac": "audio/aac",
    ".aiff": "audio/aiff",
    ".avi": "video/avi",
    ".csv": "text/csv",
    ".flac": "audio/flac",
    ".flv": "video/x-flv",
    ".heic": "image/heic",
    ".heif": "image/heif",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".mov": "video/mov",
    ".mp3": "audio/mpeg",
    ".mp4": "video/mp4",
    ".mpeg": "video/mpeg",
    ".mpg": "video/mpg",
    ".ogg": "audio/ogg",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".wav": "audio/wav",
    ".webm": "video/webm",
    ".webp": "image/webp",
    ".wmv": "video/wmv",
    ".3gp": "video/3gpp",
}

TEXT_EXTENSIONS = {
    ".cfg",
    ".conf",
    ".css",
    ".csv",
    ".go",
    ".html",
    ".ini",
    ".ipynb",
    ".java",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".py",
    ".r",
    ".rb",
    ".rst",
    ".rs",
    ".scala",
    ".sh",
    ".sql",
    ".swift",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
    ".zsh",
    ".ps1",
    ".bat",
    ".cmd",
    ".dockerfile",
    ".tf",
    ".properties",
    ".log",
    ".tsv",
    ".c",
    ".cc",
    ".cpp",
    ".h",
    ".hpp",
    ".cs",
    ".graphql",
    ".php",
    ".pl",
    ".prisma",
    ".proto",
    ".m",
    ".jsonl",
}
TEXT_FILENAMES = {"dockerfile", "makefile", "procfile"}

SUPPORTED_UPLOAD_EXTENSIONS = {
    *TEXT_EXTENSIONS,
    ".docx",
    ".xlsx",
    ".pptx",
    ".parquet",
    ".pdf",
    *MIME_BY_EXTENSION.keys(),
}

_MULTIMODAL_EXTENSIONS = set(MIME_BY_EXTENSION) - {
    ".csv",
    ".pdf",
}
CHAT_ONLY_EXTENSIONS = frozenset(_MULTIMODAL_EXTENSIONS)


def _read_uploaded_file(uploaded_file: Any) -> tuple[str, bytes]:
    filename = os.path.basename(str(uploaded_file.name))
    if len(filename) > 255:
        raise ValueError("O nome do arquivo excede 255 caracteres.")
    extension = os.path.splitext(filename)[1].lower()
    if (
        extension not in SUPPORTED_UPLOAD_EXTENSIONS
        and filename.casefold() not in TEXT_FILENAMES
    ):
        raise ValueError(f"Formato não suportado: '{extension or 'sem extensão'}'.")
    uploaded_file.seek(0)
    data = uploaded_file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValueError(
            f"O arquivo '{filename}' excede o limite de "
            f"{MAX_UPLOAD_BYTES // (1024 * 1024)} MB."
        )
    return filename, data


def _decode_text(data: bytes, filename: str) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Não foi possível identificar a codificação de '{filename}'.")


def _validate_office_archive(data: bytes, filename: str) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            unpacked_size = sum(entry.file_size for entry in entries)
            if len(entries) > 5000 or unpacked_size > MAX_ARCHIVE_UNPACKED_BYTES:
                raise ValueError(
                    f"O arquivo '{filename}' excede os limites de conteúdo "
                    "descompactado para documentos Office."
                )
    except zipfile.BadZipFile as exc:
        raise ValueError(f"O arquivo Office '{filename}' está corrompido.") from exc


def _extract_docx(data: bytes, filename: str) -> str:
    _validate_office_archive(data, filename)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        try:
            document_xml = archive.read("word/document.xml")
        except KeyError as exc:
            raise ValueError(f"O arquivo DOCX '{filename}' não contém documento.") from exc
    root = ElementTree.fromstring(document_xml)
    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    return "\n".join(
        "".join(node.text or "" for node in paragraph.iter(f"{ns}t"))
        for paragraph in root.iter(f"{ns}p")
    )


def _extract_xlsx(data: bytes, filename: str) -> str:
    _validate_office_archive(data, filename)
    from openpyxl import load_workbook

    workbook = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    blocks = []
    cells_read = 0
    try:
        for sheet in workbook.worksheets[:50]:
            blocks.append(f"[Planilha: {sheet.title}]")
            for row in sheet.iter_rows(
                max_row=MAX_TABLE_ROWS,
                max_col=MAX_TABLE_COLUMNS,
                values_only=True,
            ):
                values = ["" if value is None else str(value) for value in row]
                while values and not values[-1]:
                    values.pop()
                if values:
                    blocks.append("\t".join(values))
                cells_read += len(row)
                if cells_read >= MAX_TABLE_CELLS:
                    blocks.append("[Conteúdo limitado a 20.000 células.]")
                    return "\n".join(blocks)
    finally:
        workbook.close()
    return "\n".join(blocks)


def _extract_pptx(data: bytes, filename: str) -> str:
    _validate_office_archive(data, filename)
    from pptx import Presentation

    presentation = Presentation(io.BytesIO(data))
    blocks = []
    for slide_number, slide in enumerate(presentation.slides, start=1):
        if slide_number > 200:
            blocks.append("[Conteúdo limitado às primeiras 200 páginas.]")
            break
        blocks.append(f"[Slide {slide_number}]")
        blocks.extend(
            shape.text
            for shape in slide.shapes
            if getattr(shape, "has_text_frame", False) and shape.text.strip()
        )
    return "\n".join(blocks)


def _extract_parquet(data: bytes) -> str:
    import pyarrow.parquet as parquet

    parquet_file = parquet.ParquetFile(io.BytesIO(data))
    batches = []
    for batch in parquet_file.iter_batches(
        batch_size=250,
        columns=parquet_file.schema.names[:MAX_TABLE_COLUMNS],
    ):
        batches.append(batch.to_pandas().to_csv(index=False))
        if sum(len(part) for part in batches) >= MAX_ATTACHMENT_TEXT_CHARS:
            break
    return "\n".join(batches)


def _extract_pdf(data: bytes) -> str:
    reader = PdfReader(io.BytesIO(data))
    if len(reader.pages) > MAX_PDF_PAGES:
        raise ValueError(f"O PDF excede o limite de {MAX_PDF_PAGES} páginas.")
    text = "\n".join(page.extract_text() or "" for page in reader.pages).strip()
    if text:
        return text
    try:
        import pypdfium2 as pdfium
        import pytesseract
    except ImportError:
        return ""

    document = pdfium.PdfDocument(data)
    blocks = []
    try:
        for index, page in enumerate(document):
            if index >= min(len(document), 20):
                blocks.append("[OCR limitado às primeiras 20 páginas.]")
                break
            image = page.render(scale=1.25).to_pil()
            recognized = pytesseract.image_to_string(
                image,
                lang=os.environ.get("VETRA_OCR_LANG", "por+eng"),
            )
            if recognized.strip():
                blocks.append(recognized)
    except pytesseract.TesseractNotFoundError as exc:
        raise ValueError(
            "O OCR de PDF digitalizado requer o mecanismo Tesseract "
            "instalado no sistema."
        ) from exc
    return "\n".join(blocks).strip()


def _extract_image(data: bytes) -> str:
    try:
        from PIL import Image
        import pytesseract
    except ImportError as exc:
        raise ValueError("O suporte a OCR de imagens não está instalado.") from exc
    try:
        with Image.open(io.BytesIO(data)) as image:
            if image.width * image.height > 40_000_000:
                raise ValueError(
                    "A imagem excede o limite de 40 megapixels para OCR."
                )
            return pytesseract.image_to_string(
                image,
                lang=os.environ.get("VETRA_OCR_LANG", "por+eng"),
            ).strip()
    except pytesseract.TesseractNotFoundError as exc:
        raise ValueError(
            "O OCR requer o mecanismo Tesseract instalado no sistema."
        ) from exc


def _extract_text(data: bytes, filename: str) -> str:
    extension = os.path.splitext(filename)[1].lower()
    if extension == ".docx":
        return _extract_docx(data, filename)
    if extension == ".xlsx":
        return _extract_xlsx(data, filename)
    if extension == ".pptx":
        return _extract_pptx(data, filename)
    if extension == ".parquet":
        return _extract_parquet(data)
    if extension == ".pdf":
        return _extract_pdf(data)
    if extension in {".png", ".jpg", ".jpeg", ".webp"}:
        return _extract_image(data)
    if extension == ".ipynb":
        notebook = json.loads(_decode_text(data, filename))
        cells = notebook.get("cells", [])
        return "\n".join(
            "".join(cell.get("source", []))
            for cell in cells
            if cell.get("cell_type") in {"markdown", "code"}
        )
    if extension in {".csv", ".tsv"}:
        delimiter = "\t" if extension == ".tsv" else ","
        rows = csv.reader(io.StringIO(_decode_text(data, filename)), delimiter=delimiter)
        return "\n".join(
            "\t".join(row[:MAX_TABLE_COLUMNS])
            for _, row in zip(range(MAX_TABLE_ROWS), rows)
        )
    if extension in _MULTIMODAL_EXTENSIONS:
        raise ValueError(
            f"'{extension}' é compatível com análise na conversa, "
            "mas não pode ser indexado no RAG como texto."
        )
    return _decode_text(data, filename)


def extrair_texto_upload(uploaded_file: Any) -> str:
    filename, data = _read_uploaded_file(uploaded_file)
    return _extract_text(data, filename)[:MAX_ATTACHMENT_TEXT_CHARS]


def preparar_anexo(uploaded_file: Any) -> dict[str, str]:
    filename, data = _read_uploaded_file(uploaded_file)
    extension = os.path.splitext(filename)[1].lower()
    if extension == ".pdf":
        reader = PdfReader(io.BytesIO(data))
        if len(reader.pages) > MAX_PDF_PAGES:
            raise ValueError(f"O PDF excede o limite de {MAX_PDF_PAGES} páginas.")
    mime_type = MIME_BY_EXTENSION.get(
        extension,
        mimetypes.guess_type(filename)[0] or "application/octet-stream",
    )
    if extension in {
        ".docx",
        ".xlsx",
        ".pptx",
        ".parquet",
        *TEXT_EXTENSIONS,
    } or filename.casefold() in TEXT_FILENAMES:
        extracted = _extract_text(data, filename)[:MAX_ATTACHMENT_TEXT_CHARS]
        if not extracted.strip():
            raise ValueError(f"Não foi possível extrair conteúdo de '{filename}'.")
        data = f"[Conteúdo extraído do arquivo: {filename}]\n{extracted}".encode(
            "utf-8"
        )
        mime_type = "text/plain"
    if not data:
        raise ValueError(f"O arquivo '{filename}' está vazio.")
    return {
        "nome": filename,
        "mime_type": mime_type,
        "conteudo_b64": base64.b64encode(data).decode("ascii"),
    }
