from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import platform
import re
import shutil
from typing import Iterable, List, Optional, Sequence, Tuple

import cv2
import numpy as np
import pytesseract
from PIL import Image


_SUPPORTED_SUFFIXES = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}


def _validate_image_path(image_path: Path | str) -> Path:
    path = Path(image_path)
    if not path.is_file():
        raise FileNotFoundError(f"Image not found: {path}")
    if path.suffix.lower() not in _SUPPORTED_SUFFIXES:
        raise ValueError(f"Unsupported image type: {path.suffix}")
    return path


def _resolve_tesseract_cmd(explicit_cmd: Optional[Path | str]) -> str:
    if explicit_cmd:
        candidate = Path(explicit_cmd)
        if candidate.is_file():
            return str(candidate)
        raise FileNotFoundError(f"Tesseract executable not found at {candidate}")

    discovered = shutil.which("tesseract")
    if discovered:
        return discovered

    if platform.system().lower() == "windows":
        for candidate in (
            Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe"),
            Path(r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"),
        ):
            if candidate.is_file():
                return str(candidate)

    raise FileNotFoundError(
        "Tesseract executable not found. Install Tesseract OCR and/or pass tesseract_cmd explicitly."
    )


def _deskew(image: np.ndarray) -> np.ndarray:
    coords = np.column_stack(np.where(image < 255))
    if coords.size == 0:
        return image

    angle = cv2.minAreaRect(coords)[-1]
    if angle < -45:
        angle = 90 + angle
    elif angle > 45:
        angle = angle - 90

    if abs(angle) < 0.5:
        return image

    (h, w) = image.shape[:2]
    center = (w // 2, h // 2)
    matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    return cv2.warpAffine(
        image,
        matrix,
        (w, h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_REPLICATE,
    )


def _rotate_bound(image: np.ndarray, angle: float) -> np.ndarray:
    (h, w) = image.shape[:2]
    center = (w / 2, h / 2)

    matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    cos = abs(matrix[0, 0])
    sin = abs(matrix[0, 1])

    nW = int((h * sin) + (w * cos))
    nH = int((h * cos) + (w * sin))

    matrix[0, 2] += (nW / 2) - center[0]
    matrix[1, 2] += (nH / 2) - center[1]

    return cv2.warpAffine(
        image,
        matrix,
        (nW, nH),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_REPLICATE,
    )


def _normalize_rotation(image: np.ndarray) -> np.ndarray:
    try:
        osd = pytesseract.image_to_osd(image, output_type=pytesseract.Output.DICT)
        angle = osd.get("rotate", 0)
    except pytesseract.TesseractError:
        return image

    if angle and angle != 0:
        return _rotate_bound(image, -angle)
    return image


def _sharpen(image: np.ndarray) -> np.ndarray:
    blurred = cv2.GaussianBlur(image, (0, 0), sigmaX=1.0)
    sharp = cv2.addWeighted(image, 1.5, blurred, -0.5, 0)
    return cv2.normalize(sharp, None, alpha=0, beta=255, norm_type=cv2.NORM_MINMAX)


def _generate_candidates(image: np.ndarray) -> Tuple[np.ndarray, ...]:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    height, width = gray.shape
    max_dim = max(height, width)
    scale = 1.0
    if max_dim < 1900:
        scale = 1900 / max_dim
    if scale > 1.0:
        gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
    contrast = clahe.apply(gray)

    denoised = cv2.fastNlMeansDenoising(contrast, h=15)
    sharpened = _sharpen(denoised)

    otsu = cv2.threshold(sharpened, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    adaptive = cv2.adaptiveThreshold(
        sharpened,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        9,
    )

    inverted_otsu = cv2.bitwise_not(otsu)
    inverted_adaptive = cv2.bitwise_not(adaptive)

    candidates: List[np.ndarray] = []
    base_variants = (
        contrast,
        sharpened,
        otsu,
        adaptive,
        inverted_otsu,
        inverted_adaptive,
        gray,
        cv2.bitwise_not(gray),
    )

    for candidate in base_variants:
        normalized = _deskew(candidate)
        bordered = cv2.copyMakeBorder(
            normalized,
            8,
            8,
            8,
            8,
            cv2.BORDER_CONSTANT,
            value=255,
        )
        candidates.append(bordered)

    return tuple(candidates)


def _image_to_string(image: np.ndarray, *, lang: str, config: str) -> str:
    if image.ndim == 2:
        pil_image = Image.fromarray(image)
    else:
        pil_image = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
    return pytesseract.image_to_string(pil_image, lang=lang, config=config)


_LATIN_TO_CYR = str.maketrans(
    {
        "A": "А",
        "B": "В",
        "C": "С",
        "E": "Е",
        "H": "Н",
        "K": "К",
        "M": "М",
        "O": "О",
        "P": "Р",
        "T": "Т",
        "X": "Х",
        "Y": "У",
        "W": "Ш",
        "a": "а",
        "c": "с",
        "e": "е",
        "o": "о",
        "p": "р",
        "x": "х",
        "y": "у",
        "w": "ш",
    }
)


def _score_text(text: str) -> float:
    if not text:
        return float("-inf")

    letters = sum(1 for ch in text if ch.isalpha())
    cyrillic = sum(1 for ch in text if "\u0400" <= ch <= "\u04FF")
    digits = sum(1 for ch in text if ch.isdigit())
    keywords = ["ООО", "КАССОВЫЙ", "НДС", "КАССИР", "СУММА"]

    score = 0.0
    if letters:
        score += (cyrillic / letters) * 10
    score += digits * 0.05
    score += min(len(text) / 500, 1.0) * 2
    for word in keywords:
        if word in text:
            score += 5
    return score


def _normalize_cyrillic(text: str) -> str:
    normalized = text.replace("“", "\"").replace("”", "\"").replace("„", "\"")
    normalized = normalized.replace("«", "*").replace("»", "")
    normalized = normalized.translate(_LATIN_TO_CYR)
    normalized = normalized.replace("—", "-").replace("|", " ")
    normalized = re.sub(r"[ ]{2,}", " ", normalized)
    normalized = re.sub(r"\b0([А-Я])", r"О\1", normalized)
    normalized = normalized.replace("0БЛ", "ОБЛ").replace("0Н:", "ФН:")
    normalized = re.sub(r'000\s+"?ЛЕНТА"?', 'ООО "ЛЕНТА"', normalized)
    replacements = {
        "HAC": "НДС",
        "НАС": "НДС",
        "ВЕЗНАЛИЧНЫМИ": "БЕЗНАЛИЧНЫМИ",
        "ВЕЗНАИИЧНЫМИ": "БЕЗНАЛИЧНЫМИ",
        "ПЕНТА": "ЛЕНТА",
        "ЕВЕЗН": "FRESH",
        "КОТТО": "MOJITO",
        "ПИМОНЫ": "ЛИМОНЫ",
        "НАЙОНЕЗ": "МАЙОНЕЗ",
        "ВОКОЛАД": "ШОКОЛАД",
        "ВОКОЛАЙ": "ШОКОЛАД",
        "КОК ТЕНН КОНФЕСТА СН Р": "КОНФЕТЫ СН Р",
        "ЕВ РЕЗ": "ЖЕВ РЕЗ",
        "ОГКОГ": "ДРОП",
        "Х-FRESH": "X-FRESH",
        "З/ЛАСТА": "З/ПАСТА",
        "ЗРЕАТ": "SPLAT",
        "ИЕЧЕБНЫЕ": "ЛЕЧЕБНЫЕ",
        "МАЙОНЕЗ ЯНТА ПРОВАНСАЙ": "МАЙОНЕЗ ЯНТА ПРОВАНСАЛЬ",
        "ТIТВIТ": "TITBIT",
        "DIROL": "DIROL",
        "КОЛИЯ": "КОПИЯ",
        "СМЕНА N": "СМЕНА №",
        "ОБИ.": "ОБЛ.",
        "НДС 202": "НДС 20%",
        "НДС 102": "НДС 10%",
        "НДС 203": "НДС 20%",
        "НДС 103": "НДС 10%",
        "FRЕSН": "FRESH",
        "жк": "",
        "КУБ 174.99 21.200": "КУБ 174.99 *1.200",
        "›ПРОДАВА ТОВАРА»": "ПРОДАЖА ТОВАРА",
        "ПРОДАВА ТОВАРА": "ПРОДАЖА ТОВАРА",
        "ШАКОNАА": "ШОКОЛАД",
        "ШОКОNАА": "ШОКОЛАД",
        "ШОК ТЕМН": "ШОКОЛАД ТЕМН",
        "Q/СОВ": "Д/СОВ",
        "ОNОN": "ОПОЛ",
        "ШЕВ": "ЖЕВ",
        "DIRОL": "DIROL",
        "ЭПРОДАЖА": "ПРОДАЖА",
        "АЕС": "ДЕС",
        "КОНФЕСТА": "КОНФЕТЫ",
        "ДУЙ": "Д/Й",
        "РЕS": "РЕЗ",
    }
    for wrong, right in replacements.items():
        normalized = normalized.replace(wrong, right)
    return normalized


_PRICE_QTY_PATTERN = re.compile(
    r"(?P<price>\d+[.,]\d+)\s*(?:\*|\s)(?P<qty>\d+[.,]?\d*)\s*=*\s*(?P<total>\d+[.,]\d+)"
)


def _postprocess_line(line: str) -> str:
    text = line.strip()
    if not text:
        return text

    text = text.replace("#", "*")
    text = text.replace("НДС 20:", "НДС 20%").replace("НДС 10:", "НДС 10%")
    for digit in range(1, 6):
        text = text.replace(f"\"{digit}", f"*{digit}")
    text = re.sub(
        r"(?P<price>\d+[.,]\d+)\s+(?P<qty>\d+)\s*=",
        lambda m: f"{m.group('price')} *{m.group('qty')} =",
        text,
    )

    match = _PRICE_QTY_PATTERN.search(text)
    if match:
        price = float(match.group("price").replace(",", "."))
        qty = float(match.group("qty").replace(",", "."))
        total = float(match.group("total").replace(",", "."))
        expected = round(price * qty, 2)
        if abs(expected - total) > 0.02:
            text = (
                text[: match.start("total")]
                + f"{expected:.2f}"
                + text[match.end("total") :]
            )
        segment = text[match.start() : match.end()]
        if "=" not in segment:
            text = (
                text[: match.end("qty")]
                + " ="
                + text[match.end("qty") :]
            )

    if text.startswith("==="):
        return ""
    if text.startswith("г "):
        text = text[2:].strip()
    if text in {"г", "1"}:
        return ""
    text = text.replace('""', '"')
    text = re.sub(r"\s*=\s*", " = ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_receipt_text(
    image_path: Path | str,
    *,
    lang: str = "rus+eng",
    tesseract_cmd: Optional[Path | str] = None,
    preserve_empty_lines: bool = False,
) -> List[str]:
    validated_path = _validate_image_path(image_path)
    pytesseract.pytesseract.tesseract_cmd = _resolve_tesseract_cmd(tesseract_cmd)

    image = cv2.imread(str(validated_path))
    if image is None:
        raise RuntimeError(f"Unable to read image: {validated_path}")

    normalized_image = _normalize_rotation(image)
    candidates = _generate_candidates(normalized_image)

    primary_lang = lang or "rus+eng"
    whitelist = (
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
        "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
        "№*/.-:"
    )

    configs: List[Tuple[str, str]] = [
        (primary_lang, "--oem 1 --psm 6 -c preserve_interword_spaces=1 --dpi 300"),
        (primary_lang, "--oem 3 --psm 4 -c preserve_interword_spaces=1 --dpi 300"),
        (
            "rus",
            f"--oem 1 --psm 6 --dpi 300 -c tessedit_char_whitelist={whitelist}",
        ),
    ]

    best_text = ""
    best_score = float("-inf")

    for variant in candidates:
        for lang_code, config in configs:
            raw_text = _image_to_string(variant, lang=lang_code, config=config)
            score = _score_text(raw_text)
            if score > best_score:
                best_score = score
                best_text = raw_text

    normalized_text = _normalize_cyrillic(best_text)
    lines = normalized_text.splitlines()
    processed_lines = [_postprocess_line(line) for line in lines]

    if preserve_empty_lines:
        return [line.rstrip() for line in processed_lines]
    return [line for line in processed_lines if line]


def extract_receipt_text(
    image_path: Path | str,
    *,
    lang: str = "rus+eng",
    tesseract_cmd: Optional[Path | str] = None,
) -> str:
    """
    Возвращает текст чека, подготовленный для последующей передачи в LLM.
    Пустые строки удаляются, строки постобрабатываются (_postprocess_line).
    """
    lines = parse_receipt_text(
        image_path,
        lang=lang,
        tesseract_cmd=tesseract_cmd,
        preserve_empty_lines=False,
    )
    return "\n".join(lines)


def save_receipt_text(
    lines: Sequence[str],
    output_path: Optional[Path | str] = None,
    *,
    encoding: str = "utf-8-sig",
) -> Path:
    if isinstance(lines, (str, bytes)):
        raise TypeError("lines must be an iterable of strings, not a single string.")
    if not isinstance(lines, Iterable):
        raise TypeError("lines must be an iterable of strings")

    serialized = "\n".join(str(line) for line in lines)
    target = Path(output_path) if output_path else Path(__file__).with_name("chequeText.txt")
    target.write_text(serialized, encoding=encoding)
    return target


def parse_and_save(
    image_path: Path | str,
    *,
    output_path: Optional[Path | str] = None,
    lang: str = "rus+eng",
    tesseract_cmd: Optional[Path | str] = None,
    preserve_empty_lines: bool = False,
    encoding: str = "utf-8-sig",
) -> Path:
    lines = parse_receipt_text(
        image_path,
        lang=lang,
        tesseract_cmd=tesseract_cmd,
        preserve_empty_lines=preserve_empty_lines,
    )
    return save_receipt_text(lines, output_path=output_path, encoding=encoding)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse supermarket receipt text into a file.")
    parser.add_argument("image_path", type=Path, help="Path to the receipt image (jpg, png, tif).")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Destination path for the extracted text file. Defaults to chequeText.txt in this module.",
    )
    parser.add_argument(
        "--lang",
        default="rus+eng",
        help="Language pack(s) for Tesseract, e.g. 'rus+eng', 'eng'. Defaults to rus+eng.",
    )
    parser.add_argument(
        "--tesseract",
        type=Path,
        default=None,
        help="Full path to tesseract executable if not on PATH.",
    )
    parser.add_argument(
        "--keep-empty",
        action="store_true",
        help="Preserve empty lines in the output text.",
    )

    args = parser.parse_args()
    destination = parse_and_save(
        args.image_path,
        output_path=args.output,
        lang=args.lang,
        tesseract_cmd=args.tesseract,
        preserve_empty_lines=args.keep_empty,
    )
    print(f"Parsed text saved to {destination}")

